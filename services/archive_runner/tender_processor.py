"""
MODULE: services.archive_runner.tender_processor
RESPONSIBILITY: Orchestrate processing of a single tender (download, extract, match, save).
ALLOWED: TenderMatchRepository, FolderManager, DocumentSelector, Downloader, Extractor, MatchFinder, logging, FileValidator.
FORBIDDEN: Direct DB queries (use repositories).
ERRORS: DocumentSearchError.

Модуль обработки одного тендера.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from core.exceptions import DocumentSearchError
from services.document_search.document_selector import DocumentSelector
from services.document_search.document_downloader import DocumentDownloader
from services.document_search.archive_extractor import ArchiveExtractor
from services.document_search.match_finder import MatchFinder
from services.match_services.tender_match_repository_facade import TenderMatchRepositoryFacade
from services.archive_runner.tender_folder_manager import TenderFolderManager
from services.archive_runner.file_cleaner import FileCleaner
from services.archive_runner.processed_tenders_repository import ProcessedTendersRepository
from services.archive_runner.tender_prefetcher import PrefetchedTenderData
from services.archive_runner.document_download_manager import DocumentDownloadManager
from services.archive_runner.workbook_manager import WorkbookManager
from services.archive_runner.match_executor import MatchExecutor
from services.archive_runner.result_saver import ResultSaver
from services.archive_runner.file_validator import FileValidator


class TenderProcessor:
    """Оркестратор полного цикла обработки одного тендера."""

    def __init__(
        self,
        tender_match_repo: TenderMatchRepository,
        folder_manager: TenderFolderManager,
        document_search_service,
        selector: DocumentSelector,
        downloader: DocumentDownloader,
        extractor: ArchiveExtractor,
        match_finder: MatchFinder,
        file_cleaner: FileCleaner,
        processed_tenders_repo: Optional[ProcessedTendersRepository] = None,
        max_workers: int = 2,
        safe_call_func=None,
        get_avg_time_func=None,
        batch_delay: float = 5.0,
    ):
        self.folder_manager = folder_manager
        self.file_cleaner = file_cleaner
        self.selector = selector
        self.downloader = downloader
        self._safe_call = safe_call_func
        self.processed_tenders_repo = processed_tenders_repo

        self.download_manager = DocumentDownloadManager(downloader, max_workers)
        self.workbook_manager = WorkbookManager(selector, extractor, downloader)
        # Передаем batch_delay для пауз между партиями файлов
        self.match_executor = MatchExecutor(match_finder, max_workers, get_avg_time_func, batch_delay)
        self.result_saver = ResultSaver(tender_match_repo, safe_call_func)
        self.file_validator = FileValidator(
            self.workbook_manager, 
            self.downloader, 
            self.processed_tenders_repo
        )

    def process_tender(
        self,
        tender: Dict[str, Any],
        documents: Optional[List[Dict[str, Any]]] = None,
        existing_records: Optional[List[Dict[str, Any]]] = None,
        get_tender_documents_func=None,
        prefetched_data: Optional[PrefetchedTenderData] = None,
        processed_tenders_cache: Optional[Dict] = None,
        tender_type: str = 'new',
    ) -> Optional[Dict[str, Any]]:
        """
        Обработка одного тендера.
        
        Returns:
            Dict с результатами обработки или None в случае критической ошибки
        """
        tender_id = tender.get("id")
        registry_type = tender.get("registry_type", "44fz")
        tender_name = tender.get("auction_name", f"Торг #{tender_id}")

        # #region agent log PROCESS_TENDER_START
        import json
        import time as time_module
        log_path = Path(__file__).parent.parent.parent / ".cursor" / "debug.log"
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "analysis-trigger",
                    "hypothesisId": "process-start",
                    "location": "tender_processor.py:process_tender",
                    "message": "PROCESS_TENDER_START",
                    "data": {
                        "tender_id": tender_id,
                        "registry_type": registry_type,
                        "tender_type": tender_type,
                        "has_documents": documents is not None,
                        "documents_count": len(documents) if documents else 0
                    },
                    "timestamp": int(time_module.time() * 1000)
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        # #endregion
        
        folder_path = prefetched_data.folder_path if prefetched_data else self.folder_manager.prepare_tender_folder(tender_id, registry_type, tender_type)
        folder_name = folder_path.name if folder_path else f"{registry_type}_{tender_id}_{tender_type}"
        tender["folder_path"] = folder_path

        # Проверяем, не была ли эта торг уже обработана
        # Используем базовое имя папки без суффикса типа торга для совместимости
        base_folder_name = f"{registry_type}_{tender_id}"
        if self.processed_tenders_repo and self.processed_tenders_repo.is_tender_processed(tender_id, registry_type, base_folder_name):
            # #region agent log IS_TENDER_PROCESSED
            try:
                log_path = Path(__file__).parent.parent.parent / ".cursor" / "debug.log"
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "analysis-trigger",
                        "hypothesisId": "is-processed-check",
                        "location": "tender_processor.py:process_tender:is_processed",
                        "message": "IS_TENDER_PROCESSED_TRUE",
                        "data": {
                            "tender_id": tender_id,
                            "registry_type": registry_type,
                            "base_folder_name": base_folder_name
                        },
                        "timestamp": int(time_module.time() * 1000)
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            logger.debug(f"⏭️ Торг {tender_id} ({registry_type}) уже обработана, пропускаем")
            return None

        # Блокировки advisory-lock отключены для параллельной обработки новых и разыгранных торгов
        # Новые и разыгранные торги обрабатываются независимо в разных процессах
        
        logger.debug(f"🔍 Начинаем обработку торга {tender_id} ({registry_type}, {tender_type})")

        # Используем кэш обработанных торгов (заполняется батчем в runner.py)
        # Если тендера нет в кэше, значит он еще не обработан - не делаем запрос к БД
        match_result = None
        if processed_tenders_cache:
            match_result = processed_tenders_cache.get((tender_id, registry_type))
        
        # Если тендера нет в кэше, значит он еще не обработан - пропускаем запрос к БД
        # Кэш уже содержит все обработанные торги (загружены батчем)
        if match_result:
            # #region agent log
            import json
            import os
            # Path уже импортирован глобально
            # Используем относительный путь
            project_root = Path(__file__).parent.parent.parent
            log_path = project_root / ".cursor" / "debug.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "H",
                        "location": "tender_processor.py:process_tender:already_processed",
                        "message": "Торг уже обработан, пропускаем",
                        "data": {
                            "tender_id": tender_id,
                            "registry_type": registry_type,
                            "match_count": match_result.get("match_count", 0),
                            "processed_at": str(match_result.get("processed_at", "unknown")),
                            "is_interesting": match_result.get("is_interesting")
                        },
                        "timestamp": int(time_module.time() * 1000)
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            
            self._log_already_processed(tender_id, registry_type, match_result)
            return self.result_saver.create_skipped_result(
                tender_id, registry_type, "already_processed", match_result
            )

        # Проверяем существующие файлы на валидность (только если торг не обработан)
        # ВАЖНО: Проверка происходит ВСЕГДА, даже для prefetched файлов,
        # чтобы убедиться что они не повреждены и могут быть открыты/распакованы
        try:
            # Если есть предзагруженные записи — считаем их источником документов
            if prefetched_data and prefetched_data.download_records:
                documents = prefetched_data.download_records
                logger.debug(
                    f"Используем предзагруженные файлы для торга {tender_id} ({registry_type}), "
                    f"files={len(documents)}"
                )

            existing_records = None
            if documents:
                # Валидация скачанных (prefetch или ранее) файлов
                valid_records = self.file_validator.validate_prefetched_files(
                    documents, 
                    folder_path,
                    tender_id,
                    registry_type,
                    folder_name,
                    tender.get("user_id", 1)
                )
                if valid_records is None:
                    logger.warning(
                        f"Все скачанные файлы повреждены для торга {tender_id} ({registry_type}), очищаем папку"
                    )
                    self.folder_manager.clean_tender_folder_force(folder_path)
                    documents = None
                elif len(valid_records) < len(documents):
                    logger.warning(
                        f"Некоторые скачанные файлы повреждены для торга {tender_id} ({registry_type}), удаляем их из списка"
                    )
                    documents = valid_records
                    existing_records = valid_records
                else:
                    existing_records = valid_records
                    logger.info(f"Скачанные файлы валидны для торга {tender_id} ({registry_type}), используем их")

            if not documents and not (prefetched_data and prefetched_data.cleaned):
                # Если нет документов после prefetch — проверяем папку на существующие валидные файлы
                existing_records = self.file_validator.check_existing_files(folder_path)
                if existing_records is None:
                    logger.warning(f"Обнаружены поврежденные файлы в папке торга {tender_id} ({registry_type}), очищаем папку")
                    self.folder_manager.clean_tender_folder_force(folder_path)
                    existing_records = None
                elif existing_records:
                    logger.info(f"Найдены валидные файлы в папке торга {tender_id} ({registry_type}), используем их")

            documents = documents or (prefetched_data.documents if prefetched_data else None)
            if documents is None and get_tender_documents_func:
                documents = get_tender_documents_func(tender_id, registry_type)

            # #region agent log DOCUMENTS_RESOLVED
            try:
                log_path = Path(__file__).parent.parent.parent / ".cursor" / "debug.log"
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "analysis-trigger",
                        "hypothesisId": "docs-resolved",
                        "location": "tender_processor.py:process_tender:documents_resolved",
                        "message": "DOCUMENTS_RESOLVED",
                        "data": {
                            "tender_id": tender_id,
                            "registry_type": registry_type,
                            "has_prefetched_data": prefetched_data is not None,
                            "prefetched_docs_count": len(prefetched_data.documents) if prefetched_data and prefetched_data.documents else 0,
                            "prefetched_records_count": len(prefetched_data.download_records) if prefetched_data and prefetched_data.download_records else 0,
                            "documents_count": len(documents) if documents else 0,
                            "existing_records_count": len(existing_records) if existing_records else 0
                        },
                        "timestamp": int(time_module.time() * 1000)
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion

            # Строим список записей для скачивания/использования
            # Если есть existing_records (валидные файлы), используем их
            # Иначе используем prefetched_data.download_records (если они есть и валидны)
            download_records = self._build_download_records(existing_records, prefetched_data)
            if not download_records and documents:
                try:
                    selected_docs = self.selector.choose_documents(documents)
                    unique_docs = self.selector.group_documents_by_archive(selected_docs, documents)
                    download_records = self.download_manager.download_documents(unique_docs, documents, folder_path)
                except DocumentSearchError as error:
                    logger.warning(f"Для торга {tender_id} нет подходящих документов: {error}")

            if not download_records:
                logger.warning(f"❌ Нет файлов для обработки по торгу {tender_id} ({registry_type}) - сохраняем ошибку в БД")
                # Сохраняем ошибку в БД для последующей ручной обработки
                folder_name = folder_path.name if folder_path and folder_path.exists() else (f"{registry_type}_{tender_id}_won" if tender_type == 'won' else f"{registry_type}_{tender_id}")
                
                return self.result_saver.save_error_result(
                    tender_id,
                    registry_type,
                    error_reason="no_documents",
                    folder_name=folder_name,
                    processing_time=time.time() - processing_start
                )

            logger.info(f"\n{'=' * 80}")
            logger.info(f"Обработка торга: {tender_name} (ID: {tender_id}, {registry_type})")
            logger.info(f"{'=' * 80}")
            logger.info(f"Найдено записей для скачивания/использования: {len(download_records)}")
            
            # Детальное логирование для диагностики
            if download_records:
                total_files = sum(len(record.get("paths", [])) for record in download_records)
                logger.info(f"Всего файлов в записях: {total_files}")
                for idx, record in enumerate(download_records[:3]):  # Показываем первые 3 записи
                    paths = record.get("paths", [])
                    logger.debug(f"  Запись {idx+1}: {len(paths)} файлов, пути: {[str(p)[-50:] for p in paths[:2]]}")

            processing_start = time.time()
            logger.info(f"Подготовка путей к файлам для торга {tender_id}...")

            # #region agent log
            import json
            project_root = Path(__file__).parent.parent.parent
            log_path = project_root / ".cursor" / "debug.log"

            try:
                with open(str(log_path), "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "analysis",
                        "hypothesisId": "ANALYSIS",
                        "location": "tender_processor.py:process_tender:before_prepare_paths",
                        "message": "Перед подготовкой путей к файлам",
                        "data": {
                            "tender_id": tender_id,
                            "registry_type": registry_type,
                            "download_records_count": len(download_records),
                            "folder_path": str(folder_path) if folder_path else None
                        },
                        "timestamp": int(time_module.time() * 1000)
                    }, ensure_ascii=False) + "\n")
                    f.flush()
            except Exception:
                pass
            # #endregion
            
            # Обрабатываем поврежденные файлы: удаляем, скачиваем заново, проверяем
            download_records = self.file_validator.handle_corrupted_files(download_records, documents, folder_path)

            # #region agent log
            import json
            import os
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            log_path = os.path.join(project_root, ".cursor", "debug.log")
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "doc-processing-debug",
                        "hypothesisId": "BEFORE_PREPARE_PATHS",
                        "location": "tender_processor.py:process_tender:before_prepare_paths",
                        "message": f"Перед вызовом prepare_workbook_paths для торга {tender_id}",
                        "data": {
                            "tender_id": tender_id,
                            "registry_type": registry_type,
                            "download_records_count": len(download_records),
                            "folder_path": str(folder_path)
                        },
                        "timestamp": int(time_module.time() * 1000)
                    }))
            except Exception as e:
                pass
            # #endregion
            
            try:
                workbook_paths, archive_paths, excel_paths = self.workbook_manager.prepare_workbook_paths(
                    download_records,
                    documents,
                    folder_path,
                )
                logger.info(f"Подготовлено путей: workbook={len(workbook_paths) if workbook_paths else 0}, archive={len(archive_paths) if archive_paths else 0}, excel={len(excel_paths) if excel_paths else 0}")

                # #region agent log
                try:
                    with open(str(log_path), "a", encoding="utf-8") as f:
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "analysis",
                            "hypothesisId": "ANALYSIS",
                            "location": "tender_processor.py:process_tender:after_prepare_paths",
                            "message": "После подготовки путей к файлам",
                            "data": {
                                "tender_id": tender_id,
                                "registry_type": registry_type,
                                "workbook_paths_count": len(workbook_paths) if workbook_paths else 0,
                                "archive_paths_count": len(archive_paths) if archive_paths else 0,
                                "excel_paths_count": len(excel_paths) if excel_paths else 0
                            },
                            "timestamp": int(time_module.time() * 1000)
                        }, ensure_ascii=False) + "\n")
                        f.flush()
                except Exception:
                    pass
                # #endregion

            except Exception as prep_error:
                logger.error(f"❌ Ошибка при подготовке путей к файлам для торга {tender_id}: {prep_error}", exc_info=True)
                folder_name = folder_path.name if folder_path and folder_path.exists() else (f"{registry_type}_{tender_id}_won" if tender_type == 'won' else f"{registry_type}_{tender_id}")

                return self.result_saver.save_error_result(
                    tender_id,
                    registry_type,
                    error_reason=f"prepare_paths_error: {str(prep_error)[:200]}",
                    error_message=f"Ошибка подготовки путей: {prep_error}",
                    folder_name=folder_name,
                    processing_time=time.time() - processing_start
                )
            
            if not workbook_paths:
                logger.error(f"❌ Не удалось подготовить Excel файлы для торга {tender_id} ({registry_type})")
                logger.error(f"   download_records: {len(download_records)} записей")
                if download_records:
                    total_files = sum(len(record.get("paths", [])) for record in download_records)
                    logger.error(f"   Всего файлов в записях: {total_files}")
                    # Показываем детали первых записей
                    for idx, record in enumerate(download_records[:5]):
                        paths = record.get("paths", [])
                        logger.error(f"   Запись {idx+1}: {len(paths)} файлов")
                        for path_idx, path in enumerate(paths[:3]):
                            path_obj = Path(path)
                            exists = path_obj.exists()
                            logger.error(f"      Файл {path_idx+1}: {path_obj.name} (существует: {exists}, размер: {path_obj.stat().st_size if exists else 0})")
                logger.error(f"   Папка торга: {folder_path} (существует: {folder_path.exists()})")
                
                # Сохраняем ошибку в БД для последующей ручной обработки
                folder_name = folder_path.name if folder_path and folder_path.exists() else (f"{registry_type}_{tender_id}_won" if tender_type == 'won' else f"{registry_type}_{tender_id}")

                return self.result_saver.save_error_result(
                    tender_id,
                    registry_type,
                    error_reason="no_workbook_files",
                    error_message=f"Не удалось подготовить Excel файлы: {len(download_records)} записей, {sum(len(r.get('paths', [])) for r in download_records)} файлов",
                    folder_name=folder_name,
                    processing_time=time.time() - processing_start
                )
            
            logger.info(f"📊 Начинаем парсинг {len(workbook_paths)} файлов для торга {tender_id}...")
            if workbook_paths:
                logger.info(f"   Примеры файлов: {[p.name for p in workbook_paths[:3]]}")
            
            # #region agent log
            import json
            import os
            # Path уже импортирован глобально
            # Используем относительный путь
            project_root = Path(__file__).parent.parent.parent
            log_path = project_root / ".cursor" / "debug.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "F",
                        "location": "tender_processor.py:process_tender:before_match_executor",
                        "message": "Перед запуском match_executor.run",
                        "data": {
                            "tender_id": tender_id,
                            "registry_type": registry_type,
                            "workbook_paths_count": len(workbook_paths),
                            "workbook_paths": [str(p) for p in workbook_paths[:5]],
                            "folder_path": str(folder_path) if folder_path else None
                        },
                        "timestamp": int(time_module.time() * 1000)
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion

            try:
                logger.debug(f"Запуск match_executor.run() для торга {tender_id} с {len(workbook_paths)} файлами")

                # #region agent log - ДОСТУП ДО MATCH_EXECUTOR
                import json
                project_root = Path(__file__).parent.parent.parent
                log_path = project_root / ".cursor" / "debug.log"

                try:
                    with open(str(log_path), "a", encoding="utf-8") as f:
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "analysis",
                            "hypothesisId": "ANALYSIS",
                            "location": "tender_processor.py:process_tender:reached_match_executor",
                            "message": "ДОСТИГЛИ вызова match_executor.run",
                            "data": {
                                "tender_id": tender_id,
                                "registry_type": registry_type,
                                "workbook_paths_count": len(workbook_paths),
                                "workbook_paths_sample": [str(p)[-50:] for p in workbook_paths[:3]] if workbook_paths else []
                            },
                            "timestamp": int(time_module.time() * 1000)
                        }, ensure_ascii=False) + "\n")
                        f.flush()
                except Exception:
                    pass
                # #endregion

                # #region agent log
                import json
                import os
                log_path = r"c:\Users\wangr\PycharmProjects\pythonProject89\.cursor\debug.log"
                try:
                    with open(str(log_path), "a", encoding="utf-8") as f:
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "B",
                            "location": "tender_processor.py:process_tender:before_match_executor",
                            "message": "Перед вызовом match_executor.run",
                            "data": {
                                "tender_id": tender_id,
                                "registry_type": registry_type,
                                "workbook_paths_count": len(workbook_paths)
                            },
                            "timestamp": int(time_module.time() * 1000)
                        }, ensure_ascii=False) + "\n")
                        f.flush()
                        os.fsync(f.fileno())
                except Exception:
                    pass
                # #endregion
                
                match_result = self.match_executor.run(workbook_paths)
                matches = match_result.get("matches", [])
                failed_files = match_result.get("failed_files", [])
                
                # #region agent log
                try:
                    with open(str(log_path), "a", encoding="utf-8") as f:
                        f.flush()
                        os.fsync(f.fileno())
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "B",
                            "location": "tender_processor.py:process_tender:after_match_executor",
                            "message": "После вызова match_executor.run",
                            "data": {
                                "tender_id": tender_id,
                                "registry_type": registry_type,
                                "matches_count": len(matches) if matches else 0,
                                "failed_files_count": len(failed_files),
                                "matches_is_none": matches is None,
                                "matches_is_empty": matches == [] if matches is not None else None,
                                "sample_matches": [{"product": m.get("product"), "score": m.get("score")} for m in (matches[:3] if matches else [])]
                            },
                            "timestamp": int(time_module.time() * 1000)
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                
                logger.info(f"🔍 Match executor вернул {len(matches) if matches else 0} совпадений для торга {tender_id}")
                if matches:
                    examples = [f"{m.get('product', 'N/A')} ({m.get('score', 0):.1f}%)" for m in matches[:3]]
                    logger.info(f"   Примеры совпадений: {examples}")
                if failed_files:
                    logger.warning(f"⚠️ Обнаружено {len(failed_files)} проблемных файлов для торга {tender_id}")
                    for failed_file in failed_files[:3]:  # Показываем первые 3
                        logger.warning(f"   - {Path(failed_file['path']).name}: {failed_file['error'][:100]}")
                processing_elapsed = time.time() - processing_start
                logger.debug(f"Сохранение результатов в БД для торга {tender_id}...")
                
                # #region agent log
                try:
                    with open(str(log_path), "a", encoding="utf-8") as f:
                        f.flush()
                        os.fsync(f.fileno())
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "F",
                            "location": "tender_processor.py:process_tender:after_match_executor",
                            "message": "После завершения match_executor.run",
                            "data": {
                                "tender_id": tender_id,
                                "registry_type": registry_type,
                                "matches_count": len(matches) if matches else 0,
                                "processing_elapsed": processing_elapsed
                            },
                            "timestamp": int(time_module.time() * 1000)
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                # #region agent log
                import json
                import os
                # Используем относительный путь (Path уже импортирован глобально)
                project_root = Path(__file__).parent.parent.parent
                log_path = project_root / ".cursor" / "debug.log"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with open(str(log_path), "a", encoding="utf-8") as f:
                        f.flush()
                        os.fsync(f.fileno())
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "F",
                            "location": "tender_processor.py:process_tender:before_save",
                            "message": "Перед вызовом result_saver.save",
                            "data": {
                                "tender_id": tender_id,
                                "registry_type": registry_type,
                                "matches_count": len(matches) if matches else 0,
                                "files_count": len(workbook_paths)
                            },
                            "timestamp": int(time_module.time() * 1000)
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                
                # Получаем название папки ДО удаления (пока папка существует)
                folder_name = None
                if folder_path and folder_path.exists():
                    folder_name = folder_path.name
                else:
                    # Если папка не существует, формируем название явно
                    if tender_type == 'won':
                        folder_name = f"{registry_type}_{tender_id}_won"
                    else:
                        folder_name = f"{registry_type}_{tender_id}"
                
                # Сохраняем результаты в БД с folder_name и информацией о проблемных файлах
                result = self.result_saver.save(
                    tender_id, 
                    registry_type, 
                    matches, 
                    workbook_paths, 
                    processing_elapsed, 
                    error_reason=None, 
                    folder_name=folder_name,
                    failed_files=failed_files
                )
                
                if result is None:
                    # #region agent log - SAVE FAILED
                    try:
                        with open(str(log_path), "a", encoding="utf-8") as f:
                            f.write(json.dumps({
                                "sessionId": "debug-session",
                                "runId": "save-failure",
                                "hypothesisId": "SAVE_FAILED",
                                "location": "tender_processor.py:save_result_failure",
                                "message": "КРИТИЧНАЯ ОШИБКА: result_saver.save вернул None",
                                "data": {
                                    "tender_id": tender_id,
                                    "registry_type": registry_type,
                                    "matches_count": len(matches),
                                    "files_count": len(workbook_paths),
                                    "processing_elapsed": processing_elapsed,
                                    "match_percentage": result.get("match_percentage") if result else None
                                },
                                "timestamp": int(time_module.time() * 1000)
                            }, ensure_ascii=False) + "\n")
                            f.flush()
                            os.fsync(f.fileno())
                    except Exception:
                        pass
                    # #endregion

                # #region agent log
                try:
                    with open(str(log_path), "a", encoding="utf-8") as f:
                        f.flush()
                        os.fsync(f.fileno())
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "F",
                            "location": "tender_processor.py:process_tender:after_save",
                            "message": "После вызова result_saver.save",
                            "data": {
                                "tender_id": tender_id,
                                "registry_type": registry_type,
                                "result": result is not None,
                                "match_count": result.get("match_count") if result else None
                            },
                            "timestamp": int(time_module.time() * 1000)
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion
                
                logger.debug(f"Result saver вернул для торга {tender_id}: {result}")

                # Показываем результат обработки
                match_count = len(matches) if matches else 0
                if match_count > 0:
                    logger.info(f"🔍 Найдено совпадений: {match_count} (время обработки: {processing_elapsed:.1f} сек)")
                else:
                    logger.info(f"⚠️ Совпадений не найдено (время обработки: {processing_elapsed:.1f} сек)")

                # Удаляем файлы и папку только после успешной записи в БД (неблокирующее удаление)
                # Проблемные файлы НЕ удаляем - они сохраняются для последующей обработки
                if result is not None:
                    if failed_files:
                        logger.info(f"Результаты сохранены в БД для торга {tender_id}, удаляем файлы (кроме {len(failed_files)} проблемных)")
                    else:
                        logger.info(f"Результаты сохранены в БД для торга {tender_id}, удаляем файлы и папку")
                    try:
                        self.file_cleaner.cleanup_all_files(
                            archive_paths,
                            workbook_paths,
                            extraction_success=True,
                            db_save_success=True,
                            failed_files=failed_files,
                        )
                        # Удаляем всю папку торга только если нет проблемных файлов
                        if folder_path and folder_path.exists():
                            if failed_files:
                                logger.info(f"⚠️ Папка торга {tender_id} сохранена (содержит {len(failed_files)} проблемных файлов)")
                            else:
                                try:
                                    logger.info(f"Удаление папки торга {tender_id} после успешного сохранения в БД: {folder_path.name}")
                                    self.folder_manager.clean_tender_folder_force(folder_path)
                                    # Пытаемся удалить саму папку
                                    try:
                                        folder_path.rmdir()
                                        logger.debug(f"Папка {folder_path.name} успешно удалена")
                                    except OSError:
                                        # Папка не пуста или заблокирована - это нормально, файлы уже удалены
                                        logger.debug(f"Папка {folder_path.name} не пуста или заблокирована, пропускаем удаление")
                                except Exception as folder_cleanup_error:
                                    logger.warning(f"Не удалось удалить папку торга {tender_id}: {folder_cleanup_error}")
                    except Exception as cleanup_error:
                        # Не блокируем процесс, если удаление не удалось
                        logger.warning(f"Не удалось удалить некоторые файлы для торга {tender_id}: {cleanup_error}")
                else:
                    # Если result_saver.save() вернул None, это ошибка сохранения в БД
                    logger.error(
                        f"❌ Не удалось сохранить результаты в БД для торга {tender_id} ({registry_type}). "
                        f"Найдено совпадений: {match_count}, но сохранение не удалось."
                    )
                    # Возвращаем словарь с информацией об ошибке вместо None
                    return {
                        "tender_id": tender_id,
                        "registry_type": registry_type,
                        "match_count": match_count,
                        "match_percentage": 0.0,
                        "error": True,
                        "error_message": "Не удалось сохранить результаты в БД",
                        "error_saved": False,
                    }
                
                return result
            except Exception as processing_error:
                # Используем глобальный time модуль (импортирован в начале файла)
                processing_elapsed = time.time() - processing_start
                error_message = str(processing_error)
                logger.error(
                    f"❌ Ошибка при обработке торга {tender_id} ({registry_type}): {error_message}",
                    exc_info=True  # Добавляем полный traceback
                )
                # Сохраняем ошибку в БД для последующей ручной обработки
                # Получаем название папки ДО удаления (пока папка существует)
                folder_name = None
                if folder_path and folder_path.exists():
                    folder_name = folder_path.name
                else:
                    # Если папка не существует, формируем название явно
                    if tender_type == 'won':
                        folder_name = f"{registry_type}_{tender_id}_won"
                    else:
                        folder_name = f"{registry_type}_{tender_id}"
                
                error_result = self.result_saver.save(
                    tender_id,
                    registry_type,
                    [],
                    workbook_paths,
                    processing_elapsed,
                    error_reason=f"processing_error: {error_message[:200]}",  # Ограничиваем длину сообщения
                    folder_name=folder_name,
                )
                return {
                    "tender_id": tender_id,
                    "registry_type": registry_type,
                    "match_count": 0,
                    "match_percentage": 0.0,
                    "error": True,
                    "error_message": error_message,
                    "error_saved": error_result is not None,
                }
        except Exception as critical_error:
            # КРИТИЧЕСКАЯ ОШИБКА - выводим в консоль и крашим систему
            import traceback
            import sys
            
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА при обработке торга {tender_id} ({registry_type})"
            error_details = str(critical_error)
            full_traceback = traceback.format_exc()
            
            # Выводим в консоль (stderr для гарантированного вывода)
            print("\n" + "="*80, file=sys.stderr)
            print(f"КРИТИЧЕСКАЯ ОШИБКА", file=sys.stderr)
            print("="*80, file=sys.stderr)
            print(f"{error_msg}: {error_details}", file=sys.stderr)
            print("\nПолный traceback:", file=sys.stderr)
            print(full_traceback, file=sys.stderr)
            print("="*80 + "\n", file=sys.stderr)
            sys.stderr.flush()
            
            # Логируем через logger
            logger.critical(
                f"{error_msg}: {error_details}",
                exc_info=True
            )
            
            # Крашим систему для отладки
            sys.exit(1)


    def _build_download_records(
        self,
        existing_records: Optional[List[Dict[str, Any]]],
        prefetched_data: Optional[PrefetchedTenderData],
    ) -> List[Dict[str, Any]]:
        # Delegate to file_validator if needed, but it's simple enough here.
        # Actually, FileValidator has this method now too.
        return self.file_validator.build_download_records(existing_records, prefetched_data)

    @staticmethod
    def _log_already_processed(tender_id: int, registry_type: str, match_result: Dict[str, Any]) -> None:
        logger.info(
            f"Торг {tender_id} ({registry_type}) уже обработан: совпадений {match_result.get('match_count', 0)}, файлов {match_result.get('total_files_processed', 0)}, обработано {match_result.get('processed_at') or 'неизвестно'}"
        )
        logger.info(
            f"Пропускаем повторную обработку торга {tender_id} ({registry_type}). Для переобработки удалите запись из tender_document_matches."
        )

