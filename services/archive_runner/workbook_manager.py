"""
MODULE: services.archive_runner.workbook_manager
RESPONSIBILITY: Prepare document files for processing (unzip, convert, deduplicate).
ALLOWED: DocumentSelector, ArchiveExtractor, DocumentDownloader, ExcelPreparator, ArchiveProcessor, logging.
FORBIDDEN: Business logic (focus on file prep).
ERRORS: None.

Менеджер подготовки документов для обработки.

Координирует работу с файлами, архивами и Excel файлами.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from loguru import logger

from services.document_search.document_selector import DocumentSelector
from services.document_search.archive_extractor import ArchiveExtractor
from services.document_search.document_downloader import DocumentDownloader
from services.archive_runner.file_deduplicator import add_file_to_dict
from services.archive_runner.excel_preparator import ExcelPreparator
from services.archive_runner.archive_processor import ArchiveProcessor


class WorkbookManager:
    """Отвечает за распаковку архивов и подготовку путей к документам."""

    ARCHIVE_EXTENSIONS = {".rar", ".zip", ".7z"}
    EXCEL_EXTENSIONS = {".xlsx", ".xls"}
    WORD_EXTENSIONS = {".docx", ".doc"}
    PDF_EXTENSIONS = {".pdf"}

    def __init__(
        self,
        selector: DocumentSelector,
        extractor: ArchiveExtractor,
        downloader: Optional[DocumentDownloader] = None,
    ):
        self.selector = selector
        self.extractor = extractor
        self.downloader = downloader
        self._excel_preparator = ExcelPreparator()
        self._archive_processor = ArchiveProcessor(selector, extractor, downloader)

    def prepare_workbook_paths(
        self,
        records: List[Dict[str, Any]],
        documents: Optional[List[Dict[str, Any]]],
        tender_folder: Path,
    ) -> tuple[List[Path], List[Path], List[Path]]:
        """Возвращает списки подготовленных путей."""
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
                    "hypothesisId": "PREPARE_PATHS_START",
                    "location": "workbook_manager.py:prepare_workbook_paths:start",
                    "message": "Начинаем prepare_workbook_paths",
                    "data": {
                        "records_count": len(records),
                        "tender_folder": str(tender_folder),
                        "folder_exists": tender_folder.exists(),
                        "documents_count": len(documents) if documents else 0
                    },
                    "timestamp": int(time.time() * 1000)
                }))
        except Exception as e:
            pass
        # #endregion

        logger.info(f"🔍 Начинаем подготовку путей к файлам: {len(records)} записей")

        workbook_paths_dict: Dict[Tuple[str, int], Path] = {}
        workbook_paths_set: Set[Path] = set()
        archive_paths: List[Path] = []
        queue: List[Dict[str, Any]] = [self._normalize_record(record) for record in records]
        duplicates_count = 0
        processed_files = 0

        logger.info(f"📦 Очередь обработки: {len(queue)} записей")
        while queue:
            processed_files += 1
            if processed_files % 10 == 0:
                logger.debug(f"Обработано файлов: {processed_files}, осталось в очереди: {len(queue)}")
            record = queue.pop(0)

            # #region agent log
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "doc-processing-debug",
                        "hypothesisId": "PROCESS_RECORD",
                        "location": "workbook_manager.py:prepare_workbook_paths:process_record",
                        "message": f"Обрабатываем запись с {len(record['paths'])} файлами",
                        "data": {
                            "record_paths": [str(p)[-50:] for p in record["paths"]],
                            "processed_files": processed_files,
                            "queue_remaining": len(queue)
                        },
                        "timestamp": int(time.time() * 1000)
                    }))
            except Exception:
                pass
            # #endregion

            for file_path in record["paths"]:
                path = Path(file_path).resolve()
                if not path.exists():
                    # #region agent log
                    try:
                        with open(log_path, "a", encoding="utf-8") as f:
                            f.write(json.dumps({
                                "sessionId": "debug-session",
                                "runId": "doc-processing-debug",
                                "hypothesisId": "FILE_MISSING",
                                "location": "workbook_manager.py:prepare_workbook_paths:file_missing",
                                "message": f"Файл не существует: {path.name}",
                                "data": {"file_path": str(path)},
                                "timestamp": int(time.time() * 1000)
                            }))
                    except Exception:
                        pass
                    # #endregion
                    continue
                
                # Сначала определяем, является ли файл архивом (по расширению ИЛИ по содержимому)
                suffix = path.suffix.lower()
                is_archive = suffix in self.ARCHIVE_EXTENSIONS
                
                # Если расширение не архивное, но файл может быть архивом - проверяем содержимое
                if not is_archive and self.extractor.is_file_archive(path):
                    logger.info(f"Файл {path.name} имеет расширение {suffix}, но является архивом. Обрабатываем как архив.")
                    is_archive = True
                
                if is_archive:
                    # Это архив - распаковываем
                    logger.info(f"📦 Начинаем распаковку архива: {path.name} (размер: {path.stat().st_size / 1024 / 1024:.2f} МБ)")
                    archive_paths.append(path)

                    # #region agent log
                    try:
                        with open(log_path, "a", encoding="utf-8") as f:
                            f.write(json.dumps({
                                "sessionId": "debug-session",
                                "runId": "doc-processing-debug",
                                "hypothesisId": "BEFORE_PROCESS_ARCHIVE",
                                "location": "workbook_manager.py:prepare_workbook_paths:before_process_archive",
                                "message": f"Перед вызовом process_archive_path для {path.name}",
                                "data": {
                                    "archive_path": str(path),
                                    "archive_size": path.stat().st_size,
                                    "tender_folder": str(tender_folder)
                                },
                                "timestamp": int(time.time() * 1000)
                            }))
                    except Exception:
                        pass
                    # #endregion

                    success = self._archive_processor.process_archive_path(
                        path,
                        record,
                        documents,
                        tender_folder,
                        queue,
                        workbook_paths_dict,
                        workbook_paths_set,
                    )
                    if not success:
                        logger.warning(f"❌ Архив {path.name} пропущен из-за ошибок")
                    else:
                        logger.info(f"✅ Архив {path.name} успешно обработан")
                elif suffix in self.EXCEL_EXTENSIONS:
                    # Это Excel файл (не архив) - обрабатываем напрямую
                    if path.name.startswith("~$"):
                        continue
                    prepared_path = self._excel_preparator.prepare_excel_file(path, tender_folder)
                    if prepared_path:
                        duplicates_count += add_file_to_dict(
                            prepared_path, workbook_paths_dict, workbook_paths_set, "Excel"
                        )
                    else:
                        # Файл поврежден - удаляем исходный файл (он уже удален в prepare_excel_file)
                        logger.warning(f"Excel файл {path.name} поврежден и удален, будет пропущен")
                elif suffix in self.PDF_EXTENSIONS:
                    # PDF файлы добавляем напрямую (без копирования)
                    duplicates_count += add_file_to_dict(
                        path, workbook_paths_dict, workbook_paths_set, "PDF"
                    )
                elif suffix in self.WORD_EXTENSIONS:
                    # Word файлы добавляем напрямую (без копирования)
                    duplicates_count += add_file_to_dict(
                        path, workbook_paths_dict, workbook_paths_set, "Word"
                    )
                else:
                    logger.debug(f"Пропуск неподдерживаемого файла {path.name} (расширение: {suffix})")

        workbook_paths = list(workbook_paths_dict.values())
        if duplicates_count > 0:
            logger.info(
                f"Дедупликация файлов: найдено {duplicates_count} дубликатов, "
                f"уникальных {len(workbook_paths)}"
            )
        logger.info(f"Подготовлено уникальных документов для обработки: {len(workbook_paths)}")

        # #region agent log
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "doc-processing-debug",
                    "hypothesisId": "PREPARE_PATHS_COMPLETE",
                    "location": "workbook_manager.py:prepare_workbook_paths:complete",
                    "message": "prepare_workbook_paths завершен успешно",
                    "data": {
                        "workbook_paths_count": len(workbook_paths),
                        "archive_paths_count": len(archive_paths),
                        "duplicates_removed": duplicates_count,
                        "sample_workbook_paths": [str(p)[-50:] for p in workbook_paths[:3]] if workbook_paths else []
                    },
                    "timestamp": int(time.time() * 1000)
                }))
        except Exception:
            pass
        # #endregion

        return workbook_paths, archive_paths, workbook_paths.copy()

        # except block intentionally removed because we now raise upstream

    @staticmethod
    def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Нормализует запись для обработки.
        
        Args:
            record: Запись с метаданными
            
        Returns:
            Нормализованная запись
        """
        normalized = dict(record)
        normalized["paths"] = [Path(p) for p in record.get("paths", [])]
        normalized["retries"] = record.get("retries", 0)
        return normalized

