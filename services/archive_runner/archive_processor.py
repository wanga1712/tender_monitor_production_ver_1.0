"""
MODULE: services.archive_runner.archive_processor
RESPONSIBILITY: Process individual archives, unzip, and register extracted files.
ALLOWED: DocumentSelector, ArchiveExtractor, DocumentDownloader, logging, services.archive_runner.file_deduplicator.
FORBIDDEN: Database interaction directly.
ERRORS: None.

Модуль для обработки архивов при подготовке документов.

Содержит логику распаковки архивов и обработки извлеченных файлов.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from loguru import logger

from services.document_search.document_selector import DocumentSelector
from services.document_search.archive_extractor import ArchiveExtractor
from services.document_search.document_downloader import DocumentDownloader
from services.archive_runner.file_deduplicator import add_file_to_dict


class ArchiveProcessor:
    """Класс для обработки архивов."""
    
    def __init__(
        self,
        selector: DocumentSelector,
        extractor: ArchiveExtractor,
        downloader: Optional[DocumentDownloader] = None,
    ):
        """
        Args:
            selector: Селектор документов
            extractor: Экстрактор архивов
            downloader: Загрузчик документов (опционально)
        """
        self.selector = selector
        self.extractor = extractor
        self.downloader = downloader
    
    def process_archive_path(
        self,
        archive_path: Path,
        record: Dict[str, Any],
        documents: Optional[List[Dict[str, Any]]],
        tender_folder: Path,
        queue: List[Dict[str, Any]],
        workbook_paths_dict: Dict[Tuple[str, int], Path],
        workbook_paths_set: Set[Path],
    ) -> bool:
        """
        Обработка одного архива: распаковка и добавление файлов.
        """
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
                    "hypothesisId": "ARCHIVE_PROCESS_START",
                    "location": "archive_processor.py:process_archive_path:start",
                    "message": f"Начинаем обработку архива: {archive_path.name}",
                    "data": {
                        "archive_path": str(archive_path),
                        "archive_exists": archive_path.exists(),
                        "archive_size": archive_path.stat().st_size if archive_path.exists() else 0,
                        "tender_folder": str(tender_folder)
                    },
                    "timestamp": int(time.time() * 1000)
                }))
        except Exception as e:
            pass
        # #endregion
        
        # Args:
        #     archive_path: Путь к архиву
        #     record: Запись с метаданными
        #     documents: Список всех документов (для повторной загрузки)
        #     tender_folder: Папка тендера
        #     queue: Очередь для обработки
        #     workbook_paths_dict: Словарь путей для дедупликации
        #     workbook_paths_set: Множество путей
        #     
        # Returns:
        #     True если архив успешно обработан, False если ошибка
        try:
            doc_meta = record.get("doc") or {}
            base_name, part_number = self.selector.split_archive_name(
                doc_meta.get("file_name") or archive_path.name
            )
            if part_number and part_number > 1:
                logger.debug(
                    "Пропускаем распаковку части %s (обрабатывается вместе с первой частью)",
                    archive_path.name,
                )
                return True

            target_dir = self._resolve_extract_dir(record, tender_folder, archive_path, base_name)

            # #region agent log
            import json
            project_root = Path(__file__).parent.parent.parent
            log_path = project_root / ".cursor" / "debug.log"

            try:
                with open(str(log_path), "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "extraction",
                        "hypothesisId": "EXTRACTION",
                        "location": "archive_processor.py:process_archive_path:before_extract",
                        "message": "Перед распаковкой архива",
                        "data": {
                            "archive_name": archive_path.name,
                            "archive_size": archive_path.stat().st_size if archive_path.exists() else 0,
                            "target_dir": str(target_dir)
                        },
                        "timestamp": int(time.time() * 1000)
                    }, ensure_ascii=False) + "\n")
                    f.flush()
            except Exception:
                pass
            # #endregion

            extracted_paths = self.extractor.extract_archive(archive_path, target_dir)

            # #region agent log
            try:
                with open(str(log_path), "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "extraction",
                        "hypothesisId": "EXTRACTION",
                        "location": "archive_processor.py:process_archive_path:after_extract",
                        "message": "После распаковки архива",
                        "data": {
                            "archive_name": archive_path.name,
                            "extracted_paths_count": len(extracted_paths) if extracted_paths else 0,
                            "extracted_paths_sample": [str(p)[-50:] for p in extracted_paths[:5]] if extracted_paths else []
                        },
                        "timestamp": int(time.time() * 1000)
                    }, ensure_ascii=False) + "\n")
                    f.flush()
            except Exception:
                pass
            # #endregion

            if not extracted_paths:
                logger.warning(
                    f"Архив {archive_path.name} не содержит поддерживаемых документов (Excel, Word, PDF)"
                )
            else:
                new_files = 0
                duplicates = 0
                for extracted_path in extracted_paths:
                    path = Path(extracted_path).resolve()
                    if not path.exists():
                        continue
                    if add_file_to_dict(path, workbook_paths_dict, workbook_paths_set, "") > 0:
                        duplicates += 1
                    else:
                        new_files += 1

                logger.info(
                    f"Архив {archive_path.name} распакован: найдено {len(extracted_paths)} файлов, "
                    f"новых {new_files}, дубликатов {duplicates}"
                )
                record.setdefault("extracted", []).extend(extracted_paths)
            return True
        except Exception as error:
            logger.warning(f"Архив {archive_path.name} поврежден: {error}")
            self._remove_file_force(archive_path)
            doc_meta = record.get("doc")
            retries = record.get("retries", 0)
            if not doc_meta or retries >= 1 or not documents or not self.downloader:
                logger.error(f"Повторная загрузка для {archive_path.name} невозможна")
                return False
            try:
                new_paths = self.downloader.download_required_documents(
                    doc_meta, documents, tender_folder
                )
                if new_paths:
                    queue.append(
                        {
                            "doc": doc_meta,
                            "paths": new_paths,
                            "source": "re-download",
                            "retries": retries + 1,
                        }
                    )
                    logger.info(f"Архив {archive_path.name} перезагружен повторно")
            except Exception as retry_error:
                logger.error(f"Не удалось повторно скачать архив {archive_path.name}: {retry_error}")
                return False
        return True
    
    def _resolve_extract_dir(
        self,
        record: Dict[str, Any],
        tender_folder: Path,
        archive_path: Path,
        base_name: Optional[str] = None,
    ) -> Path:
        """
        Определяет директорию для распаковки архива.
        
        Args:
            record: Запись с метаданными
            tender_folder: Папка тендера
            archive_path: Путь к архиву
            base_name: Базовое имя архива (опционально)
            
        Returns:
            Путь к директории для распаковки
        """
        doc_meta = record.get("doc") or {}
        file_name = doc_meta.get("file_name")
        if base_name is None and file_name:
            base_name, _ = self.selector.split_archive_name(file_name)
        if base_name:
            sanitized = base_name.replace("/", "_")
            return tender_folder / f"extract_{sanitized}"
        return tender_folder / f"extract_{archive_path.stem}"
    
    @staticmethod
    def _remove_file_force(path: Path) -> None:
        """
        Принудительное удаление файла.
        
        Args:
            path: Путь к файлу для удаления
        """
        try:
            if path.exists():
                path.unlink()
        except Exception as error:
            logger.debug(f"Не удалось удалить файл {path}: {error}")

