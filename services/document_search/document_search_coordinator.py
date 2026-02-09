"""
Координатор для поиска информации в документации торгов.

Координирует работу всех модулей поиска документов:
- DocumentSelector - выбор документов
- DocumentDownloader - загрузка документов  
- ArchiveExtractor - извлечение архивов
- MatchFinder - поиск совпадений
- ProgressService - управление прогрессом
- WorkbookProcessor - обработка рабочих книг
- MatchAggregatorService - агрегация совпадений
- TenderFolderService - управление директориями
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger

from core.database import DatabaseManager
from core.exceptions import DocumentSearchError
from services.helpers.archive_cleanup import ArchiveCleanupManager

from .document_downloader import DocumentDownloader
from .document_selector import DocumentSelector
from .archive_extractor import ArchiveExtractor
from .match_finder import MatchFinder
from .progress_service import ProgressService
from .workbook_processor import WorkbookProcessor
from .match_aggregator_service import MatchAggregatorService
from .tender_folder_service import TenderFolderService


class DocumentSearchCoordinator:
    """Координатор поиска информации в документации торгов."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        download_dir: Path,
        unrar_path: Optional[str] = None,
        winrar_path: Optional[str] = None,
        cleanup_manager: Optional[ArchiveCleanupManager] = None,
        progress_callback: Optional[callable] = None,
    ):
        """
        Инициализация координатора.
        
        Args:
            db_manager: Менеджер БД с таблицей products
            download_dir: Директория для сохранения файлов
            unrar_path: Путь к инструменту UnRAR (опционально)
            winrar_path: Путь к директории WinRAR (опционально)
            cleanup_manager: Менеджер очистки временных файлов
            progress_callback: Функция для обновления прогресса (stage, progress, detail)
        """
        self.db_manager = db_manager
        self._product_names: Optional[List[str]] = None
        self.cleanup_manager = cleanup_manager or ArchiveCleanupManager()

        # Инициализация сервисов
        self.progress_service = ProgressService(progress_callback)
        self.folder_service = TenderFolderService(download_dir)
        self.extractor = ArchiveExtractor(unrar_path, winrar_path)
        self.downloader = DocumentDownloader(download_dir, progress_callback)
        self.selector = DocumentSelector()
        self.workbook_processor = WorkbookProcessor(self.extractor)
        self.match_aggregator = MatchAggregatorService()

    def ensure_products_loaded(self) -> None:
        """Ленивая загрузка названий товаров (по требованию пользователя)."""
        if self._product_names is not None:
            return

        logger.info("Загрузка списка товаров для поиска по документации...")
        query = "SELECT name FROM products WHERE name IS NOT NULL"
        results = self.db_manager.execute_query(query)
        self._product_names = [row.get("name", "").strip() for row in results if row.get("name")]
        logger.info(f"Получено наименований товаров: {len(self._product_names)}")

    def run_document_search(
        self,
        documents: List[Dict[str, Any]],
        tender_id: Optional[int] = None,
        registry_type: str = "44fz",
    ) -> Dict[str, Any]:
        """
        Основной сценарий: найти документы, скачать и выполнить поиск.

        Args:
            documents: Метаданные документов торга
            tender_id: ID торга для создания папки
            registry_type: Тип реестра (44fz/223fz)

        Returns:
            Словарь с путем к файлу и найденными совпадениями
        """
        if not documents:
            raise DocumentSearchError("У выбранного торга нет приложенных документов.")

        # Подготовка директории
        self.progress_service.update_progress("Подготовка...", 0, "Выбор документов для обработки")
        tender_folder = self.folder_service.prepare_tender_folder(tender_id, registry_type)

        # Выбор документов
        target_docs = self.selector.choose_documents(documents)
        unique_docs_to_download = self.selector.group_documents_by_archive(target_docs, documents)
        logger.info(f"Найдено уникальных документов/архивов для скачивания: {len(unique_docs_to_download)}")
        
        # Скачивание документов
        downloaded_paths = self._download_documents(unique_docs_to_download, documents, tender_folder)
        
        if not downloaded_paths:
            raise DocumentSearchError("Не удалось скачать ни один документ.")

        # Обработка рабочих книг
        workbook_paths = self.workbook_processor.prepare_workbook_paths(downloaded_paths)
        self.progress_service.update_progress("Извлечение данных", 60, 
                                            f"Найдено файлов для обработки: {len(workbook_paths)}")

        # Поиск совпадений
        self.progress_service.update_progress("Сверка с данными из БД", 60, "Загрузка списка товаров...")
        self.ensure_products_loaded()
        matches = self._find_matches_in_workbooks(workbook_paths)

        self.progress_service.update_progress("Завершено", 100, f"Найдено совпадений: {len(matches)}")
        logger.info(f"Поиск по документации завершен, найдено совпадений: {len(matches)}")

        # Формирование результата
        result = self._prepare_result(workbook_paths, downloaded_paths, matches, tender_folder)
        
        # Очистка временных файлов
        self._cleanup_temp_files(downloaded_paths)

        return result

    def _download_documents(self, unique_docs_to_download: List[Dict[str, Any]], 
                           documents: List[Dict[str, Any]], tender_folder: Path) -> List[Path]:
        """Скачивает документы параллельно."""
        total_to_download = len(unique_docs_to_download)
        self.progress_service.log_stage_start("Скачивание документов", 
                                            f"Начинаю параллельное скачивание {total_to_download} документов/архивов")
        
        all_downloaded_paths: List[Path] = []
        downloaded_count = 0
        
        with ThreadPoolExecutor(max_workers=min(8, total_to_download)) as executor:
            future_to_doc = {
                executor.submit(
                    self.downloader.download_required_documents,
                    target_doc,
                    documents,
                    tender_folder,
                ): target_doc
                for target_doc in unique_docs_to_download
            }
            
            for future in as_completed(future_to_doc):
                target_doc = future_to_doc[future]
                doc_name = target_doc.get('file_name') or target_doc.get('document_links', 'unknown')
                
                try:
                    downloaded_paths = future.result(timeout=600)
                    all_downloaded_paths.extend(downloaded_paths)
                    downloaded_count += 1
                    
                    progress = int((downloaded_count / total_to_download) * 30) if total_to_download > 0 else 0
                    self.progress_service.log_stage_progress(
                        "Скачивание документов",
                        progress,
                        downloaded_count,
                        total_to_download,
                        doc_name
                    )
                    logger.info(f"Успешно скачан документ/архив: {doc_name}")
                    
                except Exception as error:
                    logger.error(f"Ошибка при скачивании документа {doc_name}: {error}")
                    continue
        
        self.progress_service.update_progress("Скачивание документов", 30, 
                                            f"Скачано документов: {downloaded_count}/{total_to_download}")
        return all_downloaded_paths

    def _find_matches_in_workbooks(self, workbook_paths: List[Path]) -> List[Dict[str, Any]]:
        """Выполняет поиск совпадений во всех рабочих книгах."""
        finder = MatchFinder(self._product_names)
        all_matches: List[List[Dict[str, Any]]] = []
        total_files = len(workbook_paths)
        
        for idx, workbook_path in enumerate(workbook_paths):
            progress = 60 + int(((idx + 1) / total_files) * 35) if total_files > 0 else 60
            file_name = workbook_path.name
            
            self.progress_service.log_stage_progress(
                "Сверка с данными из БД",
                progress,
                idx + 1,
                total_files,
                file_name
            )
            
            logger.info(f"Поиск по документу: {workbook_path}")
            matches = finder.search_workbook_for_products(workbook_path)
            matches_with_source = self.match_aggregator.merge_matches_with_source(matches, str(workbook_path))
            all_matches.append(matches_with_source)
        
        return self.match_aggregator.aggregate_matches(all_matches)

    def _prepare_result(self, workbook_paths: List[Path], downloaded_paths: List[Path], 
                       matches: List[Dict[str, Any]], tender_folder: Path) -> Dict[str, Any]:
        """Формирует итоговый результат поиска."""
        return {
            "file_path": str(workbook_paths[0]) if workbook_paths else "",
            "matches": matches,
            "tender_folder": str(tender_folder),
            "downloaded_files": [str(path) for path in downloaded_paths],
            "extract_dirs": [str(path) for path in self.extractor.active_extract_dirs],
        }

    def _cleanup_temp_files(self, downloaded_paths: List[Path]) -> None:
        """Очищает временные файлы."""
        try:
            self.cleanup_manager.cleanup(
                downloaded_paths,
                self.extractor.active_extract_dirs,
                [],  # matches не используются для очистки
            )
        except Exception as cleanup_error:
            logger.warning(f"Не удалось очистить временные файлы: {cleanup_error}")