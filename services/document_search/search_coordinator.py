"""
MODULE: services.document_search.search_coordinator
RESPONSIBILITY: Coordinate the entire document search process (directory prep, downloading, processing).
ALLOWED: document_selector, document_downloader, archive_extractor, document_downloader_coordinator, workbook_preparator, match_aggregator, helpers.archive_cleanup, logging, shutil.
FORBIDDEN: Direct lower-level parsing logic.
ERRORS: None.

Модуль для координации процесса поиска документов.
"""

import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from services.document_search.document_selector import DocumentSelector
from services.document_search.document_downloader import DocumentDownloader
from services.document_search.archive_extractor import ArchiveExtractor
from services.document_search.document_downloader_coordinator import DocumentDownloaderCoordinator
from services.document_search.workbook_preparator import WorkbookPreparator
from services.document_search.match_aggregator import MatchAggregator
from services.helpers.archive_cleanup import ArchiveCleanupManager


class DocumentSearchCoordinator:
    """Класс для координации процесса поиска документов"""
    
    def __init__(
        self,
        download_dir: Path,
        downloader: DocumentDownloader,
        selector: DocumentSelector,
        extractor: ArchiveExtractor,
        cleanup_manager: ArchiveCleanupManager,
        progress_callback: Optional[callable] = None,
    ):
        """
        Инициализация координатора
        
        Args:
            download_dir: Директория для сохранения файлов
            downloader: Загрузчик документов
            selector: Селектор документов
            extractor: Экстрактор архивов
            cleanup_manager: Менеджер очистки временных файлов
            progress_callback: Функция для обновления прогресса
        """
        self.download_dir = download_dir
        self.selector = selector
        self.extractor = extractor
        self.cleanup_manager = cleanup_manager
        self.progress_callback = progress_callback
        
        # Инициализируем координаторы
        self.download_coordinator = DocumentDownloaderCoordinator(downloader, progress_callback)
        self.workbook_preparator = WorkbookPreparator(selector, extractor)
        self.match_aggregator = MatchAggregator(progress_callback)
        
        # Инициализируем координаторы
        self.download_coordinator = DocumentDownloaderCoordinator(downloader, progress_callback)
        self.workbook_preparator = WorkbookPreparator(selector, extractor)
        self.match_aggregator = MatchAggregator(progress_callback)
    
    def _update_progress(self, stage: str, progress: int, detail: Optional[str] = None):
        """Обновление прогресса через callback"""
        if self.progress_callback:
            try:
                self.progress_callback(stage, progress, detail)
            except Exception as error:
                logger.debug(f"Ошибка при обновлении прогресса: {error}")
    
    def prepare_tender_folder(
        self,
        tender_id: Optional[int],
        registry_type: Optional[str],
    ) -> Path:
        """Создает рабочую директорию для файлов торга."""
        if not tender_id:
            fallback_dir = self.download_dir / "tender_temp"
            fallback_dir.mkdir(parents=True, exist_ok=True)
            return fallback_dir

        safe_type = (registry_type or "tender").strip().lower() or "tender"
        folder_name = f"{safe_type}_{tender_id}"
        target_dir = self.download_dir / folder_name
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir
    
    def prepare_workbook_paths(self, downloaded_paths: List[Path]) -> List[Path]:
        """Определение путей ко всем Excel файлам"""
        return self.workbook_preparator.prepare_workbook_paths(downloaded_paths)
    
    def aggregate_matches_for_workbooks(
        self,
        workbook_paths: List[Path],
        product_names: List[str],
    ) -> List[Dict[str, Any]]:
        """Выполняет поиск по всем Excel и объединяет совпадения"""
        return self.match_aggregator.aggregate_matches_for_workbooks(workbook_paths, product_names)
    
    def handle_failed_download_attempt(
        self,
        tender_folder: Path,
        downloaded_files: List[Path],
    ) -> None:
        """Удаляет скачанные файлы и очищает папку после неудачной попытки."""
        extract_dirs = self.extractor.active_extract_dirs
        try:
            self.cleanup_manager.cleanup(downloaded_files, extract_dirs, [])
        except Exception as cleanup_error:
            logger.warning(f"Не удалось полностью очистить временные файлы: {cleanup_error}")
        finally:
            self.extractor.clear_active_extract_dirs()
            self.reset_tender_folder(tender_folder)
    
    @staticmethod
    def reset_tender_folder(tender_folder: Path) -> None:
        """Полностью очищает папку скачивания для торга."""
        try:
            if tender_folder.exists():
                shutil.rmtree(tender_folder, ignore_errors=True)
        except Exception as error:
            logger.warning(f"Не удалось удалить директорию {tender_folder}: {error}")
        tender_folder.mkdir(parents=True, exist_ok=True)
    
    def download_documents(
        self,
        unique_docs_to_download: List[Dict[str, Any]],
        documents: List[Dict[str, Any]],
        tender_folder: Path,
    ) -> List[Path]:
        """Скачивание документов с повторными попытками"""
        return self.download_coordinator.download_documents(
            unique_docs_to_download,
            documents,
            tender_folder,
        )

