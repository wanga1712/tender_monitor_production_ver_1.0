"""
Фасад для обратной совместимости с оригинальным DocumentSearchService.

Предоставляет тот же интерфейс, что и оригинальный DocumentSearchService,
но использует декомпозированную архитектуру через координатор сервисов.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from core.database import DatabaseManager
from core.exceptions import DocumentSearchError
from services.helpers.archive_cleanup import ArchiveCleanupManager

from .document_search_coordinator import DocumentSearchCoordinator


class DocumentSearchFacade:
    """Фасад для обратной совместимости с DocumentSearchService."""

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
        Инициализация фасада.
        
        Args:
            db_manager: Менеджер БД с таблицей products
            download_dir: Директория для сохранения файлов
            unrar_path: Путь к инструменту UnRAR (опционально)
            winrar_path: Путь к директории WinRAR (опционально)
            cleanup_manager: Менеджер очистки временных файлов
            progress_callback: Функция для обновления прогресса (stage, progress, detail)
        """
        self.db_manager = db_manager
        self.download_dir = Path(download_dir)
        self.unrar_path = unrar_path
        self.winrar_path = winrar_path
        self.cleanup_manager = cleanup_manager
        self.progress_callback = progress_callback
        self._product_names: Optional[List[str]] = None
        
        # Инициализация координатора
        self.coordinator = DocumentSearchCoordinator(
            db_manager=db_manager,
            download_dir=download_dir,
            unrar_path=unrar_path,
            winrar_path=winrar_path,
            cleanup_manager=cleanup_manager,
            progress_callback=progress_callback,
        )

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
        return self.coordinator.run_document_search(documents, tender_id, registry_type)

    def ensure_products_loaded(self) -> None:
        """Ленивая загрузка названий товаров (по требованию пользователя)."""
        self.coordinator.ensure_products_loaded()

    def _update_progress(self, stage: str, progress: int, detail: Optional[str] = None):
        """Обновление прогресса через callback (для совместимости)."""
        if self.progress_callback:
            try:
                self.progress_callback(stage, progress, detail)
            except Exception as error:
                from loguru import logger
                logger.debug(f"Ошибка при обновлении прогресса: {error}")

    # Методы для совместимости с оригинальным интерфейсом
    def _prepare_tender_folder(
        self,
        tender_id: Optional[int],
        registry_type: Optional[str],
    ) -> Path:
        """Создает рабочую директорию для файлов торга."""
        from .tender_folder_service import TenderFolderService
        folder_service = TenderFolderService(self.download_dir)
        return folder_service.prepare_tender_folder(tender_id, registry_type)

    def _prepare_workbook_paths(self, downloaded_paths: List[Path]) -> List[Path]:
        """Определение путей ко всем Excel файлам."""
        from .workbook_processor import WorkbookProcessor
        from .archive_extractor import ArchiveExtractor
        
        extractor = ArchiveExtractor(self.unrar_path, self.winrar_path)
        processor = WorkbookProcessor(extractor)
        return processor.prepare_workbook_paths(downloaded_paths)

    def _aggregate_matches_for_workbooks(self, workbook_paths: List[Path]) -> List[Dict[str, Any]]:
        """Выполняет поиск по всем Excel и объединяет совпадения."""
        from .match_aggregator_service import MatchAggregatorService
        from .match_finder import MatchFinder
        
        if self._product_names is None:
            self.ensure_products_loaded()

        finder = MatchFinder(self._product_names)
        aggregator = MatchAggregatorService()
        
        all_matches: List[List[Dict[str, Any]]] = []
        
        for workbook_path in workbook_paths:
            matches = finder.search_workbook_for_products(workbook_path)
            matches_with_source = aggregator.merge_matches_with_source(matches, str(workbook_path))
            all_matches.append(matches_with_source)
        
        return aggregator.aggregate_matches(all_matches)