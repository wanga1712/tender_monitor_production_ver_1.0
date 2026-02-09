"""
MODULE: services.document_search_service
RESPONSIBILITY: Coordinate document search operations (download, extract, match).
ALLOWED: DatabaseManager, DocumentDownloader, ArchiveExtractor, MatchFinder.
FORBIDDEN: Direct DB queries (use managers), UI logic.
ERRORS: DocumentSearchError.

Сервис для скачивания документации торгов и поиска товаров внутри Excel-файлов.

Рефакторенная версия с разделением на модули.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from core.database import DatabaseManager
from core.exceptions import DocumentSearchError
from services.helpers.archive_cleanup import ArchiveCleanupManager
from services.document_search.document_downloader import DocumentDownloader
from services.document_search.document_selector import DocumentSelector
from services.document_search.archive_extractor import ArchiveExtractor
from services.document_search.match_finder import MatchFinder
from services.document_search.product_loader import ProductLoader
from services.document_search.search_coordinator import DocumentSearchCoordinator


class DocumentSearchService:
    """
    Сервис поиска информации в документации торгов.
    
    Координирует работу модулей:
    - DocumentSelector - выбор документов
    - DocumentDownloader - загрузка документов
    - ArchiveExtractor - извлечение архивов
    - MatchFinder - поиск совпадений
    """

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
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.cleanup_manager = cleanup_manager or ArchiveCleanupManager()
        self.progress_callback = progress_callback

        # Инициализируем модули
        self._downloader = DocumentDownloader(self.download_dir, progress_callback)
        self._selector = DocumentSelector()
        self._extractor = ArchiveExtractor(unrar_path, winrar_path)
        
        # Инициализируем загрузчик товаров и координатор
        self.product_loader = ProductLoader(db_manager)
        self.coordinator = DocumentSearchCoordinator(
            download_dir=self.download_dir,
            downloader=self._downloader,
            selector=self._selector,
            extractor=self._extractor,
            cleanup_manager=self.cleanup_manager,
            progress_callback=progress_callback,
        )
        
        # Для обратной совместимости
        self._product_names: Optional[List[str]] = None

    def _update_progress(self, stage: str, progress: int, detail: Optional[str] = None):
        """Обновление прогресса через callback"""
        if self.progress_callback:
            try:
                self.progress_callback(stage, progress, detail)
            except Exception as error:
                logger.debug(f"Ошибка при обновлении прогресса: {error}")

    def ensure_products_loaded(self) -> None:
        """Ленивая загрузка названий товаров (по требованию пользователя)."""
        self.product_loader.ensure_products_loaded()
        self._product_names = self.product_loader.product_names

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

        self.coordinator._update_progress("Подготовка...", 0, "Выбор документов для обработки")
        tender_folder = self.coordinator.prepare_tender_folder(tender_id, registry_type)

        target_docs = self._selector.choose_documents(documents)
        unique_docs_to_download = self._selector.group_documents_by_archive(target_docs, documents)
        logger.info(f"Найдено уникальных документов/архивов для скачивания: {len(unique_docs_to_download)}")

        # ЭТАП 1: Скачивание документов
        successful_downloads = self.coordinator.download_documents(
            unique_docs_to_download,
            documents,
            tender_folder,
        )

        # ЭТАП 2: Извлечение данных
        self.coordinator._update_progress("Извлечение данных", 30, "Распаковка архивов и подготовка файлов...")
        workbook_paths = self.coordinator.prepare_workbook_paths(successful_downloads)
        self.coordinator._update_progress("Извлечение данных", 60, f"Найдено файлов для обработки: {len(workbook_paths)}")
        
        # ЭТАП 3: Сверка с данными из БД
        self.coordinator._update_progress("Сверка с данными из БД", 60, "Загрузка списка товаров...")
        self.ensure_products_loaded()
        matches = self.coordinator.aggregate_matches_for_workbooks(
            workbook_paths,
            self._product_names or [],
        )

        self.coordinator._update_progress("Завершено", 100, f"Найдено совпадений: {len(matches)}")
        logger.info(f"Поиск по документации завершен, найдено совпадений: {len(matches)}")

        result = {
            "file_path": str(workbook_paths[0]) if workbook_paths else "",
            "matches": matches,
            "tender_folder": str(tender_folder),
            "downloaded_files": [str(path) for path in successful_downloads],
            "extract_dirs": [str(path) for path in self._extractor.active_extract_dirs],
        }

        try:
            self.cleanup_manager.cleanup(
                successful_downloads,
                self._extractor.active_extract_dirs,
                matches,
            )
        except Exception as cleanup_error:
            logger.warning(f"Не удалось очистить временные файлы: {cleanup_error}")

        return result


    def debug_process_local_archives(
        self,
        archive_paths: List[str],
    ) -> Dict[str, Any]:
        """
        Обрабатывает локальные архивы (уже скачанные).
        
        Args:
            archive_paths: Список путей к архивам (строки)
            
        Returns:
            Словарь с результатами:
            - workbook_paths: список путей к Excel файлам
            - matches: список найденных совпадений
            - extract_dirs: список директорий с распакованными файлами
        """
        archive_path_objects = [Path(path) for path in archive_paths]
        
        # Извлекаем Excel файлы из архивов
        workbook_paths = self.coordinator.prepare_workbook_paths(archive_path_objects)
        
        # Загружаем товары и ищем совпадения
        self.ensure_products_loaded()
        matches = self.coordinator.aggregate_matches_for_workbooks(workbook_paths, self._product_names or [])
        
        return {
            "workbook_paths": [str(path) for path in workbook_paths],
            "matches": matches,
            "extract_dirs": [str(path) for path in self._extractor.active_extract_dirs],
        }

