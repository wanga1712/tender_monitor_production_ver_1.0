"""
Главный координатор обработки архивов.
Отвечает за оркестрацию всех сервисов обработки.
"""

from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

from core.tender_database import TenderDatabaseManager
from services.tender_repositories.tender_documents_repository import TenderDocumentsRepository
from services.match_services.tender_match_repository_facade import TenderMatchRepositoryFacade
from ..archive_runner.processed_tenders_repository import ProcessedTendersRepository
from services.document_search_service import DocumentSearchService
from services.document_search.document_selector import DocumentSelector
from services.document_search.download_timeout_calculator import create_timeout_calculator

from .download_service import TenderDownloadService
from .processing_service import DocumentProcessingService
from .match_service import MatchDetectionService
from .cleanup_service import FileCleanupService
from .cloud_service import CloudUploadService
from .error_service import ErrorHandlingService
from .queue_service import TenderQueueService
from .tender_coordinator import TenderProcessingCoordinator

logger = logging.getLogger(__name__)


class ArchiveProcessingCoordinator:
    """Главный координатор для обработки архивов документов тендеров."""
    
    def __init__(
        self,
        tender_db_manager: TenderDatabaseManager,
        product_db_manager: TenderDatabaseManager,
        download_dir: Path,
        config: Any,
        max_workers: int = 4,
        batch_delay: float = 10.0
    ):
        """
        Инициализация главного координатора обработки архивов.
        
        Args:
            tender_db_manager: Менеджер БД для данных тендеров
            product_db_manager: Менеджер БД для продуктов
            download_dir: Директория для скачивания файлов
            config: Конфигурация приложения
            max_workers: Максимальное количество рабочих процессов
            batch_delay: Задержка между батчами обработки
        """
        self.tender_db_manager = tender_db_manager
        self.product_db_manager = product_db_manager
        self.download_dir = download_dir
        self.config = config
        self.max_workers = max_workers
        self.batch_delay = batch_delay
        
        # Инициализация репозиториев
        self.tender_repo = TenderDocumentsRepository(tender_db_manager)
        self.tender_match_repo = TenderMatchRepository(tender_db_manager)
        self.processed_tenders_repo = ProcessedTendersRepository(tender_db_manager)
        
        # Инициализация сервисов
        self.download_service = self._create_download_service()
        self.processing_service = self._create_processing_service()
        self.match_service = self._create_match_service()
        self.cleanup_service = self._create_cleanup_service()
        self.cloud_service = self._create_cloud_service()
        self.error_service = self._create_error_service()
        self.queue_service = self._create_queue_service()
        
        # Инициализация координатора обработки торгов
        self.tender_coordinator = self._create_tender_coordinator()
    
    def _create_download_service(self) -> TenderDownloadService:
        """Создание сервиса скачивания документов."""
        timeout_calculator = create_timeout_calculator(self.tender_db_manager)
        return TenderDownloadService(self.download_dir, timeout_calculator)
    
    def _create_processing_service(self) -> DocumentProcessingService:
        """Создание сервиса обработки документов."""
        return DocumentProcessingService(
            unrar_path=self.config.unrar_tool,
            winrar_path=self.config.winrar_path
        )
    
    def _create_match_service(self) -> MatchDetectionService:
        """Создание сервиса поиска совпадений."""
        # Загружаем стоп-фразы из БД
        document_stop_phrases = []
        try:
            document_stop_phrases_rows = getattr(self.tender_repo, "get_document_stop_phrases", lambda _uid: [])(1)
            document_stop_phrases = [
                row.get("phrase", "").strip()
                for row in document_stop_phrases_rows
                if row.get("phrase")
            ]
        except Exception:
            logger.warning("Не удалось загрузить стоп-фразы из БД")
        
        # Загружаем названия продуктов
        document_search_service = DocumentSearchService(
            self.product_db_manager, self.download_dir,
            unrar_path=self.config.unrar_tool,
            winrar_path=self.config.winrar_path
        )
        document_search_service.ensure_products_loaded()
        
        return MatchDetectionService(
            product_names=document_search_service._product_names,
            stop_phrases=document_stop_phrases,
            user_search_phrases=[]
        )
    
    def _create_cleanup_service(self) -> FileCleanupService:
        """Создание сервиса очистки файлов."""
        return FileCleanupService(self.download_dir)
    
    def _create_cloud_service(self) -> CloudUploadService:
        """Создание сервиса облачной загрузки."""
        return CloudUploadService(None)  # TODO: Добавить Яндекс.Диск при необходимости
    
    def _create_error_service(self) -> ErrorHandlingService:
        """Создание сервиса обработки ошибок."""
        return ErrorHandlingService(max_retries=3, retry_delay=2.0)
    
    def _create_queue_service(self) -> TenderQueueService:
        """Создание сервиса управления очередями."""
        return TenderQueueService()
    
    def _create_tender_coordinator(self) -> TenderProcessingCoordinator:
        """Создание координатора обработки торгов."""
        return TenderProcessingCoordinator(
            download_service=self.download_service,
            processing_service=self.processing_service,
            match_service=self.match_service,
            cleanup_service=self.cleanup_service,
            cloud_service=self.cloud_service,
            error_service=self.error_service,
            queue_service=self.queue_service,
            max_workers=self.max_workers
        )
    
    def run(self, specific_tender_ids: Optional[List[Dict[str, Any]]] = None, 
            registry_type: Optional[str] = None, tender_type: str = 'new') -> Dict[str, Any]:
        """
        Запуск полного цикла обработки архивов.
        
        Args:
            specific_tender_ids: Список конкретных тендеров для обработки
            registry_type: Тип реестра для фильтрации
            tender_type: Тип торгов ('new' или 'won')
            
        Returns:
            Результаты обработки
        """
        logger.info(f"Запуск обработки архивов. Тип: {tender_type}, Реестр: {registry_type}")
        
        try:
            # Обрабатываем через координатор торгов
            results = self.tender_coordinator.process(
                specific_tender_ids=specific_tender_ids,
                registry_type=registry_type,
                tender_type=tender_type
            )
            
            logger.info(f"Обработка завершена. Успешно: {results.get('successful', 0)}, "
                       f"Ошибок: {results.get('failed', 0)}")
            
            return results
            
        except Exception as e:
            logger.error(f"Критическая ошибка при запуске обработки: {e}")
            return {
                'processed': 0,
                'successful': 0,
                'failed': 0,
                'errors': [{'error': f'Critical error: {e}'}]
            }
    
    def process_existing_folders(self, registry_type: Optional[str] = None, 
                                tender_type: str = 'new') -> int:
        """
        Обработка существующих папок с документами.
        
        Args:
            registry_type: Тип реестра для фильтрации
            tender_type: Тип торгов
            
        Returns:
            Количество обработанных папок
        """
        logger.info(f"Обработка существующих папок. Реестр: {registry_type}")
        
        try:
            # Для существующих папок tender_type не используется в фильтрации
            processed_count = self.tender_coordinator.process_existing_folders(registry_type)
            logger.info(f"Обработано существующих папок: {processed_count}")
            return processed_count
            
        except Exception as e:
            logger.error(f"Ошибка обработки существующих папок: {e}")
            return 0
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Получение статистики обработки.
        
        Returns:
            Статистика обработки
        """
        return self.tender_coordinator.get_processing_stats()
    
    def cleanup(self):
        """Очистка ресурсов и временных файлов."""
        try:
            self.cleanup_service.cleanup_temporary_files()
            self.cleanup_service.optimize_storage()
            logger.info("Очистка ресурсов завершена")
        except Exception as e:
            logger.error(f"Ошибка при очистке ресурсов: {e}")