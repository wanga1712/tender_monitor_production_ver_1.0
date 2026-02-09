"""
MODULE: core.dependency_injection
RESPONSIBILITY: Central Dependency Injection Container (Singleton).
ALLOWED: Importing all services and repositories.
FORBIDDEN: Business logic.
ERRORS: None.

Контейнер зависимостей для внедрения зависимостей (Dependency Injection)

Обеспечивает централизованное создание и управление зависимостями,
уменьшая связанность между модулями.
"""

from typing import Optional, Dict, Any
from loguru import logger

from core.tender_database import TenderDatabaseManager
from core.database import DatabaseManager
from services.tender_services.tender_repository_facade import TenderRepositoryFacade
from services.document_search_service import DocumentSearchService
from services.match_services.tender_match_repository_facade import TenderMatchRepositoryFacade
from config.settings import config


class DependencyContainer:
    """
    Контейнер зависимостей для управления жизненным циклом сервисов
    
    Реализует паттерн Singleton для обеспечения единой точки доступа
    к зависимостям во всем приложении.
    """
    
    _instance: Optional['DependencyContainer'] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self._tender_db_manager: Optional[TenderDatabaseManager] = None
        self._commercial_db_manager: Optional[DatabaseManager] = None
        self._tender_repository: Optional[TenderRepositoryFacade] = None
        self._tender_match_repository: Optional[TenderMatchRepositoryFacade] = None
        self._tender_match_repository_facade: Optional[TenderMatchRepositoryFacade] = None
        self._document_search_service: Optional[DocumentSearchService] = None
    
    def get_tender_database_manager(self) -> TenderDatabaseManager:
        """Получение менеджера базы данных tender_monitor"""
        if self._tender_db_manager is None:
            logger.info("Создание TenderDatabaseManager")
            self._tender_db_manager = TenderDatabaseManager(config.tender_database)
            try:
                self._tender_db_manager.connect(fallback_to_offline=True)
            except Exception as e:
                logger.error(f"Критическая ошибка при подключении к БД tender_monitor: {e}")
                # Даже если fallback_to_offline не сработал, продолжаем работу
        return self._tender_db_manager
    
    def get_commercial_database_manager(self) -> DatabaseManager:
        """Получение менеджера базы данных commercial_db"""
        if self._commercial_db_manager is None:
            logger.info("Создание DatabaseManager")
            self._commercial_db_manager = DatabaseManager(config.database)
            self._commercial_db_manager.connect()
        return self._commercial_db_manager
    
    def get_tender_repository(self) -> TenderRepositoryFacade:
        """Получение репозитория торгов"""
        if self._tender_repository is None:
            logger.info("Создание TenderRepositoryFacade")
            db_manager = self.get_tender_database_manager()
            self._tender_repository = TenderRepositoryFacade(db_manager)
        return self._tender_repository
    
    def get_tender_match_repository(self) -> TenderMatchRepositoryFacade:
        """Получение репозитория соответствий торгов"""
        if self._tender_match_repository is None:
            logger.info("Создание TenderMatchRepositoryFacade")
            db_manager = self.get_tender_database_manager()
            self._tender_match_repository = TenderMatchRepositoryFacade(db_manager)
        return self._tender_match_repository
    
    def get_tender_match_repository_facade(self) -> TenderMatchRepositoryFacade:
        """Получение фасада репозитория результатов поиска"""
        if self._tender_match_repository_facade is None:
            logger.info("Создание TenderMatchRepositoryFacade")
            db_manager = self.get_tender_database_manager()
            self._tender_match_repository_facade = TenderMatchRepositoryFacade(db_manager)
        return self._tender_match_repository_facade
    
    def get_document_search_service(self) -> DocumentSearchService:
        """Получение сервиса поиска документов"""
        if self._document_search_service is None:
            logger.info("Создание DocumentSearchService")
            commercial_db = self.get_commercial_database_manager()
            # Получаем путь к директории для скачивания документов из .env
            from pathlib import Path
            from config.settings import config
            download_dir = Path(config.document_download_dir) if config.document_download_dir else Path.home() / "Downloads" / "ЕИС_Документация"
            self._document_search_service = DocumentSearchService(
                commercial_db_manager=commercial_db,
                download_dir=download_dir,
                unrar_path=config.unrar_tool,
                winrar_path=config.winrar_path,
            )
        return self._document_search_service
    
    def cleanup(self):
        """Очистка ресурсов при завершении работы приложения"""
        logger.info("Очистка зависимостей")
        
        if self._tender_db_manager:
            self._tender_db_manager.disconnect()
            self._tender_db_manager = None
        
        if self._commercial_db_manager:
            self._commercial_db_manager.disconnect()
            self._commercial_db_manager = None
        
        self._tender_repository = None
        self._tender_match_repository = None
        self._tender_match_repository_facade = None
        self._document_search_service = None


# Глобальный экземпляр контейнера
container = DependencyContainer()

