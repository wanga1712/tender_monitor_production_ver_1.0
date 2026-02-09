"""
Фасад для обратной совместимости с TenderMatchRepository.

Этот фасад предоставляет тот же интерфейс, что и оригинальный TenderMatchRepository,
но делегирует работу специализированным сервисам.
"""

from typing import Optional, Dict, Any, List, Sequence
from loguru import logger
from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseQueryError, DatabaseConnectionError

from services.match_finder import MatchFinder
from services.match_validator import MatchValidator
from services.interest_calculator import InterestCalculator

from .match_coordinator import MatchCoordinator
from .match_result_service import MatchResultService
from .match_query_service import MatchQueryService
from .match_status_service import MatchStatusService
from .tender_lock_service import TenderLockService
from .match_detail_service import MatchDetailService
from .file_error_service import FileErrorService


class TenderMatchRepositoryFacade:
    """
    Фасад для обратной совместимости с TenderMatchRepository.
    
    Предоставляет тот же интерфейс, что и оригинальный TenderMatchRepository,
    но делегирует работу специализированным сервисам через MatchCoordinator.
    """
    
    def __init__(self, db_manager: TenderDatabaseManager):
        """Инициализация фасада."""
        self.db_manager = db_manager
        
        # Сохраняем оригинальные зависимости для совместимости
        self.match_finder = MatchFinder(db_manager)
        self.match_validator = MatchValidator(db_manager)
        
        # Создаем координатор, который будет оркестрировать работу сервисов
        self.coordinator = MatchCoordinator(db_manager)
    
    # region Методы для обратной совместимости
    
    def save_match_result(
        self,
        tender_id: int,
        registry_type: str,
        match_count: int,
        match_percentage: float,
        processing_time_seconds: Optional[float] = None,
        total_files_processed: int = 0,
        total_size_bytes: int = 0,
        error_reason: Optional[str] = None,
        folder_name: Optional[str] = None,
        has_error: bool = False,
    ) -> Optional[int]:
        """
        Сохранение или обновление результата поиска совпадений.
        
        Делегирует работу MatchResultService через координатор.
        """
        # Используем InterestCalculator для бизнес-логики (как в оригинале)
        is_interesting_value = InterestCalculator.calculate_interest(
            match_count, match_percentage, error_reason
        )
        
        return self.coordinator.match_result_service.save_match_result(
            tender_id=tender_id,
            registry_type=registry_type,
            match_count=match_count,
            match_percentage=match_percentage,
            processing_time_seconds=processing_time_seconds,
            total_files_processed=total_files_processed,
            total_size_bytes=total_size_bytes,
            error_reason=error_reason,
            folder_name=folder_name,
            has_error=has_error,
            is_interesting=is_interesting_value
        )
    
    def save_match_details(self, match_id: int, details: Sequence[Dict[str, Any]]) -> None:
        """
        Сохраняет детальные совпадения для записи tender_document_matches.
        
        Делегирует работу MatchDetailService через координатор.
        """
        self.coordinator.match_detail_service.save_match_details(match_id, details)
    
    def save_file_errors(
        self,
        match_id: int,
        failed_files: List[Dict[str, Any]],
    ) -> None:
        """
        Сохраняет информацию об ошибках обработки файлов.
        
        Делегирует работу FileErrorService через координатор.
        """
        self.coordinator.file_error_service.save_file_errors(match_id, failed_files)
    
    def get_match_result(self, tender_id: int, registry_type: str) -> Optional[Dict[str, Any]]:
        """
        Получает результат поиска совпадений по ID закупки и типу реестра.
        
        Делегирует работу MatchQueryService через координатор.
        """
        return self.coordinator.match_query_service.get_match_result(tender_id, registry_type)
    
    def get_match_result_by_folder_name(self, folder_name: str) -> Optional[Dict[str, Any]]:
        """
        Получает результат поиска совпадений по имени папки.
        
        Делегирует работу MatchQueryService через координатор.
        """
        return self.coordinator.match_query_service.get_match_result_by_folder_name(folder_name)
    
    def get_match_summary(self, tender_id: int, registry_type: str) -> Dict[str, Any]:
        """
        Получает сводную информацию по результатам поиска.
        
        Делегирует работу MatchQueryService через координатор.
        """
        return self.coordinator.match_query_service.get_match_summary(tender_id, registry_type)
    
    def get_match_details(self, match_id: int) -> List[Dict[str, Any]]:
        """
        Получает детали совпадений для указанного match_id.
        
        Делегирует работу MatchDetailService через координатор.
        """
        return self.coordinator.match_detail_service.get_match_details(match_id)
    
    def get_match_results_batch(self, tender_ids: List[int], registry_type: str) -> List[Dict[str, Any]]:
        """
        Получает результаты поиска для пакета закупок.
        
        Делегирует работу MatchQueryService через координатор.
        """
        return self.coordinator.match_query_service.get_match_results_batch(tender_ids, registry_type)
    
    def set_interesting_status(self, match_id: int, is_interesting: bool) -> bool:
        """
        Устанавливает статус "интересно" для результата поиска.
        
        Делегирует работу MatchStatusService через координатор.
        """
        return self.coordinator.match_status_service.set_interesting_status(match_id, is_interesting)
    
    def filter_uninteresting_tenders(self, tender_ids: List[int], registry_type: str) -> List[int]:
        """
        Фильтрует неинтересные тендеры из списка.
        
        Делегирует работу MatchStatusService через координатор.
        """
        return self.coordinator.match_status_service.filter_uninteresting_tenders(tender_ids, registry_type)
    
    def acquire_tender_lock(self, tender_id: int, registry_type: str, timeout_seconds: int = 300) -> bool:
        """
        Получает блокировку для тендера.
        
        Делегирует работу TenderLockService через координатор.
        """
        return self.coordinator.tender_lock_service.acquire_tender_lock(tender_id, registry_type, timeout_seconds)
    
    def release_tender_lock(self, tender_id: int, registry_type: str) -> bool:
        """
        Освобождает блокировку для тендера.
        
        Делегирует работу TenderLockService через координатор.
        """
        return self.coordinator.tender_lock_service.release_tender_lock(tender_id, registry_type)
    
    def _table_exists(self, table_name: str) -> bool:
        """
        Проверяет существование таблицы в базе данных.
        
        Делегирует работу базовому сервису через координатор.
        """
        # Используем любой сервис, у которого есть этот метод
        return self.coordinator.match_result_service._table_exists(table_name)
    
    def _fetch_match_id(self, tender_id: int, registry_type: str) -> Optional[int]:
        """
        Получает ID совпадения по ID закупки и типу реестра.
        
        Делегирует работу MatchQueryService через координатор.
        """
        result = self.coordinator.match_query_service.get_match_result(tender_id, registry_type)
        return result.get('id') if result else None
    
    # endregion
    
    # region Свойства для обратной совместимости
    
    @property
    def db_manager(self):
        """Возвращает менеджер базы данных для обратной совместимости."""
        return self._db_manager
    
    @db_manager.setter
    def db_manager(self, value):
        """Устанавливает менеджер базы данных."""
        self._db_manager = value
    
    # endregion