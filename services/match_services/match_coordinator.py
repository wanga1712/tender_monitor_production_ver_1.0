"""
MODULE: services.match_services.match_coordinator
RESPONSIBILITY: Главный координатор для работы с результатами поиска совпадений
ALLOWED: MatchResultService, MatchQueryService, MatchStatusService, TenderLockService
FORBIDDEN: Прямая бизнес-логика, прямые проверки БД
ERRORS: Должен пробрасывать DatabaseQueryError, DatabaseConnectionError

Главный координатор для работы с результатами поиска совпадений.
Делегирует работу специализированным сервисам.
"""

from typing import Optional, Dict, Any, List, Sequence
from datetime import datetime
from pathlib import Path
import json
import time
from loguru import logger

from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseQueryError, DatabaseConnectionError
from psycopg2.extras import RealDictCursor

from services.match_finder import MatchFinder
from services.match_validator import MatchValidator
from services.interest_calculator import InterestCalculator
from .match_result_service import MatchResultService
from .match_query_service import MatchQueryService
from .match_status_service import MatchStatusService
from .tender_lock_service import TenderLockService
from .match_detail_service import MatchDetailService
from .file_error_service import FileErrorService


class MatchCoordinator:
    """
    Главный координатор для работы с результатами поиска совпадений
    """
    
    def __init__(self, db_manager: TenderDatabaseManager):
        """
        Инициализация координатора
        
        Args:
            db_manager: Менеджер базы данных tender_monitor
        """
        self.db_manager = db_manager
        self.match_finder = MatchFinder(db_manager)
        self.match_validator = MatchValidator(db_manager)
        self.match_result_service = MatchResultService(db_manager)
        self.match_query_service = MatchQueryService(db_manager)
        self.match_status_service = MatchStatusService(db_manager)
        self.tender_lock_service = TenderLockService(db_manager)
        self.match_detail_service = MatchDetailService(db_manager)
        self.file_error_service = FileErrorService(db_manager)
    
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
        Сохранение или обновление результата поиска совпадений
        
        Args:
            tender_id: ID закупки
            registry_type: Тип реестра ('44fz' или '223fz')
            match_count: Количество найденных совпадений
            match_percentage: Процент совпадений (0.0-100.0)
            processing_time_seconds: Время обработки в секундах
            total_files_processed: Количество обработанных файлов
            total_size_bytes: Размер обработанных файлов в байтах
            error_reason: Причина ошибки (None = успешно, текст = описание ошибки)
        
        Returns:
            ID сохраненной записи или None при ошибке
        """
        try:
            # Валидация входных параметров
            if not self.match_validator.validate_match_params(tender_id, registry_type, match_count, match_percentage):
                logger.warning(f"Невалидные параметры для сохранения результата: tender_id={tender_id}")
                return None
            
            if not self.match_validator.validate_folder_name(folder_name):
                logger.warning(f"Невалидное имя папки: {folder_name}")
                folder_name = None
            
            # Используем InterestCalculator для бизнес-логики
            is_interesting_value = InterestCalculator.calculate_interest(
                match_count, match_percentage, error_reason
            )
            
            # Делегируем сохранение специализированному сервису
            return self.match_result_service.save_match_result(
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
            
        except Exception as e:
            logger.error(f"Ошибка сохранения результата совпадения: {e}")
            return None
    
    def save_match_details(self, match_id: int, details: Sequence[Dict[str, Any]]) -> None:
        """Сохранение деталей совпадений"""
        self.match_result_service.save_match_details(match_id, details)
    
    def save_file_errors(self, match_id: int, file_errors: Sequence[Dict[str, Any]]) -> None:
        """Сохранение ошибок файлов"""
        self.match_result_service.save_file_errors(match_id, file_errors)
    
    def get_match_result(self, tender_id: int, registry_type: str) -> Optional[Dict[str, Any]]:
        """Получение результата совпадения"""
        return self.match_query_service.get_match_result(tender_id, registry_type)
    
    def get_match_result_by_folder_name(self, folder_name: str) -> Optional[Dict[str, Any]]:
        """Получение результата по имени папки"""
        return self.match_query_service.get_match_result_by_folder_name(folder_name)
    
    def get_match_summary(self, tender_id: int, registry_type: str) -> Optional[Dict[str, Any]]:
        """Получение сводки совпадения"""
        return self.match_query_service.get_match_summary(tender_id, registry_type)
    
    def get_match_details(self, match_id: int) -> List[Dict[str, Any]]:
        """Получение деталей совпадения"""
        return self.match_query_service.get_match_details(match_id)
    
    def set_interesting_status(self, tender_id: int, registry_type: str, is_interesting: bool) -> bool:
        """Установка статуса интереса"""
        return self.match_status_service.set_interesting_status(tender_id, registry_type, is_interesting)
    
    def get_match_results_batch(self, tender_ids: List[int], registry_type: str) -> List[Dict[str, Any]]:
        """Получение результатов партией"""
        return self.match_query_service.get_match_results_batch(tender_ids, registry_type)
    
    def filter_uninteresting_tenders(self, tenders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Фильтрация неинтересных тендеров"""
        return self.match_status_service.filter_uninteresting_tenders(tenders)
    
    def acquire_tender_lock(self, tender_id: int, registry_type: str, tender_type: str = 'new') -> bool:
        """Блокировка тендера"""
        return self.tender_lock_service.acquire_tender_lock(tender_id, registry_type, tender_type)
    
    def release_tender_lock(self, tender_id: int, registry_type: str, tender_type: str = 'new') -> bool:
        """Разблокировка тендера"""
        return self.tender_lock_service.release_tender_lock(tender_id, registry_type, tender_type)