"""
MODULE: services.match_services.match_query_service
RESPONSIBILITY: Сервис запросов данных результатов поиска совпадений
ALLOWED: TenderDatabaseManager, SQL запросы, фильтрация данных
FORBIDDEN: Бизнес-логика интереса, модификация данных
ERRORS: Должен пробрасывать DatabaseQueryError, DatabaseConnectionError

Сервис для запросов данных результатов поиска совпадений.
Отвечает за чтение и поиск данных совпадений.
"""

from typing import Optional, Dict, Any, List
from loguru import logger

from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseQueryError, DatabaseConnectionError
from psycopg2.extras import RealDictCursor


class MatchQueryService:
    """Сервис запросов данных результатов поиска совпадений"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        """Инициализация сервиса"""
        self.db_manager = db_manager
    
    def get_match_result(self, tender_id: int, registry_type: str) -> Optional[Dict[str, Any]]:
        """Получение результата совпадения по ID тендера и типу реестра"""
        query = """
            SELECT * FROM tender_document_matches 
            WHERE tender_id = %s AND registry_type = %s
        """
        
        try:
            result = self.db_manager.execute_query(query, (tender_id, registry_type))
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Ошибка получения результата совпадения: {e}")
            return None
    
    def get_match_result_by_folder_name(self, folder_name: str) -> Optional[Dict[str, Any]]:
        """Получение результата совпадения по имени папки"""
        query = """
            SELECT * FROM tender_document_matches 
            WHERE folder_name = %s
        """
        
        try:
            result = self.db_manager.execute_query(query, (folder_name,))
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Ошибка получения результата по имени папки: {e}")
            return None
    
    def get_match_summary(self, tender_id: int, registry_type: str) -> Optional[Dict[str, Any]]:
        """Получение сводки совпадения"""
        query = """
            SELECT 
                tender_id, registry_type, match_count, match_percentage,
                processing_time_seconds, total_files_processed, total_size_bytes,
                error_reason, folder_name, has_error, is_interesting,
                created_at, updated_at
            FROM tender_document_matches 
            WHERE tender_id = %s AND registry_type = %s
        """
        
        try:
            result = self.db_manager.execute_query(query, (tender_id, registry_type))
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Ошибка получения сводки совпадения: {e}")
            return None
    
    def get_match_details(self, match_id: int) -> List[Dict[str, Any]]:
        """Получение деталей совпадения"""
        query = """
            SELECT * FROM tender_document_match_details 
            WHERE match_id = %s
            ORDER BY score DESC
        """
        
        try:
            result = self.db_manager.execute_query(query, (match_id,))
            return result if result else []
        except Exception as e:
            logger.error(f"Ошибка получения деталей совпадения: {e}")
            return []
    
    def get_match_results_batch(self, tender_ids: List[int], registry_type: str) -> List[Dict[str, Any]]:
        """Получение результатов партией"""
        if not tender_ids:
            return []
        
        query = """
            SELECT * FROM tender_document_matches 
            WHERE tender_id = ANY(%s) AND registry_type = %s
        """
        
        try:
            result = self.db_manager.execute_query(query, (tender_ids, registry_type))
            return result if result else []
        except Exception as e:
            logger.error(f"Ошибка получения результатов партией: {e}")
            return []
    
    def get_uninteresting_tenders(self, registry_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получение неинтересных тендеров"""
        if registry_type:
            query = """
                SELECT tender_id, registry_type FROM tender_document_matches 
                WHERE is_interesting = FALSE AND registry_type = %s
            """
            params = (registry_type,)
        else:
            query = """
                SELECT tender_id, registry_type FROM tender_document_matches 
                WHERE is_interesting = FALSE
            """
            params = None
        
        try:
            result = self.db_manager.execute_query(query, params)
            return result if result else []
        except Exception as e:
            logger.error(f"Ошибка получения неинтересных тендеров: {e}")
            return []
    
    def get_interesting_tenders(self, registry_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получение интересных тендеров"""
        if registry_type:
            query = """
                SELECT tender_id, registry_type FROM tender_document_matches 
                WHERE is_interesting = TRUE AND registry_type = %s
            """
            params = (registry_type,)
        else:
            query = """
                SELECT tender_id, registry_type FROM tender_document_matches 
                WHERE is_interesting = TRUE
            """
            params = None
        
        try:
            result = self.db_manager.execute_query(query, params)
            return result if result else []
        except Exception as e:
            logger.error(f"Ошибка получения интересных тендеров: {e}")
            return []
