"""
MODULE: services.match_finder

RESPONSIBILITY:
- Поиск и извлечение результатов совпадений из базы данных
- Фильтрация и пакетная обработка записей

ALLOWED:
- Запросы к базе данных через TenderDatabaseManager
- Логирование через loguru
- Стандартные типы Python

FORBIDDEN:
- Бизнес-логика определения интереса
- Валидация данных
- Управление блокировками

ERRORS:
- Должен пробрасывать DatabaseQueryError, DatabaseConnectionError
"""

from typing import Optional, Dict, Any, List
from loguru import logger
from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseQueryError, DatabaseConnectionError
from psycopg2.extras import RealDictCursor


class MatchFinder:
    """Сервис для поиска и извлечения результатов совпадений"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        """
        Инициализация сервиса поиска
        
        Args:
            db_manager: Менеджер базы данных tender_monitor
        """
        self.db_manager = db_manager
    
    def get_match_result(self, tender_id: int, registry_type: str) -> Optional[Dict[str, Any]]:
        """
        Получение результата поиска по ID закупки и типу реестра
        
        Args:
            tender_id: ID закупки
            registry_type: Тип реестра ('44fz' или '223fz')
        
        Returns:
            Словарь с данными результата или None
        """
        try:
            query = """
                SELECT * FROM tender_document_matches 
                WHERE tender_id = %s AND registry_type = %s
                ORDER BY processed_at DESC 
                LIMIT 1
            """
            result = self.db_manager.execute_query(query, (tender_id, registry_type), RealDictCursor)
            return result[0] if result and len(result) > 0 else None
        except (DatabaseQueryError, DatabaseConnectionError):
            raise
        except Exception as e:
            logger.error(f"Ошибка при получении результата поиска для tender_id={tender_id}: {e}")
            return None
    
    def get_match_result_by_folder_name(self, folder_name: str) -> Optional[Dict[str, Any]]:
        """
        Получение результата поиска по имени папки
        
        Args:
            folder_name: Имя папки с документами
        
        Returns:
            Словарь с данными результата или None
        """
        try:
            query = """
                SELECT * FROM tender_document_matches 
                WHERE folder_name = %s
                ORDER BY processed_at DESC 
                LIMIT 1
            """
            result = self.db_manager.execute_query(query, (folder_name,), RealDictCursor)
            return result[0] if result and len(result) > 0 else None
        except (DatabaseQueryError, DatabaseConnectionError):
            raise
        except Exception as e:
            logger.error(f"Ошибка при получении результата поиска по folder_name={folder_name}: {e}")
            return None
    
    def get_match_summary(self, tender_id: int, registry_type: str) -> Optional[Dict[str, Any]]:
        """
        Получение сводки по результатам поиска
        
        Args:
            tender_id: ID закупки
            registry_type: Тип реестра
        
        Returns:
            Словарь со сводкой или None
        """
        try:
            query = """
                SELECT 
                    tender_id,
                    registry_type,
                    COUNT(*) as total_matches,
                    MAX(match_percentage) as max_percentage,
                    MAX(processed_at) as last_processed,
                    BOOL_OR(is_interesting) as ever_interesting
                FROM tender_document_matches 
                WHERE tender_id = %s AND registry_type = %s
                GROUP BY tender_id, registry_type
            """
            result = self.db_manager.execute_query(query, (tender_id, registry_type), RealDictCursor)
            return result[0] if result and len(result) > 0 else None
        except (DatabaseQueryError, DatabaseConnectionError):
            raise
        except Exception as e:
            logger.error(f"Ошибка при получении сводки для tender_id={tender_id}: {e}")
            return None
    
    def get_match_details(self, match_id: int) -> List[Dict[str, Any]]:
        """
        Получение деталей совпадений по ID результата
        
        Args:
            match_id: ID результата поиска
        
        Returns:
            Список словарей с деталями совпадений
        """
        try:
            query = """
                SELECT * FROM tender_document_match_details 
                WHERE match_id = %s
                ORDER BY score DESC
            """
            result = self.db_manager.execute_query(query, (match_id,), RealDictCursor)
            return result if result else []
        except (DatabaseQueryError, DatabaseConnectionError):
            raise
        except Exception as e:
            logger.error(f"Ошибка при получении деталей для match_id={match_id}: {e}")
            return []
    
    def get_match_results_batch(
        self, 
        tender_ids: List[int], 
        registry_type: str
    ) -> List[Dict[str, Any]]:
        """
        Пакетное получение результатов поиска
        
        Args:
            tender_ids: Список ID закупок
            registry_type: Тип реестра
        
        Returns:
            Список словарей с результатами
        """
        if not tender_ids:
            return []
            
        try:
            query = """
                SELECT DISTINCT ON (tender_id) *
                FROM tender_document_matches 
                WHERE tender_id = ANY(%s) AND registry_type = %s
                ORDER BY tender_id, processed_at DESC
            """
            result = self.db_manager.execute_query(query, (tender_ids, registry_type), RealDictCursor)
            return result if result else []
        except (DatabaseQueryError, DatabaseConnectionError):
            raise
        except Exception as e:
            logger.error(f"Ошибка при пакетном получении результатов: {e}")
            return []
    
    def filter_uninteresting_tenders(
        self, 
        tender_ids: List[int], 
        registry_type: str
    ) -> List[int]:
        """
        Фильтрация неинтересных закупок
        
        Args:
            tender_ids: Список ID закупок для проверки
            registry_type: Тип реестра
        
        Returns:
            Список ID неинтересных закупок
        """
        if not tender_ids:
            return []
            
        try:
            query = """
                SELECT tender_id
                FROM tender_document_matches 
                WHERE tender_id = ANY(%s) 
                AND registry_type = %s 
                AND is_interesting = FALSE
                GROUP BY tender_id
            """
            result = self.db_manager.execute_query(query, (tender_ids, registry_type), RealDictCursor)
            return [row['tender_id'] for row in result] if result else []
        except (DatabaseQueryError, DatabaseConnectionError):
            raise
        except Exception as e:
            logger.error(f"Ошибка при фильтрации неинтересных закупок: {e}")
            return []
