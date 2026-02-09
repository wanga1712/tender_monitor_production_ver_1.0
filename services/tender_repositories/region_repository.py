"""
MODULE: services.tender_repositories.region_repository
RESPONSIBILITY: Access region data.
ALLOWED: typing, loguru, core.tender_database, psycopg2.extras.
FORBIDDEN: Business logic outside DB operations.
ERRORS: Database exceptions.

Репозиторий для работы с регионами.
"""

from typing import List, Dict, Any
from loguru import logger
from core.tender_database import TenderDatabaseManager
from psycopg2.extras import RealDictCursor


class RegionRepository:
    """Репозиторий для работы с регионами"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
    
    def get_all_regions(self) -> List[Dict[str, Any]]:
        """Получение всех регионов"""
        try:
            query = """
                SELECT 
                    id,
                    code,
                    name
                FROM region
                ORDER BY name
            """
            results = self.db_manager.execute_query(query, None, RealDictCursor)
            return [dict(row) for row in results] if results else []
            
        except Exception as e:
            logger.error(f"Ошибка при получении регионов: {e}")
            return []

