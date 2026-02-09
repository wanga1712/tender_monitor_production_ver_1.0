"""
MODULE: services.tender_repositories.user_okpd_repository
RESPONSIBILITY: Manage user-specific OKPD codes.
ALLOWED: typing, loguru, core.tender_database, psycopg2.extras.
FORBIDDEN: Business logic outside DB operations.
ERRORS: Database exceptions.

Репозиторий для работы с ОКПД кодами пользователя.
"""

from typing import List, Dict, Any, Optional
from loguru import logger
from core.tender_database import TenderDatabaseManager
from psycopg2.extras import RealDictCursor


class UserOkpdRepository:
    """Репозиторий для работы с ОКПД кодами пользователя"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
    
    def get_user_okpd_codes(self, user_id: int, category_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Получение сохраненных кодов ОКПД пользователя"""
        try:
            query = """
                SELECT 
                    o.id,
                    o.user_id,
                    o.okpd_code,
                    o.name,
                    o.setting_id,
                    o.category_id,
                    c.main_code,
                    c.sub_code,
                    c.name as okpd_name,
                    cat.name as category_name
                FROM okpd_from_users o
                LEFT JOIN collection_codes_okpd c 
                    ON o.okpd_code = COALESCE(c.sub_code, c.main_code)
                LEFT JOIN okpd_categories cat ON o.category_id = cat.id
                WHERE o.user_id = %s
            """
            params = [user_id]
            
            if category_id is not None:
                query += " AND o.category_id = %s"
                params.append(category_id)
            
            query += " ORDER BY cat.name NULLS LAST, o.okpd_code"
            
            results = self.db_manager.execute_query(
                query, 
                tuple(params), 
                RealDictCursor
            )
            return [dict(row) for row in results] if results else []
            
        except Exception as e:
            logger.error(f"Ошибка при получении ОКПД пользователя: {e}")
            return []
    
    def add_user_okpd_code(
        self, 
        user_id: int, 
        okpd_code: str, 
        name: Optional[str] = None,
        setting_id: Optional[int] = None
    ) -> Optional[int]:
        """Добавление кода ОКПД для пользователя"""
        try:
            check_query = """
                SELECT id FROM okpd_from_users 
                WHERE user_id = %s AND okpd_code = %s
            """
            existing = self.db_manager.execute_query(
                check_query, 
                (user_id, okpd_code),
                RealDictCursor
            )
            
            if existing:
                logger.warning(f"ОКПД код {okpd_code} уже добавлен для пользователя {user_id}")
                return existing[0].get('id')
            
            if not name:
                name_query = """
                    SELECT name FROM collection_codes_okpd
                    WHERE main_code = %s OR sub_code = %s
                    LIMIT 1
                """
                name_result = self.db_manager.execute_query(
                    name_query,
                    (okpd_code, okpd_code),
                    RealDictCursor
                )
                if name_result:
                    name = name_result[0].get('name')
            
            insert_query = """
                INSERT INTO okpd_from_users (user_id, okpd_code, name, setting_id)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """
            result = self.db_manager.execute_query(
                insert_query,
                (user_id, okpd_code, name, setting_id),
                RealDictCursor
            )
            if result:
                okpd_id = result[0].get('id')
                logger.info(f"Добавлен ОКПД код {okpd_code} для пользователя {user_id} (id={okpd_id})")
                return okpd_id
            return None
            
        except Exception as e:
            logger.error(f"Ошибка при добавлении ОКПД кода: {e}")
            return None
    
    def remove_user_okpd_code(self, user_id: int, okpd_id: int) -> bool:
        """Удаление кода ОКПД пользователя"""
        try:
            query = """
                DELETE FROM okpd_from_users 
                WHERE id = %s AND user_id = %s
            """
            self.db_manager.execute_update(query, (okpd_id, user_id))
            logger.info(f"Удален ОКПД код (id={okpd_id}) для пользователя {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при удалении ОКПД кода: {e}")
            return False

