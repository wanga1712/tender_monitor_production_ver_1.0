"""
MODULE: services.tender_repositories.okpd_category_repository
RESPONSIBILITY: Manage OKPD categories in DB.
ALLOWED: typing, loguru, core.tender_database, psycopg2.extras.
FORBIDDEN: Business logic outside DB operations.
ERRORS: Database exceptions.

Репозиторий для работы с категориями ОКПД.
"""

from typing import List, Dict, Any, Optional
from loguru import logger
from core.tender_database import TenderDatabaseManager
from psycopg2.extras import RealDictCursor


class OkpdCategoryRepository:
    """Репозиторий для работы с категориями ОКПД"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
    
    def get_okpd_categories(self, user_id: int) -> List[Dict[str, Any]]:
        """Получение всех категорий ОКПД пользователя"""
        try:
            query = """
                SELECT 
                    id,
                    user_id,
                    name,
                    description,
                    created_at,
                    updated_at
                FROM okpd_categories
                WHERE user_id = %s
                ORDER BY name
            """
            results = self.db_manager.execute_query(
                query,
                (user_id,),
                RealDictCursor
            )
            return [dict(row) for row in results] if results else []
        except Exception as e:
            logger.error(f"Ошибка при получении категорий ОКПД: {e}")
            return []
    
    def create_okpd_category(
        self,
        user_id: int,
        name: str,
        description: Optional[str] = None
    ) -> Optional[int]:
        """Создание новой категории ОКПД"""
        try:
            check_query = """
                SELECT id FROM okpd_categories
                WHERE user_id = %s AND name = %s
            """
            existing = self.db_manager.execute_query(
                check_query,
                (user_id, name)
            )
            if existing:
                logger.warning(f"Категория '{name}' уже существует для пользователя {user_id}")
                return existing[0].get('id')
            
            insert_query = """
                INSERT INTO okpd_categories (user_id, name, description)
                VALUES (%s, %s, %s)
                RETURNING id
            """
            result = self.db_manager.execute_query(
                insert_query,
                (user_id, name, description),
                RealDictCursor
            )
            if result:
                category_id = result[0].get('id')
                logger.info(f"Создана категория ОКПД '{name}' (id={category_id}) для пользователя {user_id}")
                return category_id
            return None
        except Exception as e:
            logger.error(f"Ошибка при создании категории ОКПД: {e}")
            return None
    
    def update_okpd_category(
        self,
        category_id: int,
        user_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None
    ) -> bool:
        """Обновление категории ОКПД"""
        try:
            updates = []
            params = []
            
            if name is not None:
                updates.append("name = %s")
                params.append(name)
            
            if description is not None:
                updates.append("description = %s")
                params.append(description)
            
            if not updates:
                return False
            
            updates.append("updated_at = CURRENT_TIMESTAMP")
            params.extend([category_id, user_id])
            
            query = f"""
                UPDATE okpd_categories
                SET {', '.join(updates)}
                WHERE id = %s AND user_id = %s
            """
            self.db_manager.execute_update(query, tuple(params))
            logger.info(f"Обновлена категория ОКПД (id={category_id})")
            return True
        except Exception as e:
            logger.error(f"Ошибка при обновлении категории ОКПД: {e}")
            return False
    
    def delete_okpd_category(self, category_id: int, user_id: int) -> bool:
        """Удаление категории ОКПД"""
        try:
            query = """
                DELETE FROM okpd_categories
                WHERE id = %s AND user_id = %s
            """
            self.db_manager.execute_update(query, (category_id, user_id))
            logger.info(f"Удалена категория ОКПД (id={category_id})")
            return True
        except Exception as e:
            logger.error(f"Ошибка при удалении категории ОКПД: {e}")
            return False
    
    def assign_okpd_to_category(
        self,
        user_id: int,
        okpd_id: int,
        category_id: Optional[int] = None
    ) -> bool:
        """Привязка ОКПД кода к категории"""
        try:
            check_query = """
                SELECT id FROM okpd_from_users
                WHERE id = %s AND user_id = %s
            """
            existing = self.db_manager.execute_query(
                check_query,
                (okpd_id, user_id)
            )
            if not existing:
                logger.warning(f"ОКПД код (id={okpd_id}) не найден или не принадлежит пользователю {user_id}")
                return False
            
            if category_id is not None:
                cat_check_query = """
                    SELECT id FROM okpd_categories
                    WHERE id = %s AND user_id = %s
                """
                cat_existing = self.db_manager.execute_query(
                    cat_check_query,
                    (category_id, user_id)
                )
                if not cat_existing:
                    logger.warning(f"Категория (id={category_id}) не найдена или не принадлежит пользователю {user_id}")
                    return False
            
            update_query = """
                UPDATE okpd_from_users
                SET category_id = %s
                WHERE id = %s AND user_id = %s
            """
            self.db_manager.execute_update(
                update_query,
                (category_id, okpd_id, user_id)
            )
            logger.info(f"ОКПД код (id={okpd_id}) {'привязан к категории' if category_id else 'отвязан от категории'} (category_id={category_id})")
            return True
        except Exception as e:
            logger.error(f"Ошибка при привязке ОКПД к категории: {e}")
            return False
    
    def get_okpd_codes_by_category(self, user_id: int, category_id: int) -> List[str]:
        """Получение списка кодов ОКПД из категории"""
        try:
            query = """
                SELECT okpd_code
                FROM okpd_from_users
                WHERE user_id = %s AND category_id = %s
            """
            results = self.db_manager.execute_query(
                query,
                (user_id, category_id),
                RealDictCursor
            )
            return [row.get('okpd_code', '') for row in results if row.get('okpd_code')]
        except Exception as e:
            logger.error(f"Ошибка при получении ОКПД кодов из категории: {e}")
            return []

