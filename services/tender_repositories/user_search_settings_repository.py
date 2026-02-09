"""
MODULE: services.tender_repositories.user_search_settings_repository
RESPONSIBILITY: Manage user search settings.
ALLOWED: typing, loguru, core.tender_database, psycopg2.extras.
FORBIDDEN: Business logic outside DB operations.
ERRORS: Database exceptions.

Репозиторий для работы с настройками поиска пользователя.
"""

from typing import Optional, Dict, Any
from loguru import logger
from core.tender_database import TenderDatabaseManager
from psycopg2.extras import RealDictCursor


class UserSearchSettingsRepository:
    """Репозиторий для работы с настройками поиска пользователя"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
        self._table_ensured = False
    
    def _ensure_table_exists(self):
        """Создание таблицы для настроек пользователя, если она не существует (ленивая инициализация)"""
        if self._table_ensured:
            return
        
        try:
            # Проверяем подключение к БД
            if not self.db_manager.is_connected():
                self.db_manager.connect()
            
            query = """
                CREATE TABLE IF NOT EXISTS user_search_settings (
                    user_id INTEGER PRIMARY KEY,
                    region_id INTEGER,
                    category_id INTEGER,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            self.db_manager.execute_query(query)
            
            # Создаем индекс отдельно
            try:
                index_query = """
                    CREATE INDEX IF NOT EXISTS idx_user_search_settings_user_id 
                        ON user_search_settings(user_id)
                """
                self.db_manager.execute_query(index_query)
            except Exception as idx_e:
                # Если индекс уже существует, игнорируем ошибку
                logger.debug(f"Индекс уже существует или ошибка при создании: {idx_e}")
            
            self._table_ensured = True
            logger.debug("Таблица user_search_settings создана или уже существует")
        except Exception as e:
            # Проверяем, что ошибка не связана с тем, что таблица уже существует
            error_str = str(e).lower()
            if 'already exists' in error_str or 'duplicate' in error_str:
                self._table_ensured = True
                logger.debug("Таблица user_search_settings уже существует")
            else:
                logger.warning(f"Не удалось создать таблицу user_search_settings: {e}")
    
    def save_user_settings(self, user_id: int, region_id: Optional[int], category_id: Optional[int]) -> bool:
        """
        Сохранение настроек поиска пользователя
        
        Args:
            user_id: ID пользователя
            region_id: ID региона (может быть None)
            category_id: ID категории (может быть None)
            
        Returns:
            True если успешно сохранено
        """
        # Убеждаемся, что таблица существует
        self._ensure_table_exists()
        
        try:
            # Используем INSERT ... ON CONFLICT для обновления существующей записи
            query = """
                INSERT INTO user_search_settings (user_id, region_id, category_id, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) 
                DO UPDATE SET 
                    region_id = EXCLUDED.region_id,
                    category_id = EXCLUDED.category_id,
                    updated_at = CURRENT_TIMESTAMP
            """
            self.db_manager.execute_update(query, (user_id, region_id, category_id))
            logger.info(f"Настройки поиска сохранены для пользователя {user_id}: region_id={region_id}, category_id={category_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении настроек поиска для пользователя {user_id}: {e}", exc_info=True)
            return False
    
    def get_user_settings(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение настроек поиска пользователя
        
        Args:
            user_id: ID пользователя
            
        Returns:
            Словарь с настройками или None если настройки не найдены
        """
        # Убеждаемся, что таблица существует
        self._ensure_table_exists()
        
        try:
            query = """
                SELECT user_id, region_id, category_id, updated_at
                FROM user_search_settings
                WHERE user_id = %s
            """
            results = self.db_manager.execute_query(query, (user_id,), RealDictCursor)
            
            if results:
                settings = dict(results[0])
                logger.debug(f"Настройки поиска загружены для пользователя {user_id}: {settings}")
                return settings
            else:
                logger.debug(f"Настройки поиска не найдены для пользователя {user_id}")
                return None
        except Exception as e:
            logger.error(f"Ошибка при загрузке настроек поиска для пользователя {user_id}: {e}", exc_info=True)
            return None

