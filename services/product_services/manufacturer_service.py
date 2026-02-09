"""
Сервис для работы с производителями товаров.

Отвечает за:
- Получение списка производителей
- Кэширование производителей
- Очистку кэша производителей
"""

from typing import List, Dict, Any, Optional
from core.database import DatabaseManager
from loguru import logger


class ManufacturerService:
    """Сервис для управления производителями товаров."""

    def __init__(self, db_manager: DatabaseManager):
        """
        Инициализация сервиса производителей.
        
        Args:
            db_manager: Менеджер базы данных
        """
        self.db = db_manager
        # Кэш для производителей
        self._manufacturers_cache: Optional[List[Dict[str, Any]]] = None

    def get_manufacturers(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        Получение списка всех производителей с кэшированием.
        
        Args:
            use_cache: Использовать кэш (по умолчанию True)
        
        Returns:
            Список словарей с полями id и name
        """
        if use_cache and self._manufacturers_cache is not None:
            return self._manufacturers_cache
        
        try:
            query = "SELECT id, name FROM manufacturers ORDER BY name"
            result = self.db.execute_query(query)
            if use_cache:
                self._manufacturers_cache = result
            return result
        except Exception as e:
            logger.error(f"Ошибка при получении производителей: {e}")
            return []

    def clear_manufacturers_cache(self) -> None:
        """Очистка кэша производителей."""
        self._manufacturers_cache = None