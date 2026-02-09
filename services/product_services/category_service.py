"""
Сервис для работы с категориями и подкатегориями товаров.

Отвечает за:
- Получение списка категорий
- Получение подкатегорий по категории
- Кэширование категорий и подкатегорий
- Очистку кэша
"""

from typing import List, Dict, Any, Optional
from core.database import DatabaseManager
from loguru import logger


class CategoryService:
    """Сервис для управления категориями и подкатегориями товаров."""

    def __init__(self, db_manager: DatabaseManager):
        """
        Инициализация сервиса категорий.
        
        Args:
            db_manager: Менеджер базы данных
        """
        self.db = db_manager
        # Кэш для категорий и подкатегорий
        self._categories_cache: Optional[List[Dict[str, Any]]] = None
        self._subcategories_cache: Dict[int, List[Dict[str, Any]]] = {}

    def get_categories(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        Получение списка всех категорий с кэшированием.
        
        Args:
            use_cache: Использовать кэш (по умолчанию True)
        
        Returns:
            Список словарей с полями id и name
        """
        if use_cache and self._categories_cache is not None:
            return self._categories_cache
        
        try:
            query = "SELECT id, name FROM categories ORDER BY name"
            result = self.db.execute_query(query)
            if use_cache:
                self._categories_cache = result
            return result
        except Exception as e:
            logger.error(f"Ошибка при получении категорий: {e}")
            return []

    def get_subcategories(self, category_id: Optional[int] = None, 
                        use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        Получение списка подкатегорий с кэшированием.
        
        Args:
            category_id: Идентификатор категории (опционально)
            use_cache: Использовать кэш (по умолчанию True)
        
        Returns:
            Список словарей с полями id, name, category_id
        """
        if category_id and use_cache and category_id in self._subcategories_cache:
            return self._subcategories_cache[category_id]
        
        try:
            if category_id:
                query = """
                    SELECT id, name, category_id 
                    FROM subcategories 
                    WHERE category_id = %s 
                    ORDER BY name
                """
                result = self.db.execute_query(query, (category_id,))
                if use_cache:
                    self._subcategories_cache[category_id] = result
                return result
            else:
                query = """
                    SELECT id, name, category_id 
                    FROM subcategories 
                    ORDER BY category_id, name
                """
                return self.db.execute_query(query)
        except Exception as e:
            logger.error(f"Ошибка при получении подкатегорий: {e}")
            return []

    def clear_categories_cache(self) -> None:
        """Очистка кэша категорий."""
        self._categories_cache = None

    def clear_subcategories_cache(self, category_id: Optional[int] = None) -> None:
        """
        Очистка кэша подкатегорий.
        
        Args:
            category_id: Идентификатор категории (опционально)
        """
        if category_id:
            self._subcategories_cache.pop(category_id, None)
        else:
            self._subcategories_cache.clear()

    def clear_all_cache(self) -> None:
        """Очистка всего кэша категорий и подкатегорий."""
        self.clear_categories_cache()
        self.clear_subcategories_cache()