"""
MODULE: services.document_search.product_loader
RESPONSIBILITY: Load product names from the database for matching.
ALLOWED: core.database, logging.
FORBIDDEN: Business logic processing (only loading).
ERRORS: None.

Модуль для загрузки списка товаров из БД.
"""

from typing import List, Optional
from loguru import logger

from core.database import DatabaseManager


class ProductLoader:
    """Класс для загрузки списка товаров"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Инициализация загрузчика товаров
        
        Args:
            db_manager: Менеджер БД с таблицей products
        """
        self.db_manager = db_manager
        self._product_names: Optional[List[str]] = None
    
    def ensure_products_loaded(self) -> None:
        """Ленивая загрузка названий товаров (по требованию пользователя)."""
        if self._product_names is not None:
            return

        logger.info("Загрузка списка товаров для поиска по документации...")
        query = "SELECT name FROM products WHERE name IS NOT NULL"
        results = self.db_manager.execute_query(query)
        self._product_names = [row.get("name", "").strip() for row in results if row.get("name")]
        logger.info(f"Получено наименований товаров: {len(self._product_names)}")
    
    @property
    def product_names(self) -> Optional[List[str]]:
        """Получение списка названий товаров"""
        return self._product_names

