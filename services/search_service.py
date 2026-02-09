"""
MODULE: services.search_service
RESPONSIBILITY: Search for commercial products in the database.
ALLOWED: DatabaseManager, SQL queries.
FORBIDDEN: Tender operations, external APIs.
ERRORS: DatabaseQueryError.
"""

from typing import List, Optional, Dict, Any
from core.database import DatabaseManager
from core.models import Product, Manufacturer, Category, Subcategory


class SearchService:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def search_products(
            self,
            category_name: Optional[str] = None,
            subcategory_name: Optional[str] = None,
            manufacturer_name: Optional[str] = None,
            product_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Поиск продуктов по фильтрам"""
        query = """
        SELECT 
            p.name, 
            m.name as manufacturer, 
            c.name as category, 
            pp.price,
            p.id as product_id
        FROM products p
        LEFT JOIN manufacturers m ON p.manufacturer_id = m.id
        LEFT JOIN subcategories s ON p.subcategory_id = s.id
        LEFT JOIN categories c ON s.category_id = c.id
        LEFT JOIN product_pricing pp ON p.id = pp.product_id
        WHERE 1=1
        """
        params = []

        if category_name:
            query += " AND c.name = %s"
            params.append(category_name)

        if subcategory_name:
            query += " AND s.name = %s"
            params.append(subcategory_name)

        if manufacturer_name:
            query += " AND m.name = %s"
            params.append(manufacturer_name)

        if product_name:
            query += " AND p.name ILIKE %s"
            params.append(f"%{product_name}%")

        query += " ORDER BY p.name"

        return self.db_manager.execute_query(query, tuple(params))

    def get_categories(self) -> List[Dict[str, Any]]:
        """Получение списка категорий"""
        return self.db_manager.execute_query(
            "SELECT id, name FROM categories ORDER BY name"
        )

    def get_manufacturers(self) -> List[Dict[str, Any]]:
        """Получение списка производителей"""
        return self.db_manager.execute_query(
            "SELECT id, name FROM manufacturers ORDER BY name"
        )