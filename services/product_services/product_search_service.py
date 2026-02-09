"""
Сервис для поиска и получения информации о товарах.

Отвечает за:
- Поиск товаров по различным критериям
- Получение товара по ID
- Получение информации о ценах товаров
- Получение информации об упаковке товаров
"""

from typing import List, Dict, Any, Optional
from core.database import DatabaseManager
from loguru import logger


class ProductSearchService:
    """Сервис для поиска и получения информации о товарах."""

    def __init__(self, db_manager: DatabaseManager):
        """
        Инициализация сервиса поиска товаров.
        
        Args:
            db_manager: Менеджер базы данных
        """
        self.db = db_manager

    def search_products(self, search_text: Optional[str] = None,
                       category_id: Optional[int] = None,
                       subcategory_id: Optional[int] = None,
                       manufacturer_id: Optional[int] = None,
                       limit: int = 100) -> List[Dict[str, Any]]:
        """
        Поиск товаров по различным критериям.
        
        Args:
            search_text: Текст для поиска
            category_id: ID категории
            subcategory_id: ID подкатегории
            manufacturer_id: ID производителя
            limit: Лимит результатов
        
        Returns:
            Список товаров с основной информацией
        """
        try:
            query = """
                SELECT 
                    p.id, p.name, p.description, p.subcategory_id,
                    p.manufacturer_id, 
                    s.name as subcategory_name, m.name as manufacturer_name,
                    c.name as category_name
                FROM products p
                LEFT JOIN subcategories s ON p.subcategory_id = s.id
                LEFT JOIN categories c ON s.category_id = c.id
                LEFT JOIN manufacturers m ON p.manufacturer_id = m.id
                WHERE 1=1
            """
            
            params = []
            
            if search_text:
                query += " AND (p.name ILIKE %s OR p.description ILIKE %s)"
                search_pattern = f"%{search_text}%"
                params.extend([search_pattern, search_pattern])
            
            if category_id:
                query += " AND s.category_id = %s"
                params.append(category_id)
            
            if subcategory_id:
                query += " AND p.subcategory_id = %s"
                params.append(subcategory_id)
            
            if manufacturer_id:
                query += " AND p.manufacturer_id = %s"
                params.append(manufacturer_id)
            
            query += " ORDER BY p.name LIMIT %s"
            params.append(limit)
            
            return self.db.execute_query(query, tuple(params))
            
        except Exception as e:
            logger.error(f"Ошибка при поиске товаров: {e}")
            return []

    def get_product_by_id(self, product_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение товара по идентификатору.
        
        Args:
            product_id: ID товара
        
        Returns:
            Информация о товаре или None если не найден
        """
        try:
            query = """
                SELECT 
                    p.id, p.name, p.description, p.category_id, p.subcategory_id,
                    p.manufacturer_id, c.name as category_name,
                    s.name as subcategory_name, m.name as manufacturer_name
                FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                LEFT JOIN subcategories s ON p.subcategory_id = s.id
                LEFT JOIN manufacturers m ON p.manufacturer_id = m.id
                WHERE p.id = %s
            """
            
            result = self.db.execute_query(query, (product_id,))
            return result[0] if result else None
            
        except Exception as e:
            logger.error(f"Ошибка при получении товара по ID {product_id}: {e}")
            return None