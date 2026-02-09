"""
Сервис для работы с упаковкой товаров.

Отвечает за:
- Получение информации об упаковке товаров
- Поиск упаковки по названию комплекта
"""

from typing import List, Dict, Any, Optional
from core.database import DatabaseManager
from loguru import logger


class PackagingService:
    """Сервис для управления упаковкой товаров."""

    def __init__(self, db_manager: DatabaseManager):
        """
        Инициализация сервиса упаковки.
        
        Args:
            db_manager: Менеджер базы данных
        """
        self.db = db_manager

    def get_product_packaging(self, product_id: int, 
                             kit_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получение информации об упаковке товара.
        
        Args:
            product_id: ID товара
            kit_name: Название комплекта (опционально)
        
        Returns:
            Список информации об упаковке
        """
        try:
            if kit_name:
                query = """
                    SELECT 
                        id, product_id, kit_name, container_type, size,
                        quantity, weight, created_at, updated_at
                    FROM product_packaging 
                    WHERE product_id = %s AND kit_name ILIKE %s
                    ORDER BY kit_name, container_type
                """
                return self.db.execute_query(query, (product_id, f"%{kit_name}%"))
            else:
                query = """
                    SELECT 
                        id, product_id, kit_name, container_type, size,
                        quantity, weight, created_at, updated_at
                    FROM product_packaging 
                    WHERE product_id = %s
                    ORDER BY kit_name, container_type
                """
                return self.db.execute_query(query, (product_id,))
            
        except Exception as e:
            logger.error(f"Ошибка при получении упаковки товара {product_id}: {e}")
            return []