"""
Сервис для работы с ценами товаров.

Отвечает за:
- Получение информации о ценах товаров
- Обновление цен товаров
- Обновление веса товаров
- Обновление единиц измерения
"""

from typing import List, Dict, Any, Optional
from core.database import DatabaseManager
from loguru import logger


class PricingService:
    """Сервис для управления ценами товаров."""

    def __init__(self, db_manager: DatabaseManager):
        """
        Инициализация сервиса цен.
        
        Args:
            db_manager: Менеджер базы данных
        """
        self.db = db_manager

    def get_product_pricing(self, product_id: int) -> List[Dict[str, Any]]:
        """
        Получение информации о ценах товара.
        
        Args:
            product_id: ID товара
        
        Returns:
            Список ценовых предложений
        """
        try:
            query = """
                SELECT 
                    id, product_id, container_type, size, price, weight,
                    created_at, updated_at
                FROM product_pricing 
                WHERE product_id = %s
                ORDER BY container_type, size
            """
            
            return self.db.execute_query(query, (product_id,))
            
        except Exception as e:
            logger.error(f"Ошибка при получении цен товара {product_id}: {e}")
            return []

    def update_product_weight(self, pricing_id: int, weight: float) -> bool:
        """
        Обновление веса товара.
        
        Args:
            pricing_id: ID ценового предложения
            weight: Новый вес
        
        Returns:
            True если успешно, False если ошибка
        """
        try:
            query = "UPDATE product_pricing SET weight = %s WHERE id = %s"
            self.db.execute_query(query, (weight, pricing_id))
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении веса {pricing_id}: {e}")
            return False

    def update_product_price(self, pricing_id: int, price: float) -> bool:
        """
        Обновление цены товара.
        
        Args:
            pricing_id: ID ценового предложения
            price: Новая цена
        
        Returns:
            True если успешно, False если ошибка
        """
        try:
            query = "UPDATE product_pricing SET price = %s WHERE id = %s"
            self.db.execute_query(query, (price, pricing_id))
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении цены {pricing_id}: {e}")
            return False

    def update_product_unit(self, pricing_id: int, container_type: str, size: str) -> bool:
        """
        Обновление единицы измерения товара.
        
        Args:
            pricing_id: ID ценового предложения
            container_type: Тип упаковки
            size: Размер
        
        Returns:
            True если успешно, False если ошибка
        """
        try:
            query = "UPDATE product_pricing SET container_type = %s, size = %s WHERE id = %s"
            self.db.execute_query(query, (container_type, size, pricing_id))
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении единицы измерения {pricing_id}: {e}")
            return False