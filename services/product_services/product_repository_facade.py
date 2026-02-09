"""
Фасад для обратной совместимости с оригинальным ProductRepository.

Предоставляет тот же интерфейс, что и оригинальный ProductRepository,
но использует декомпозированную архитектуру через координатор сервисов.
"""

from typing import List, Dict, Any, Optional
from core.database import DatabaseManager
from .product_coordinator import ProductCoordinator


class ProductRepositoryFacade:
    """Фасад для обратной совместимости с ProductRepository."""

    def __init__(self, db_manager: DatabaseManager):
        """
        Инициализация фасада.
        
        Args:
            db_manager: Менеджер базы данных
        """
        self.db = db_manager
        self.coordinator = ProductCoordinator(db_manager)
        
        # Сохраняем кэш для совместимости с оригинальным интерфейсом
        self._categories_cache = None
        self._manufacturers_cache = None
        self._subcategories_cache = {}

    def get_categories(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Получение списка всех категорий с кэшированием."""
        return self.coordinator.get_categories(use_cache)

    def get_subcategories(self, category_id: Optional[int] = None, 
                         use_cache: bool = True) -> List[Dict[str, Any]]:
        """Получение списка подкатегорий с кэшированием."""
        return self.coordinator.get_subcategories(category_id, use_cache)

    def clear_categories_cache(self) -> None:
        """Очистка кэша категорий."""
        self.coordinator.clear_categories_cache()

    def clear_subcategories_cache(self, category_id: Optional[int] = None) -> None:
        """Очистка кэша подкатегорий."""
        self.coordinator.clear_subcategories_cache(category_id)

    def get_manufacturers(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Получение списка всех производителей с кэшированием."""
        return self.coordinator.get_manufacturers(use_cache)

    def clear_manufacturers_cache(self) -> None:
        """Очистка кэша производителей."""
        self.coordinator.clear_manufacturers_cache()

    def clear_all_cache(self) -> None:
        """Очистка всего кэша категорий и подкатегорий."""
        self.coordinator.clear_all_cache()

    def search_products(self, search_text: Optional[str] = None,
                       category_id: Optional[int] = None,
                       subcategory_id: Optional[int] = None,
                       manufacturer_id: Optional[int] = None,
                       limit: int = 100) -> List[Dict[str, Any]]:
        """Поиск товаров по различным критериям."""
        return self.coordinator.search_products(
            search_text, category_id, subcategory_id, manufacturer_id, limit
        )

    def get_product_by_id(self, product_id: int) -> Optional[Dict[str, Any]]:
        """Получение товара по идентификатору."""
        return self.coordinator.get_product_by_id(product_id)

    def get_product_pricing(self, product_id: int) -> List[Dict[str, Any]]:
        """Получение информации о ценах товара."""
        return self.coordinator.get_product_pricing(product_id)

    def update_product_weight(self, pricing_id: int, weight: float) -> bool:
        """Обновление веса товара."""
        return self.coordinator.update_product_weight(pricing_id, weight)

    def update_product_price(self, pricing_id: int, price: float) -> bool:
        """Обновление цены товара."""
        return self.coordinator.update_product_price(pricing_id, price)

    def update_product_unit(self, pricing_id: int, container_type: str, size: str) -> bool:
        """Обновление единицы измерения товара."""
        return self.coordinator.update_product_unit(pricing_id, container_type, size)

    def get_product_packaging(self, product_id: int, 
                             kit_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получение информации об упаковке товара."""
        return self.coordinator.get_product_packaging(product_id, kit_name)

    # Метод для совместимости с оригинальным интерфейсом
    def update_product_name(self, product_id: int, name: str) -> bool:
        """
        Обновление названия товара.
        
        Args:
            product_id: ID товара
            name: Новое название
        
        Returns:
            True если успешно, False если ошибка
        """
        try:
            query = "UPDATE products SET name = %s WHERE id = %s"
            self.db.execute_query(query, (name, product_id))
            return True
            
        except Exception as e:
            from loguru import logger
            logger.error(f"Ошибка при обновлении названия товара {product_id}: {e}")
            return False