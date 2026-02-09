"""
Координатор для оркестрации сервисов работы с товарами.

Координирует работу всех специализированных сервисов:
- CategoryService - категории и подкатегории
- ManufacturerService - производители
- ProductSearchService - поиск товаров  
- PricingService - цены товаров
- PackagingService - упаковка товаров
"""

from typing import List, Dict, Any, Optional
from core.database import DatabaseManager
from .category_service import CategoryService
from .manufacturer_service import ManufacturerService
from .product_search_service import ProductSearchService
from .pricing_service import PricingService
from .packaging_service import PackagingService


class ProductCoordinator:
    """Координатор сервисов работы с товарами."""

    def __init__(self, db_manager: DatabaseManager):
        """
        Инициализация координатора.
        
        Args:
            db_manager: Менеджер базы данных
        """
        self.db = db_manager
        
        # Инициализация всех сервисов
        self.category_service = CategoryService(db_manager)
        self.manufacturer_service = ManufacturerService(db_manager)
        self.product_search_service = ProductSearchService(db_manager)
        self.pricing_service = PricingService(db_manager)
        self.packaging_service = PackagingService(db_manager)

    # Методы для работы с категориями
    def get_categories(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Получение списка всех категорий."""
        return self.category_service.get_categories(use_cache)

    def get_subcategories(self, category_id: Optional[int] = None, 
                         use_cache: bool = True) -> List[Dict[str, Any]]:
        """Получение списка подкатегорий."""
        return self.category_service.get_subcategories(category_id, use_cache)

    def clear_categories_cache(self) -> None:
        """Очистка кэша категорий."""
        self.category_service.clear_categories_cache()

    def clear_subcategories_cache(self, category_id: Optional[int] = None) -> None:
        """Очистка кэша подкатегорий."""
        self.category_service.clear_subcategories_cache(category_id)

    def clear_all_cache(self) -> None:
        """Очистка всего кэша категорий и подкатегорий."""
        self.category_service.clear_all_cache()

    # Методы для работы с производителями
    def get_manufacturers(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Получение списка всех производителей."""
        return self.manufacturer_service.get_manufacturers(use_cache)

    def clear_manufacturers_cache(self) -> None:
        """Очистка кэша производителей."""
        self.manufacturer_service.clear_manufacturers_cache()

    # Методы для поиска товаров
    def search_products(self, search_text: Optional[str] = None,
                       category_id: Optional[int] = None,
                       subcategory_id: Optional[int] = None,
                       manufacturer_id: Optional[int] = None,
                       limit: int = 100) -> List[Dict[str, Any]]:
        """Поиск товаров по различным критериям."""
        return self.product_search_service.search_products(
            search_text, category_id, subcategory_id, manufacturer_id, limit
        )

    def get_product_by_id(self, product_id: int) -> Optional[Dict[str, Any]]:
        """Получение товара по идентификатору."""
        return self.product_search_service.get_product_by_id(product_id)

    # Методы для работы с ценами
    def get_product_pricing(self, product_id: int) -> List[Dict[str, Any]]:
        """Получение информации о ценах товара."""
        return self.pricing_service.get_product_pricing(product_id)

    def update_product_weight(self, pricing_id: int, weight: float) -> bool:
        """Обновление веса товара."""
        return self.pricing_service.update_product_weight(pricing_id, weight)

    def update_product_price(self, pricing_id: int, price: float) -> bool:
        """Обновление цены товара."""
        return self.pricing_service.update_product_price(pricing_id, price)

    def update_product_unit(self, pricing_id: int, container_type: str, size: str) -> bool:
        """Обновление единицы измерения товара."""
        return self.pricing_service.update_product_unit(pricing_id, container_type, size)

    # Методы для работы с упаковкой
    def get_product_packaging(self, product_id: int, 
                             kit_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получение информации об упаковке товара."""
        return self.packaging_service.get_product_packaging(product_id, kit_name)