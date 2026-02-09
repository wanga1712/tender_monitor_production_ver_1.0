"""
MODULE: core.models
RESPONSIBILITY: Define domain data structures (dataclasses, enums).
ALLOWED: Dataclasses, Enums, Typing.
FORBIDDEN: Business logic, database operations.
ERRORS: None.

Модели данных для CRM системы B2B AutoDesk

Модуль содержит dataclass модели для представления сущностей системы:
- Производители, категории, подкатегории
- Товары с техническими характеристиками
- Цены и упаковка товаров
- Коммерческие предложения
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum


class VehicleType(Enum):
    """Типы транспортных средств для доставки"""
    MANIPULATOR = "manipulator"
    REAR_LOADER = "rear_loader"
    SIDE_LOADER = "side_loader"
    GAZELLE = "gazelle"


@dataclass
class Manufacturer:
    """
    Модель производителя товаров
    
    Attributes:
        id: Уникальный идентификатор производителя
        name: Название производителя
        office_address: Адрес офиса производителя
        factory_address: Адрес завода производителя
        created_at: Дата создания записи
    """
    id: int
    name: str
    office_address: Optional[str]
    factory_address: Optional[str]
    created_at: datetime


@dataclass
class Category:
    """
    Модель категории товаров
    
    Attributes:
        id: Уникальный идентификатор категории
        name: Название категории
        description: Описание категории
        created_at: Дата создания записи
    """
    id: int
    name: str
    description: Optional[str]
    created_at: datetime


@dataclass
class Subcategory:
    """
    Модель подкатегории товаров
    
    Attributes:
        id: Уникальный идентификатор подкатегории
        name: Название подкатегории
        description: Описание подкатегории
        category_id: Идентификатор родительской категории
        created_at: Дата создания записи
    """
    id: int
    name: str
    description: Optional[str]
    category_id: int
    created_at: datetime


@dataclass
class Product:
    """
    Модель товара с полной информацией
    
    Attributes:
        id: Уникальный идентификатор товара
        name: Название товара
        description: Описание товара
        manufacturer_id: Идентификатор производителя
        subcategory_id: Идентификатор подкатегории
        technical_specs: Технические характеристики (JSON)
        application_areas: Области применения
        advantages: Преимущества товара
        consumption: Расход товара
        storage: Условия хранения
        color: Цвет товара
        safety: Информация о безопасности
        url: URL страницы товара
        image_url: URL изображения товара
        pdf_url: URL PDF документации
        local_image_path: Локальный путь к изображению
        local_pdf_path: Локальный путь к PDF
        created_at: Дата создания записи
    """
    id: int
    name: str
    description: Optional[str]
    manufacturer_id: int
    subcategory_id: int
    technical_specs: Dict[str, Any]
    application_areas: Optional[str]
    advantages: Optional[str]
    consumption: Optional[str]
    storage: Optional[str]
    color: Optional[str]
    safety: Optional[str]
    url: Optional[str]
    image_url: Optional[str]
    pdf_url: Optional[str]
    local_image_path: Optional[str]
    local_pdf_path: Optional[str]
    created_at: datetime


@dataclass
class ProductPricing:
    """
    Модель цены товара для конкретного варианта упаковки
    
    Attributes:
        id: Уникальный идентификатор записи о цене
        product_id: Идентификатор товара
        kit_name: Название комплекта/варианта упаковки
        container_type: Тип тары (мешок, бочка, и т.д.)
        size: Размер упаковки
        weight: Вес одной единицы упаковки (кг)
        additional_info: Дополнительная информация
        price: Цена за единицу упаковки
        created_at: Дата создания записи
    """
    id: int
    product_id: int
    kit_name: str
    container_type: str
    size: str
    weight: float
    additional_info: Optional[str]
    price: float
    created_at: datetime


@dataclass
class ProductPackaging:
    """
    Модель информации об упаковке товара
    
    Attributes:
        id: Уникальный идентификатор записи об упаковке
        product_id: Идентификатор товара
        kit_name: Название комплекта
        component: Компонент упаковки
        container: Тип контейнера
        quantity_per_pallet: Количество единиц на паллете
        created_at: Дата создания записи
    """
    id: int
    product_id: int
    kit_name: str
    component: str
    container: str
    quantity_per_pallet: int
    created_at: datetime


@dataclass
class QuotationItem:
    """
    Модель позиции в коммерческом предложении
    
    Attributes:
        product: Товар
        pricing: Информация о цене
        quantity: Количество товара
        packaging: Информация об упаковке (опционально)
    """
    product: Product
    pricing: ProductPricing
    quantity: int
    packaging: Optional[ProductPackaging]


@dataclass
class Quotation:
    """
    Модель коммерческого предложения
    
    Attributes:
        number: Номер коммерческого предложения
        date: Дата создания
        client_info: Информация о клиенте
        items: Список позиций в предложении
        delivery_info: Информация о доставке
        total_amount: Итоговая сумма предложения
    """
    number: str
    date: datetime
    client_info: Dict[str, Any]
    items: List[QuotationItem]
    delivery_info: Dict[str, Any]
    total_amount: float = 0.0