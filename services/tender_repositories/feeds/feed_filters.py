"""
MODULE: services.tender_repositories.feeds.feed_filters
RESPONSIBILITY: Define data structures for feed filters.
ALLOWED: dataclasses, typing.
FORBIDDEN: Business logic, database operations.
ERRORS: None.

Модуль с фильтрами для загрузки тендеров.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class FeedFilters:
    """Базовые фильтры для выборки тендеров."""

    user_id: int
    okpd_codes: List[str]
    stop_words: List[str]
    region_id: Optional[int]
    category_id: Optional[int]
    limit: int


@dataclass
class WonFilters(FeedFilters):
    """Фильтры для разыгранных тендеров."""

    min_delivery_days: int = 90

