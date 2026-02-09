"""
MODULE: services.tender_repositories
RESPONSIBILITY: Expose repository classes.
ALLOWED: Internal modules.
FORBIDDEN: None.
ERRORS: None.

Репозитории для работы с данными торгов.
"""

from services.tender_repositories.okpd_repository import OkpdRepository
from services.tender_repositories.user_okpd_repository import UserOkpdRepository
from services.tender_repositories.okpd_category_repository import OkpdCategoryRepository
from services.tender_repositories.stop_words_repository import StopWordsRepository
from services.tender_repositories.region_repository import RegionRepository

__all__ = [
    'OkpdRepository',
    'UserOkpdRepository',
    'OkpdCategoryRepository',
    'StopWordsRepository',
    'RegionRepository',
]

