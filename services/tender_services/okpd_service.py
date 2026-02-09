"""
Сервис для работы с OKPD кодами и категориями.

Отвечает за:
- Поиск и получение OKPD кодов
- Управление пользовательскими OKPD кодами
- Работа с категориями OKPD
"""

from typing import List, Dict, Any, Optional
from loguru import logger

from core.tender_database import TenderDatabaseManager
from services.tender_repositories.okpd_repository import OkpdRepository
from services.tender_repositories.user_okpd_repository import UserOkpdRepository
from services.tender_repositories.okpd_category_repository import OkpdCategoryRepository


class OKPDService:
    """Сервис для работы с OKPD кодами и категориями."""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
        self.okpd_repo = OkpdRepository(db_manager)
        self.user_okpd_repo = UserOkpdRepository(db_manager)
        self.okpd_category_repo = OkpdCategoryRepository(db_manager)
    
    def search_okpd_codes(self, search_text: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Поиск OKPD кодов по тексту."""
        return self.okpd_repo.search_okpd_codes(search_text, limit)
    
    def get_all_okpd_codes(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Получение всех OKPD кодов."""
        return self.okpd_repo.get_all_okpd_codes(limit)
    
    def get_user_okpd_codes(self, user_id: int, category_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Получение OKPD кодов пользователя."""
        return self.user_okpd_repo.get_user_okpd_codes(user_id, category_id)
    
    def add_user_okpd_code(self, user_id: int, okpd_code: str, name: Optional[str] = None, 
                          setting_id: Optional[int] = None) -> Optional[int]:
        """Добавление OKPD кода пользователю."""
        return self.user_okpd_repo.add_user_okpd_code(user_id, okpd_code, name, setting_id)
    
    def remove_user_okpd_code(self, user_id: int, okpd_id: int) -> bool:
        """Удаление OKPD кода пользователя."""
        return self.user_okpd_repo.remove_user_okpd_code(user_id, okpd_id)
    
    def get_okpd_by_code(self, okpd_code: str) -> Optional[Dict[str, Any]]:
        """Получение OKPD кода по значению."""
        return self.okpd_repo.get_okpd_by_code(okpd_code)
    
    def get_okpd_categories(self, user_id: int) -> List[Dict[str, Any]]:
        """Получение категорий OKPD пользователя."""
        return self.okpd_category_repo.get_okpd_categories(user_id)
    
    def create_okpd_category(self, user_id: int, name: str, description: Optional[str] = None) -> Optional[int]:
        """Создание категории OKPD."""
        return self.okpd_category_repo.create_okpd_category(user_id, name, description)
    
    def update_okpd_category(self, category_id: int, user_id: int, name: Optional[str] = None, 
                           description: Optional[str] = None) -> bool:
        """Обновление категории OKPD."""
        return self.okpd_category_repo.update_okpd_category(category_id, user_id, name, description)
    
    def delete_okpd_category(self, category_id: int, user_id: int) -> bool:
        """Удаление категории OKPD."""
        return self.okpd_category_repo.delete_okpd_category(category_id, user_id)
    
    def assign_okpd_to_category(self, user_id: int, okpd_id: int, category_id: Optional[int] = None) -> bool:
        """Назначение OKPD кода категории."""
        return self.okpd_category_repo.assign_okpd_to_category(user_id, okpd_id, category_id)
    
    def get_okpd_codes_by_category(self, user_id: int, category_id: Optional[int] = None) -> List[str]:
        """Получение OKPD кодов по категории."""
        return self.okpd_category_repo.get_okpd_codes_by_category(user_id, category_id)