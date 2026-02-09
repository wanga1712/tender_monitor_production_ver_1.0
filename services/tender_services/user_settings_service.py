"""
Сервис для работы с пользовательскими настройками.

Отвечает за:
- Настройки поиска пользователя
- Стоп-слова и поисковые фразы
- Регионы и другие пользовательские предпочтения
"""

from typing import List, Dict, Any, Optional
from loguru import logger

from core.tender_database import TenderDatabaseManager
from services.tender_repositories.user_search_settings_repository import UserSearchSettingsRepository
from services.tender_repositories.stop_words_repository import StopWordsRepository
from services.tender_repositories.document_stop_phrases_repository import DocumentStopPhrasesRepository
from services.tender_repositories.user_search_phrases_repository import UserSearchPhrasesRepository
from services.tender_repositories.region_repository import RegionRepository


class UserSettingsService:
    """Сервис для работы с пользовательскими настройками."""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
        self.user_settings_repo = UserSearchSettingsRepository(db_manager)
        self.stop_words_repo = StopWordsRepository(db_manager)
        self.document_stop_phrases_repo = DocumentStopPhrasesRepository(db_manager)
        self.user_search_phrases_repo = UserSearchPhrasesRepository(db_manager)
        self.region_repo = RegionRepository(db_manager)
    
    def save_user_search_settings(self, user_id: int, region_id: Optional[int], 
                               category_id: Optional[int]) -> bool:
        """Сохранение настроек поиска пользователя."""
        return self.user_settings_repo.save_user_settings(user_id, region_id, category_id)
    
    def get_user_search_settings(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получение настроек поиска пользователя."""
        return self.user_settings_repo.get_user_settings(user_id)
    
    def get_all_regions(self) -> List[Dict[str, Any]]:
        """Получение всех регионов."""
        return self.region_repo.get_all_regions()
    
    def search_okpd_codes_by_region(self, search_text: Optional[str], 
                                  region_id: Optional[int], limit: int = 100) -> List[Dict[str, Any]]:
        """Поиск OKPD кодов с фильтрацией по региону."""
        return self.region_repo.search_okpd_codes_by_region(search_text, region_id, limit)
    
    def get_user_stop_words(self, user_id: int) -> List[Dict[str, Any]]:
        """Получение стоп-слов пользователя."""
        return self.stop_words_repo.get_user_stop_words(user_id)
    
    def add_user_stop_words(self, user_id: int, stop_words: List[str], 
                          setting_id: Optional[int] = None) -> Dict[str, Any]:
        """Добавление стоп-слов пользователю."""
        return self.stop_words_repo.add_user_stop_words(user_id, stop_words, setting_id)
    
    def remove_user_stop_word(self, user_id: int, stop_word_id: int) -> bool:
        """Удаление стоп-слова пользователя."""
        return self.stop_words_repo.remove_user_stop_word(user_id, stop_word_id)
    
    def get_document_stop_phrases(self, user_id: int) -> List[Dict[str, Any]]:
        """Получение стоп-фраз для документов."""
        return self.document_stop_phrases_repo.get_document_stop_phrases(user_id)
    
    def add_document_stop_phrases(self, user_id: int, phrases: List[str], 
                                setting_id: Optional[int] = None) -> Dict[str, Any]:
        """Добавление стоп-фраз для документов."""
        return self.document_stop_phrases_repo.add_document_stop_phrases(user_id, phrases, setting_id)
    
    def remove_document_stop_phrase(self, user_id: int, phrase_id: int) -> bool:
        """Удаление стоп-фразы для документов."""
        return self.document_stop_phrases_repo.remove_document_stop_phrase(user_id, phrase_id)
    
    def get_user_search_phrases(self, user_id: int) -> List[Dict[str, Any]]:
        """Получение поисковых фраз пользователя."""
        return self.user_search_phrases_repo.get_user_search_phrases(user_id)
    
    def add_user_search_phrases(self, user_id: int, phrases: List[str], 
                              setting_id: Optional[int] = None) -> Dict[str, Any]:
        """Добавление поисковых фраз пользователю."""
        return self.user_search_phrases_repo.add_user_search_phrases(user_id, phrases, setting_id)
    
    def remove_user_search_phrase(self, user_id: int, phrase_id: int) -> bool:
        """Удаление поисковой фразы пользователя."""
        return self.user_search_phrases_repo.remove_user_search_phrase(user_id, phrase_id)