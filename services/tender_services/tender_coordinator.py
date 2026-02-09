"""
Координатор для сервисов работы с тендерами.

Оркестрирует работу всех специализированных сервисов,
предоставляя единую точку доступа к функциональности.
"""

from typing import List, Dict, Any, Optional
from loguru import logger

from core.tender_database import TenderDatabaseManager
from .okpd_service import OKPDService
from .user_settings_service import UserSettingsService
from .tender_feed_service import TenderFeedService
from .document_service import DocumentService


class TenderCoordinator:
    """Координатор для сервисов работы с тендерами."""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
        self.okpd_service = OKPDService(db_manager)
        self.user_settings_service = UserSettingsService(db_manager)
        self.tender_feed_service = TenderFeedService(db_manager)
        self.document_service = DocumentService(db_manager)
    
    # OKPD методы
    def search_okpd_codes(self, search_text: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Поиск OKPD кодов."""
        return self.okpd_service.search_okpd_codes(search_text, limit)
    
    def get_all_okpd_codes(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Получение всех OKPD кодов."""
        return self.okpd_service.get_all_okpd_codes(limit)
    
    def get_user_okpd_codes(self, user_id: int, category_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Получение OKPD кодов пользователя."""
        return self.okpd_service.get_user_okpd_codes(user_id, category_id)
    
    def add_user_okpd_code(self, user_id: int, okpd_code: str, name: Optional[str] = None, 
                         setting_id: Optional[int] = None) -> Optional[int]:
        """Добавление OKPD кода пользователю."""
        return self.okpd_service.add_user_okpd_code(user_id, okpd_code, name, setting_id)
    
    def remove_user_okpd_code(self, user_id: int, okpd_id: int) -> bool:
        """Удаление OKPD кода пользователя."""
        return self.okpd_service.remove_user_okpd_code(user_id, okpd_id)
    
    def get_okpd_by_code(self, okpd_code: str) -> Optional[Dict[str, Any]]:
        """Получение OKPD кода по значению."""
        return self.okpd_service.get_okpd_by_code(okpd_code)
    
    # Настройки пользователя
    def save_user_search_settings(self, user_id: int, region_id: Optional[int], 
                               category_id: Optional[int]) -> bool:
        """Сохранение настроек поиска пользователя."""
        return self.user_settings_service.save_user_search_settings(user_id, region_id, category_id)
    
    def get_user_search_settings(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получение настроек поиска пользователя."""
        return self.user_settings_service.get_user_search_settings(user_id)
    
    def get_all_regions(self) -> List[Dict[str, Any]]:
        """Получение всех регионов."""
        return self.user_settings_service.get_all_regions()
    
    def search_okpd_codes_by_region(self, search_text: Optional[str], 
                                  region_id: Optional[int], limit: int = 100) -> List[Dict[str, Any]]:
        """Поиск OKPD кодов с фильтрацией по региону."""
        return self.user_settings_service.search_okpd_codes_by_region(search_text, region_id, limit)
    
    # Стоп-слова и фразы
    def get_user_stop_words(self, user_id: int) -> List[Dict[str, Any]]:
        """Получение стоп-слов пользователя."""
        return self.user_settings_service.get_user_stop_words(user_id)
    
    def add_user_stop_words(self, user_id: int, stop_words: List[str], 
                          setting_id: Optional[int] = None) -> Dict[str, Any]:
        """Добавление стоп-слов пользователю."""
        return self.user_settings_service.add_user_stop_words(user_id, stop_words, setting_id)
    
    def remove_user_stop_word(self, user_id: int, stop_word_id: int) -> bool:
        """Удаление стоп-слова пользователя."""
        return self.user_settings_service.remove_user_stop_word(user_id, stop_word_id)
    
    def get_document_stop_phrases(self, user_id: int) -> List[Dict[str, Any]]:
        """Получение стоп-фраз для документов."""
        return self.user_settings_service.get_document_stop_phrases(user_id)
    
    def add_document_stop_phrases(self, user_id: int, phrases: List[str], 
                                setting_id: Optional[int] = None) -> Dict[str, Any]:
        """Добавление стоп-фраз для документов."""
        return self.user_settings_service.add_document_stop_phrases(user_id, phrases, setting_id)
    
    def remove_document_stop_phrase(self, user_id: int, phrase_id: int) -> bool:
        """Удаление стоп-фразы для документов."""
        return self.user_settings_service.remove_document_stop_phrase(user_id, phrase_id)
    
    def get_user_search_phrases(self, user_id: int) -> List[Dict[str, Any]]:
        """Получение поисковых фраз пользователя."""
        return self.user_settings_service.get_user_search_phrases(user_id)
    
    def get_total_tenders_count(self) -> Dict[str, int]:
        """
        Получение общего количества записей в таблицах закупок.
        
        Returns:
            Dict с ключами 'total_44fz', 'total_223fz', 'total_all'
        """
        return self.tender_feed_service.get_total_tenders_count()
    
    def add_user_search_phrases(self, user_id: int, phrases: List[str], 
                              setting_id: Optional[int] = None) -> Dict[str, Any]:
        """Добавление поисковых фраз пользователю."""
        return self.user_settings_service.add_user_search_phrases(user_id, phrases, setting_id)
    
    def remove_user_search_phrase(self, user_id: int, phrase_id: int) -> bool:
        """Удаление поисковой фразы пользователя."""
        return self.user_settings_service.remove_user_search_phrase(user_id, phrase_id)
    
    # Фиды тендеров
    def get_new_tenders_44fz(self, user_id: int, user_okpd_codes: Optional[List[str]] = None, 
                           user_stop_words: Optional[List[str]] = None, region_id: Optional[int] = None,
                           category_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Получение новых тендеров 44-ФЗ."""
        return self.tender_feed_service.get_new_tenders_44fz(user_id, user_okpd_codes, user_stop_words, region_id, category_id, limit)
    
    def get_new_tenders_223fz(self, user_id: int, user_okpd_codes: Optional[List[str]] = None, 
                            user_stop_words: Optional[List[str]] = None, region_id: Optional[int] = None,
                            category_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Получение новых тендеров 223-ФЗ."""
        return self.tender_feed_service.get_new_tenders_223fz(user_id, user_okpd_codes, user_stop_words, region_id, category_id, limit)
    
    def get_won_tenders_44fz(self, user_id: int, user_okpd_codes: Optional[List[str]] = None, 
                           user_stop_words: Optional[List[str]] = None, region_id: Optional[int] = None,
                           category_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Получение выигранных тендеров 44-ФЗ."""
        return self.tender_feed_service.get_won_tenders_44fz(user_id, user_okpd_codes, user_stop_words, region_id, category_id, limit)
    
    def get_won_tenders_223fz(self, user_id: int, user_okpd_codes: Optional[List[str]] = None, 
                            user_stop_words: Optional[List[str]] = None, region_id: Optional[int] = None,
                            category_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Получение выигранных тендеров 223-ФЗ."""
        return self.tender_feed_service.get_won_tenders_223fz(user_id, user_okpd_codes, user_stop_words, region_id, category_id, limit)
    
    def get_commission_tenders_44fz(self, user_id: int, user_okpd_codes: Optional[List[str]] = None, 
                                  user_stop_words: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Получение комиссионных тендеров 44-ФЗ."""
        return self.tender_feed_service.get_commission_tenders_44fz(user_id, user_okpd_codes, user_stop_words)
    
    def get_commission_tenders_223fz(self, user_id: int, user_okpd_codes: Optional[List[str]] = None, 
                                   user_stop_words: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Получение комиссионных тендеров 223-ФЗ."""
        return self.tender_feed_service.get_commission_tenders_223fz(user_id, user_okpd_codes, user_stop_words)
    
    # Документы тендеров
    def get_tender_documents(self, tender_id: int, registry_type: str) -> List[Dict[str, Any]]:
        """Получение документов тендера."""
        return self.document_service.get_tender_documents(tender_id, registry_type)
    
    def get_tenders_by_ids(self, tender_ids_44fz: Optional[List[int]] = None, 
                         tender_ids_223fz: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """Получение тендеров по IDs."""
        return self.document_service.get_tenders_by_ids(tender_ids_44fz, tender_ids_223fz)
    
    # Категории OKPD
    def get_okpd_categories(self, user_id: int) -> List[Dict[str, Any]]:
        """Получение категорий OKPD пользователя."""
        return self.okpd_service.get_okpd_categories(user_id)
    
    def create_okpd_category(self, user_id: int, name: str, description: Optional[str] = None) -> Optional[int]:
        """Создание категории OKPD."""
        return self.okpd_service.create_okpd_category(user_id, name, description)
    
    def update_okpd_category(self, category_id: int, user_id: int, name: Optional[str] = None, 
                           description: Optional[str] = None) -> bool:
        """Обновление категории OKPD."""
        return self.okpd_service.update_okpd_category(category_id, user_id, name, description)
    
    def delete_okpd_category(self, category_id: int, user_id: int) -> bool:
        """Удаление категории OKPD."""
        return self.okpd_service.delete_okpd_category(category_id, user_id)
    
    def assign_okpd_to_category(self, user_id: int, okpd_id: int, category_id: Optional[int] = None) -> bool:
        """Назначение OKPD кода категории."""
        return self.okpd_service.assign_okpd_to_category(user_id, okpd_id, category_id)
    
    def get_okpd_codes_by_category(self, user_id: int, category_id: Optional[int] = None) -> List[str]:
        """Получение OKPD кодов по категории."""
        return self.okpd_service.get_okpd_codes_by_category(user_id, category_id)
    
    # Вспомогательные методы
    def _resolve_okpd_codes(self, user_id: int, category_id: Optional[int], fallback: Optional[List[str]]) -> List[str]:
        """Разрешение OKPD кодов для пользователя."""
        if category_id is not None:
            return self.okpd_service.get_okpd_codes_by_category(user_id, category_id)
        elif fallback:
            return fallback
        else:
            user_okpds = self.okpd_service.get_user_okpd_codes(user_id)
            return [okpd['code'] for okpd in user_okpds if okpd.get('code')]
    
    def _attach_documents(self, tenders: List[Dict[str, Any]]) -> None:
        """Прикрепление документов к данным тендеров."""
        self.document_service._attach_documents(tenders)
    
    def _fetch_registry_records(self, registry_type: str, tender_ids: List[int]) -> List[Dict[str, Any]]:
        """Получение записей из реестра по IDs."""
        return self.document_service._fetch_registry_records(registry_type, tender_ids)
