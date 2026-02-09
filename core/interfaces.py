"""
MODULE: core.interfaces
RESPONSIBILITY: Define Protocols and Abstract Base Classes for dependency injection.
ALLOWED: Typing imports, ABC, Protocol.
FORBIDDEN: Implementation details, concrete classes (except data structures).
ERRORS: None.

Интерфейсы (Protocol) для модульного проектирования

Определяет контракты для взаимодействия между модулями,
обеспечивая слабую связанность и возможность тестирования.
"""

from typing import Protocol, Optional, Dict, Any, List
from abc import ABC, abstractmethod


class ITenderMatchRepository(Protocol):
    """Интерфейс для репозитория результатов поиска совпадений"""
    
    def get_match_result(
        self,
        tender_id: int,
        registry_type: str,
    ) -> Optional[Dict[str, Any]]:
        """Получение результата поиска для торга"""
        ...
    
    def get_match_summary(
        self,
        tender_id: int,
        registry_type: str,
    ) -> Optional[Dict[str, Any]]:
        """Получение сводки по совпадениям с разбивкой по типам"""
        ...
    
    def get_match_results_batch(
        self,
        tender_ids: List[int],
        registry_type: str,
    ) -> Dict[int, Dict[str, Any]]:
        """Получение результатов поиска для нескольких торгов"""
        ...


class ITenderRepository(Protocol):
    """Интерфейс для репозитория торгов"""
    
    def get_new_tenders_44fz(
        self,
        user_id: int,
        user_okpd_codes: List[str],
        user_stop_words: List[str],
        region_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Получение новых торгов 44ФЗ"""
        ...
    
    def get_new_tenders_223fz(
        self,
        user_id: int,
        user_okpd_codes: List[str],
        user_stop_words: List[str],
        region_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Получение новых торгов 223ФЗ"""
        ...


class IDocumentSearchService(Protocol):
    """Интерфейс для сервиса поиска документов"""
    
    def ensure_products_loaded(self) -> None:
        """Обеспечить загрузку товаров для поиска"""
        ...
    
    def search_documents_for_tender(
        self,
        tender_id: int,
        registry_type: str,
    ) -> Dict[str, Any]:
        """Поиск товаров в документах торга"""
        ...


class IDatabaseManager(Protocol):
    """Интерфейс для менеджера базы данных"""
    
    def connect(self) -> None:
        """Подключение к базе данных"""
        ...
    
    def disconnect(self) -> None:
        """Отключение от базы данных"""
        ...
    
    def execute_query(
        self,
        query: str,
        params: tuple = (),
        cursor_factory=None,
    ) -> List[Any]:
        """Выполнение SELECT запроса"""
        ...
    
    def execute_update(
        self,
        query: str,
        params: tuple = (),
    ) -> int:
        """Выполнение INSERT/UPDATE/DELETE запроса"""
        ...

