"""
Сервис поиска совпадений в документах.
Отвечает за обнаружение продуктов и ключевых фраз в документах.
"""

from typing import List, Dict, Any
from pathlib import Path
import logging

from services.document_search.match_finder import MatchFinder
from services.document_search.document_selector import DocumentSelector
from services.document_search.keyword_matcher_class import KeywordMatcher

logger = logging.getLogger(__name__)


class MatchDetectionService:
    """Сервис обнаружения совпадений в документах."""
    
    def __init__(self, product_names: List[str], stop_phrases: List[str] = None, 
                 user_search_phrases: List[str] = None):
        """
        Инициализация сервиса поиска совпадений.
        
        Args:
            product_names: Список названий продуктов для поиска
            stop_phrases: Стоп-фразы для исключения
            user_search_phrases: Пользовательские фразы для поиска
        """
        self.match_finder = MatchFinder(
            product_names=product_names,
            stop_phrases=stop_phrases or [],
            user_search_phrases=user_search_phrases or []
        )
        self.selector = DocumentSelector()
        self.keyword_matcher = KeywordMatcher()
    
    def find_matches_in_documents(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Поиск совпадений в подготовленных документах.
        
        Args:
            documents: Список документов для анализа
            
        Returns:
            Результаты поиска совпадений
        """
        try:
            return self.match_finder.find_matches(documents)
        except Exception as e:
            logger.error(f"Ошибка поиска совпадений в документах: {e}")
            raise
    
    def select_relevant_documents(self, documents: List[Dict[str, Any]], 
                                 match_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Выбор релевантных документов на основе результатов поиска.
        
        Args:
            documents: Исходные документы
            match_results: Результаты поиска совпадений
            
        Returns:
            Отобранные релевантные документы
        """
        try:
            return self.selector.select_documents(documents, match_results)
        except Exception as e:
            logger.error(f"Ошибка выбора документов: {e}")
            raise
    
    def analyze_keyword_matches(self, text: str, keywords: List[str]) -> Dict[str, Any]:
        """
        Анализ совпадений ключевых слов в тексте.
        
        Args:
            text: Текст для анализа
            keywords: Ключевые слова для поиска
            
        Returns:
            Результаты анализа ключевых слов
        """
        try:
            return self.keyword_matcher.find_keywords(text, keywords)
        except Exception as e:
            logger.error(f"Ошибка анализа ключевых слов: {e}")
            raise