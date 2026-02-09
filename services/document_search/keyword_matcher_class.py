"""
Класс KeywordMatcher для обратной совместимости.
Создан для замены отсутствующего класса в оригинальном коде.
"""

from typing import List, Dict, Any, Optional
from .keyword_matcher import extract_keywords, check_keywords_match


class KeywordMatcher:
    """Класс-обертка для функций работы с ключевыми словами."""
    
    def __init__(self):
        """Инициализация матчера ключевых слов."""
        pass
    
    def find_keywords(self, text: str, keywords: List[str]) -> Dict[str, Any]:
        """
        Поиск ключевых слов в тексте.
        
        Args:
            text: Текст для анализа
            keywords: Список ключевых слов для поиска
            
        Returns:
            Результаты поиска ключевых слов
        """
        if not text or not keywords:
            return {"found": False, "score": 0.0, "matched_keywords": [], "full_match": False}
        
        # Используем существующую логику из check_keywords_match
        results = []
        matched_keywords = []
        
        for keyword in keywords:
            # Создаем pattern для существующей функции
            pattern = {
                "tokens": [keyword.lower()],
                "full_phrase": keyword.lower() if len(keyword) >= 3 else None
            }
            
            result = check_keywords_match(text, pattern, keyword)
            if result["found"]:
                results.append(result)
                matched_keywords.extend(result["matched_keywords"])
        
        if not results:
            return {"found": False, "score": 0.0, "matched_keywords": [], "full_match": False}
        
        # Агрегируем результаты
        max_score = max((r["score"] for r in results), default=0.0)
        full_match = any(r["full_match"] for r in results)
        
        return {
            "found": True,
            "score": max_score,
            "matched_keywords": matched_keywords,
            "full_match": full_match
        }
    
    def extract_keywords_from_product(self, product_name: str) -> Optional[Dict[str, Any]]:
        """
        Извлечение ключевых слов из названия продукта.
        
        Args:
            product_name: Название продукта
            
        Returns:
            Словарь с извлеченными ключевыми словами
        """
        return extract_keywords(product_name)