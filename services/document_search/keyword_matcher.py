"""
MODULE: services.document_search.keyword_matcher
RESPONSIBILITY: Tokenize phrases and match keywords against text (fuzzy matching).
ALLOWED: re, rapidfuzz.
FORBIDDEN: IO operations, database access.
ERRORS: None.

Модуль для работы с ключевыми словами и проверки совпадений.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import re

from rapidfuzz import fuzz


def extract_keywords(product_name: str) -> Optional[Dict[str, Any]]:
    """
    Извлечение ключевых слов из названия товара.
    
    Примеры:
    - "ДенсТоп ЭП 203 (Комплект_1)" -> ["денстоп", "эп", "203"]
    - "Реолен Адмикс Плюс" -> ["реолен", "адмикс", "плюс"]
    """
    cleaned = re.sub(r'\([^)]*\)', '', product_name).strip()
    if not cleaned:
        return None

    keywords: List[str] = []
    normalized_full_name = re.sub(r'\s+', ' ', cleaned).casefold()
    full_phrase = normalized_full_name if len(normalized_full_name) >= 3 else None

    words = re.findall(r'[а-яёА-ЯЁa-zA-Z0-9]+', cleaned, re.IGNORECASE)

    for word in words:
        word_lower = word.casefold()
        if len(word_lower) < 3:
            continue
        if word_lower.isdigit():
            continue
        keywords.append(word_lower)

    seen = set()
    unique_keywords = []
    for keyword in keywords:
        if keyword not in seen:
            unique_keywords.append(keyword)
            seen.add(keyword)

    if not keywords and not full_phrase:
        return None

    return {
        "full_phrase": full_phrase,
        "tokens": unique_keywords,
    }


def check_keywords_match(
    text: str,
    pattern: Dict[str, Any],
    product_name: str
) -> Dict[str, Any]:
    """
    Проверка наличия ключевых слов в тексте с учетом возможных опечаток.
    
    Returns:
        Dict с полями:
        - found: bool - найдены ли ключевые слова
        - score: float - процент совпадения
        - matched_keywords: List[str] - список найденных ключевых слов
        - full_match: bool - точное совпадение
    """
    keywords = pattern.get("tokens", [])
    full_phrase = pattern.get("full_phrase")

    if not keywords and not full_phrase:
        return {"found": False, "score": 0.0, "matched_keywords": [], "full_match": False}
    
    text_lower = text.casefold()
    matched_keywords = []

    product_name_clean = re.sub(r'\([^)]*\)', '', product_name).strip()
    product_name_normalized = re.sub(r'\s+', ' ', product_name_clean).casefold()
    
    if product_name_normalized and product_name_normalized in text_lower:
        return {
            "found": True,
            "score": 100.0,
            "matched_keywords": [product_name_normalized],
            "full_match": True,
        }

    if full_phrase and full_phrase in text_lower:
        matched_keywords.append(full_phrase)
        return {
            "found": True,
            "score": 100.0,
            "matched_keywords": matched_keywords,
            "full_match": True,
        }
    
    for keyword in keywords:
        if keyword in text_lower:
            matched_keywords.append(keyword)
            continue
        
        if len(keyword) >= 3:
            words_in_text = re.findall(r'[а-яёА-ЯЁa-zA-Z0-9]+', text_lower)
            for word in words_in_text:
                if len(word) >= 3:
                    similarity = fuzz.ratio(keyword, word)
                    if similarity >= 85:
                        matched_keywords.append(keyword)
                        break
    
    if not matched_keywords:
        return {"found": False, "score": 0.0, "matched_keywords": [], "full_match": False}
    
    keywords_ratio = len(matched_keywords) / len(keywords) if keywords else 0
    
    if keywords_ratio >= 0.6:
        score = 85.0 + (keywords_ratio - 0.6) * 15.0
    elif keywords_ratio >= 0.3:
        score = 35.0 + (keywords_ratio - 0.3) * 50.0
    else:
        return {"found": False, "score": 0.0, "matched_keywords": [], "full_match": False}
    
    return {
        "found": True,
        "score": score,
        "matched_keywords": matched_keywords,
        "full_match": False,
    }

