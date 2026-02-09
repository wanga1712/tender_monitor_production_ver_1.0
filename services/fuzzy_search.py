"""
MODULE: services.fuzzy_search
RESPONSIBILITY: Fuzzy search for products using rapidfuzz or fallback.
ALLOWED: rapidfuzz, fuzzywuzzy, difflib, logging.
FORBIDDEN: Database connection (input is list of dicts).
ERRORS: None.

Сервис нечеткого поиска (fuzzy search) для обработки опечаток

Использует библиотеку rapidfuzz для поиска товаров с учетом возможных опечаток
в названиях. Например, "ipone" найдет "iphone".
"""

from typing import List, Dict, Any, Optional
from loguru import logger

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    try:
        # Fallback на fuzzywuzzy если rapidfuzz недоступен
        from fuzzywuzzy import fuzz, process
        RAPIDFUZZ_AVAILABLE = True
        logger.warning("Используется fuzzywuzzy вместо rapidfuzz. Рекомендуется установить rapidfuzz для лучшей производительности.")
    except ImportError:
        # Если обе библиотеки недоступны, используем встроенный difflib
        import difflib
        RAPIDFUZZ_AVAILABLE = False
        logger.warning("Библиотеки rapidfuzz и fuzzywuzzy не установлены. Используется встроенный difflib (менее точный поиск).")


def fuzzy_search_products(
    products: List[Dict[str, Any]],
    search_text: str,
    threshold: int = 70,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Нечеткий поиск товаров с учетом опечаток
    
    Args:
        products: Список товаров для поиска
        search_text: Текст для поиска (может содержать опечатки)
        threshold: Минимальный порог совпадения (0-100). Чем выше, тем строже поиск
        limit: Максимальное количество результатов
    
    Returns:
        Отсортированный список товаров по релевантности
    
    Examples:
        >>> products = [{'name': 'iPhone 13'}, {'name': 'Samsung Galaxy'}]
        >>> fuzzy_search_products(products, 'ipone', threshold=70)
        [{'name': 'iPhone 13', 'match_score': 91}]
    """
    if not search_text or not products:
        return products[:limit]
    
    search_text_lower = search_text.lower().strip()
    
    if not RAPIDFUZZ_AVAILABLE:
        # Используем встроенный difflib как fallback
        return _fuzzy_search_difflib(products, search_text_lower, threshold, limit)
    
    # Используем rapidfuzz для точного поиска
    try:
        # Извлекаем названия товаров
        product_names = [p.get('name', '') for p in products]
        
        # Выполняем нечеткий поиск
        try:
            matches = process.extract(
                search_text_lower,
                product_names,
                scorer=fuzz.WRatio,  # Weighted Ratio - лучший алгоритм для поиска
                limit=limit,
                score_cutoff=threshold
            )
        except Exception as e:
            logger.error(f"Ошибка при выполнении fuzzy search: {e}")
            return _simple_search(products, search_text_lower, limit)
        
        # Создаем словарь для быстрого доступа
        products_dict = {p.get('name', ''): p for p in products}
        
        # Формируем результат с оценками совпадения
        results = []
        for match in matches:
            # process.extract возвращает кортеж (name, score, index) или (name, score)
            if len(match) >= 2:
                name = match[0]
                score = match[1]
                if name in products_dict:
                    product = products_dict[name].copy()
                    product['match_score'] = score
                    results.append(product)
        
        # Сортируем по убыванию релевантности
        results.sort(key=lambda x: x.get('match_score', 0), reverse=True)
        
        logger.debug(f"Fuzzy search: '{search_text}' -> найдено {len(results)} товаров")
        return results
        
    except Exception as e:
        logger.error(f"Ошибка при нечетком поиске: {e}")
        # Fallback на обычный поиск
        return _simple_search(products, search_text_lower, limit)


def _fuzzy_search_difflib(
    products: List[Dict[str, Any]],
    search_text: str,
    threshold: int,
    limit: int
) -> List[Dict[str, Any]]:
    """
    Нечеткий поиск с использованием встроенного difflib
    
    Менее точный, но не требует дополнительных зависимостей
    """
    import difflib
    
    results = []
    for product in products:
        name = product.get('name', '').lower()
        # Вычисляем коэффициент схожести
        ratio = difflib.SequenceMatcher(None, search_text, name).ratio()
        score = int(ratio * 100)
        
        if score >= threshold:
            product_copy = product.copy()
            product_copy['match_score'] = score
            results.append(product_copy)
    
    # Сортируем по убыванию релевантности
    results.sort(key=lambda x: x.get('match_score', 0), reverse=True)
    return results[:limit]


def _simple_search(
    products: List[Dict[str, Any]],
    search_text: str,
    limit: int
) -> List[Dict[str, Any]]:
    """
    Простой поиск по вхождению подстроки (fallback)
    """
    results = []
    search_text_lower = search_text.lower()
    
    for product in products:
        name = product.get('name', '').lower()
        if search_text_lower in name:
            results.append(product)
            if len(results) >= limit:
                break
    
    return results


def combine_search_results(
    exact_matches: List[Dict[str, Any]],
    fuzzy_matches: List[Dict[str, Any]],
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Объединение результатов точного и нечеткого поиска
    
    Сначала показываются точные совпадения, затем нечеткие
    
    Args:
        exact_matches: Результаты точного поиска
        fuzzy_matches: Результаты нечеткого поиска
        limit: Максимальное количество результатов
    
    Returns:
        Объединенный список товаров
    """
    # Убираем дубликаты (товары, которые есть в обоих списках)
    exact_names = {p.get('name') for p in exact_matches}
    fuzzy_unique = [p for p in fuzzy_matches if p.get('name') not in exact_names]
    
    # Объединяем: сначала точные совпадения, потом нечеткие
    combined = exact_matches + fuzzy_unique
    
    return combined[:limit]

