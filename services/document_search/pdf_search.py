"""
MODULE: services.document_search.pdf_search
RESPONSIBILITY: Search for product matches within PDF files.
ALLOWED: pdf_processor, additional_phrases, logging, time, gc.
FORBIDDEN: Direct database access.
ERRORS: None.

Модуль для поиска совпадений в PDF файлах.

Содержит методы поиска товаров и дополнительных фраз в PDF документах.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import time
import gc

from loguru import logger

from services.document_search.pdf_processor import PDFProcessor
from services.document_search.additional_phrases import get_additional_search_phrases


def search_pdf_for_products(
    file_path: Path,
    product_patterns: Dict[str, Dict[str, Any]],
    check_keywords_match_func,
) -> List[Dict[str, Any]]:
    """
    Парсинг PDF и поиск совпадений с названиями товаров по ключевым словам.
    
    Args:
        file_path: Путь к PDF файлу
        product_patterns: Словарь паттернов для поиска товаров
        check_keywords_match_func: Функция проверки совпадения ключевых слов
        
    Returns:
        Список найденных совпадений
    """
    if not product_patterns:
        logger.warning("Список паттернов товаров пуст, поиск не будет выполнен.")
        return []
    
    pdf_processor = PDFProcessor()
    found_matches: Dict[str, Dict[str, Any]] = {}
    
    cells_processed = 0
    last_log_time = time.time()
    log_interval = 10.0
    
    try:
        for cell_info in pdf_processor.iter_pdf_cells(file_path):
            cells_processed += 1
            current_time = time.time()
            
            if current_time - last_log_time >= log_interval or cells_processed % 10000 == 0:
                logger.info(
                    f"Обработка PDF {file_path.name}: обработано {cells_processed} элементов..."
                )
                last_log_time = current_time
            
            text = cell_info["text"]
            display_text = cell_info["display_text"]
            cell_matches: List[Tuple[str, Dict[str, Any]]] = []
            
            match_func = check_keywords_match_func or check_keywords_match
            for product_name, pattern in product_patterns.items():
                match_result = match_func(text, pattern, product_name)
                if match_result["found"]:
                    cell_matches.append((product_name, match_result))
            
            if not cell_matches:
                continue
            
            full_matches = [
                (product_name, match_result)
                for product_name, match_result in cell_matches
                if match_result["full_match"]
            ]
            if full_matches:
                matches_to_apply = full_matches
            else:
                matches_to_apply = cell_matches
            
            best_product, best_match = max(matches_to_apply, key=lambda item: item[1]["score"])
            
            existing = found_matches.get(best_product)
            if existing and existing.get("score", 0) >= best_match["score"]:
                continue
            
            found_matches[best_product] = {
                "product_name": best_product,
                "score": best_match["score"],
                "matched_text": text,
                "matched_display_text": display_text,
                "sheet_name": cell_info["sheet_name"],
                "row": cell_info["row"],
                "column": cell_info["column"],
                "cell_address": cell_info["cell_address"],
                "matched_keywords": best_match["matched_keywords"],
                "row_data": {},
                "full_row": [],
                "left_context": [],
                "right_context": [],
                "column_names": {},
            }
    except Exception as error:
        logger.error(f"Ошибка при обработке PDF {file_path.name}: {error}")
        return []
    
    if cells_processed > 0:
        logger.info(
            f"PDF {file_path.name} обработан: {cells_processed} элементов, найдено совпадений: {len(found_matches)}"
        )
    
    filtered_matches = [
        match for match in found_matches.values()
        if match.get("score", 0) >= 85.0
    ]
    
    sorted_matches = sorted(filtered_matches, key=lambda item: item["score"], reverse=True)
    
    gc.collect()
    
    return sorted_matches[:50]


def search_additional_phrases_in_pdf(file_path: Path, custom_phrases: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Поиск дополнительных фраз в PDF файлах.
    
    Args:
        file_path: Путь к PDF файлу
        custom_phrases: Пользовательские фразы для поиска (объединяются с дополнительными)
        
    Returns:
        Список найденных совпадений с дополнительными фразами
    """
    if custom_phrases:
        phrases = custom_phrases
    else:
        phrases = get_additional_search_phrases()
    if not phrases:
        return []
    
    pdf_processor = PDFProcessor()
    found_matches: Dict[str, Dict[str, Any]] = {}
    
    try:
        for cell_info in pdf_processor.iter_pdf_cells(file_path):
            text = cell_info["text"]
            text_lower = text.casefold()
            
            for phrase in phrases:
                if phrase in text_lower:
                    product_name = phrase
                    
                    existing = found_matches.get(product_name)
                    if existing:
                        if existing.get("score", 0) >= 85.0:
                            continue
                    
                    found_matches[product_name] = {
                        "product_name": product_name,
                        "score": 85.0,
                        "matched_text": text,
                        "matched_display_text": cell_info["display_text"],
                        "sheet_name": cell_info["sheet_name"],
                        "row": cell_info["row"],
                        "column": cell_info["column"],
                        "cell_address": cell_info["cell_address"],
                        "matched_keywords": [phrase],
                        "row_data": {},
                        "full_row": [],
                        "left_context": [],
                        "right_context": [],
                        "column_names": {},
                        "is_additional_phrase": True,
                    }
    except Exception as error:
        logger.error(f"Ошибка при поиске дополнительных фраз в PDF {file_path.name}: {error}")
        return []
    
    return list(found_matches.values())

