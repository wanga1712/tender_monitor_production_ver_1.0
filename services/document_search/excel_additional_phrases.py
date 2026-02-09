"""
MODULE: services.document_search.excel_additional_phrases
RESPONSIBILITY: Search for additional phrases specifically in Excel files.
ALLOWED: ExcelParser, additional_phrases, logging.
FORBIDDEN: Parsing logic (use ExcelParser).
ERRORS: None.

Модуль для поиска дополнительных фраз в Excel файлах.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from services.document_search.excel_parser import ExcelParser
from services.document_search.additional_phrases import get_additional_search_phrases


def search_additional_phrases_in_excel(
    file_path: Path,
    excel_parser: ExcelParser,
    custom_phrases: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Поиск дополнительных фраз в Excel файлах.
    
    Args:
        file_path: Путь к Excel файлу
        excel_parser: Парсер Excel файлов
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
    
    found_matches: Dict[str, Dict[str, Any]] = {}
    
    for cell_info in excel_parser.iter_workbook_cells(file_path):
        text = cell_info["text"]
        text_lower = text.casefold()
        
        # Ищем каждую фразу в тексте
        for phrase in phrases:
            if phrase in text_lower:
                # Если фраза найдена, создаем совпадение
                product_name = phrase  # Используем саму фразу как название
                
                existing = found_matches.get(product_name)
                if existing:
                    # Если уже есть совпадение, проверяем оценку
                    if existing.get("score", 0) >= 85.0:
                        continue
                
                row_data = excel_parser.extract_row_data(
                    file_path,
                    cell_info["sheet_name"],
                    cell_info["row"]
                )
                
                full_row_context = excel_parser.extract_full_row_with_context(
                    file_path,
                    cell_info["sheet_name"],
                    cell_info["row"],
                    cell_info["column"],
                    context_cols=3
                )
                
                found_matches[product_name] = {
                    "product_name": product_name,
                    "score": 85.0,  # Фиксированная оценка для дополнительных фраз
                    "matched_text": text,
                    "matched_display_text": cell_info["display_text"],
                    "sheet_name": cell_info["sheet_name"],
                    "row": cell_info["row"],
                    "column": cell_info["column"],
                    "cell_address": cell_info["cell_address"],
                    "matched_keywords": [phrase],
                    "row_data": row_data,
                    "full_row": full_row_context.get("full_row", []),
                    "left_context": full_row_context.get("left_context", []),
                    "right_context": full_row_context.get("right_context", []),
                    "column_names": full_row_context.get("column_names", {}),
                    "is_additional_phrase": True,  # Флаг, что это дополнительная фраза
                }
    
    return list(found_matches.values())

