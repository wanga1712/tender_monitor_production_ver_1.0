"""
MODULE: services.document_search.document_search_base
RESPONSIBILITY: Core logic for searching keywords/products within parsed document content.
ALLOWED: keyword_matcher, logging.
FORBIDDEN: Parsing logic (use iterators), IO operations.
ERRORS: None.

Базовый модуль для универсального поиска совпадений в документах.

Содержит общую логику поиска, которая используется для всех типов документов
(Excel, Word, PDF) для соблюдения принципа DRY.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Callable, Optional
import time
import gc

from loguru import logger

from services.document_search.keyword_matcher import check_keywords_match


def search_document_for_products(
    file_path: Path,
    cell_iterator: Iterable[Dict[str, Any]],
    product_patterns: Dict[str, Dict[str, Any]],
    check_keywords_match_func: Optional[Callable] = None,
    file_type: str = "документ",
    log_interval: float = 10.0,
    log_every_n: int = 10000,
    extract_row_data_func: Optional[Callable] = None,
    extract_row_context_func: Optional[Callable] = None,
    stop_phrases: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Универсальный метод поиска совпадений товаров в документах любого типа.
    
    Args:
        file_path: Путь к файлу
        cell_iterator: Итератор по ячейкам/элементам документа
        product_patterns: Словарь паттернов для поиска товаров
        check_keywords_match_func: Функция проверки совпадения (по умолчанию check_keywords_match)
        file_type: Тип файла для логирования
        log_interval: Интервал логирования в секундах
        log_every_n: Логировать каждые N элементов
        extract_row_data_func: Опциональная функция извлечения данных строки (для Excel)
        extract_row_context_func: Опциональная функция извлечения контекста строки (для Excel)
        
    Returns:
        Список найденных совпадений (топ-50, отсортированные по score)
    """
    if not product_patterns:
        logger.warning("Список паттернов товаров пуст, поиск не будет выполнен.")
        return []

    match_func = check_keywords_match_func or check_keywords_match
    found_matches: Dict[str, Dict[str, Any]] = {}

    cells_processed = 0
    last_log_time = time.time()

    # Нормализуем стоп-фразы один раз
    normalized_stop_phrases: List[str] = []
    if stop_phrases:
        normalized_stop_phrases = [phrase.casefold() for phrase in stop_phrases if phrase.strip()]

    try:
        for cell_info in cell_iterator:
            cells_processed += 1
            current_time = time.time()
            
            if current_time - last_log_time >= log_interval or cells_processed % log_every_n == 0:
                logger.info(
                    f"Обработка {file_type} {file_path.name}: обработано {cells_processed} элементов..."
                )
                last_log_time = current_time

            text = cell_info.get("text", "")
            display_text = cell_info.get("display_text", text)

            text_lower = text.casefold()

            # Если текст ячейки содержит одну из стоп-фраз, полностью пропускаем её
            if normalized_stop_phrases and any(phrase in text_lower for phrase in normalized_stop_phrases):
                continue
            cell_matches: List[Tuple[str, Dict[str, Any]]] = []

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

            # Базовые данные совпадения
            match_data = {
                "product_name": best_product,
                "score": best_match["score"],
                "matched_text": text,
                "matched_display_text": display_text,
                "sheet_name": cell_info.get("sheet_name", ""),
                "row": cell_info.get("row", 0),
                "column": cell_info.get("column", ""),
                "cell_address": cell_info.get("cell_address", ""),
                "matched_keywords": best_match["matched_keywords"],
            }
            
            # Дополнительные данные для Excel (если функции предоставлены)
            if extract_row_data_func and extract_row_context_func:
                try:
                    row_data = extract_row_data_func(
                        file_path,
                        cell_info.get("sheet_name", ""),
                        cell_info.get("row", 0)
                    )
                    full_row_context = extract_row_context_func(
                        file_path,
                        cell_info.get("sheet_name", ""),
                        cell_info.get("row", 0),
                        cell_info.get("column", ""),
                        context_cols=3
                    )
                    match_data.update({
                        "row_data": row_data,
                        "full_row": full_row_context.get("full_row", []),
                        "left_context": full_row_context.get("left_context", []),
                        "right_context": full_row_context.get("right_context", []),
                        "column_names": full_row_context.get("column_names", {}),
                    })
                except Exception as error:
                    logger.debug(f"Не удалось извлечь контекст строки: {error}")
                    match_data.update({
                        "row_data": {},
                        "full_row": [],
                        "left_context": [],
                        "right_context": [],
                        "column_names": {},
                    })
            else:
                # Для PDF и Word - пустые значения
                match_data.update({
                    "row_data": {},
                    "full_row": [],
                    "left_context": [],
                    "right_context": [],
                    "column_names": {},
                })

            found_matches[best_product] = match_data

        if cells_processed > 0:
            logger.info(
                f"{file_type.capitalize()} {file_path.name} обработан: {cells_processed} элементов, "
                f"найдено совпадений: {len(found_matches)}"
            )
        
        filtered_matches = [
            match for match in found_matches.values()
            if match.get("score", 0) >= 85.0
        ]
        
        sorted_matches = sorted(filtered_matches, key=lambda item: item["score"], reverse=True)
        
        gc.collect()
        
        return sorted_matches[:50]
    except Exception as error:
        logger.error(f"Ошибка при обработке {file_type} {file_path.name}: {error}")
        return []

