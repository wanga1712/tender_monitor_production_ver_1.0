"""
MODULE: services.document_search.excel_utils
RESPONSIBILITY: Utility functions for Excel data manipulation (column letters, formatting).
ALLOWED: re.
FORBIDDEN: Complex business logic, IO operations.
ERRORS: None.

Утилиты для работы с Excel файлами.
"""

from __future__ import annotations

from typing import Any, Optional
import re


def xls_col_to_letter(col_idx: int) -> str:
    """Конвертация номера столбца (0-based) в букву Excel (A, B, C, ...)."""
    result = ""
    col_idx += 1
    while col_idx > 0:
        col_idx -= 1
        result = chr(65 + (col_idx % 26)) + result
        col_idx //= 26
    return result


def letter_to_col_index(column_letter: str) -> int:
    """Конвертация буквы столбца (A, B, C, ...) в индекс (1-based для xlsx, 0-based для xls)."""
    result = 0
    for char in column_letter:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result


def normalize_cell_value(value: Any) -> Optional[str]:
    """Очистка значения ячейки перед поиском."""
    if isinstance(value, str):
        text = value.strip()
    elif isinstance(value, (int, float)):
        text = f"{value}".strip()
    else:
        text = str(value).strip()

    cleaned = re.sub(r"\s+", " ", text)
    return cleaned.casefold() if cleaned else None


def format_cell_display(value: Any) -> str:
    """Подготовка значения ячейки для отображения пользователю."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    return str(value).strip()

