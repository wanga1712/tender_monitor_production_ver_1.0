"""
MODULE: services.document_search.xls_row_extractor
RESPONSIBILITY: Extract specific row data (quantity, cost) from .xls sheets.
ALLOWED: xlrd, excel_utils, error_logger, logging.
FORBIDDEN: Database access.
ERRORS: None.

Извлечение данных из строк .xls файлов.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from loguru import logger

from services.error_logger import get_error_logger
from services.document_search.excel_utils import xls_col_to_letter, format_cell_display

try:
    import xlrd
    XLRD_AVAILABLE = True
except ImportError:
    XLRD_AVAILABLE = False


def extract_xls_row_data(
    file_path: Path,
    sheet_name: str,
    row_number: int,
    extract_xlsx_row_data_func=None,
) -> Dict[str, Any]:
    """Извлечение данных из .xls файла."""
    if not XLRD_AVAILABLE:
        return {}
    
    try:
        # Пробуем открыть с разными параметрами для старых файлов
        try:
            workbook = xlrd.open_workbook(str(file_path), formatting_info=False)
        except Exception:
            # Если не получилось, пробуем с другими параметрами
            try:
                workbook = xlrd.open_workbook(str(file_path), on_demand=True)
            except Exception:
                workbook = xlrd.open_workbook(str(file_path))
    except Exception as error:
        error_msg = str(error).lower()
        if "xlsx file" in error_msg and extract_xlsx_row_data_func:
            logger.warning(
                f"Файл {file_path.name} имеет расширение .xls, но содержит формат XLSX (лист {sheet_name}). "
                f"Повторная попытка через openpyxl."
            )
            return extract_xlsx_row_data_func(file_path, sheet_name, row_number)
        logger.error(f"Не удалось открыть документ {file_path}: {error}")
        get_error_logger().log_file_open_error(
            file_path=file_path,
            error_message=str(error),
            file_type="xls",
        )
        return {}
    
    sheet = workbook.sheet_by_name(sheet_name)
    row_idx = row_number - 1
    if row_idx >= sheet.nrows:
        return {}
    
    row_values = []
    max_col = sheet.ncols
    for col_idx in range(max_col):
        cell = sheet.cell(row_idx, col_idx)
        row_values.append(format_cell_display(cell.value) if cell.value else "")
    
    return _extract_row_data_from_values(row_values, max_col, sheet, xls_col_to_letter)


def _extract_row_data_from_values(
    row_values: list,
    max_col: int,
    sheet,
    col_to_letter_func,
) -> Dict[str, Any]:
    """Извлечение данных из значений строки для .xls."""
    column_headers = {}
    header_row = None
    
    for header_row_idx in range(min(5, sheet.nrows)):
        for col_idx in range(min(max_col, 20)):
            cell = sheet.cell(header_row_idx, col_idx)
            if cell.value:
                header_value = str(cell.value).strip().lower()
                if header_value == "количество" or (
                    header_value.startswith("количество") and "количество" not in column_headers
                ):
                    col_letter = col_to_letter_func(col_idx)
                    column_headers["количество"] = {"col": col_letter, "idx": col_idx}
                    if header_row is None:
                        header_row = header_row_idx
    
    if "количество" not in column_headers and max_col >= 4:
        column_headers["количество"] = {"col": "D", "idx": 3}
    
    cost_unit_col = None
    total_cost_col = None
    
    for header_row_idx in range(min(5, sheet.nrows)):
        for col_idx in range(min(max_col, 20)):
            cell = sheet.cell(header_row_idx, col_idx)
            if cell.value:
                header_value = str(cell.value).strip().lower()
                if ("стоимость единицы" in header_value or 
                    (header_value.startswith("стоимость") and "единицы" in header_value)):
                    if cost_unit_col is None:
                        col_letter = col_to_letter_func(col_idx)
                        cost_unit_col = {"col": col_letter, "idx": col_idx, "name": str(cell.value).strip()}
                if "общая стоимость" in header_value or header_value.startswith("общая стоимость"):
                    if total_cost_col is None:
                        col_letter = col_to_letter_func(col_idx)
                        total_cost_col = {"col": col_letter, "idx": col_idx, "name": str(cell.value).strip()}
    
    return _build_result_dict(row_values, column_headers, cost_unit_col, total_cost_col, max_col, col_to_letter_func)


def _build_result_dict(
    row_values: list,
    column_headers: dict,
    cost_unit_col: dict,
    total_cost_col: dict,
    max_col: int,
    col_to_letter_func,
) -> Dict[str, Any]:
    """Построение результирующего словаря для .xls."""
    result = {}
    if "количество" in column_headers:
        col_idx = column_headers["количество"]["idx"]
        if col_idx < len(row_values) and row_values[col_idx]:
            result["количество"] = {
                "value": row_values[col_idx],
                "column": column_headers["количество"]["col"],
                "name": "Количество"
            }
    
    if cost_unit_col:
        col_idx = cost_unit_col["idx"]
        if col_idx < len(row_values) and row_values[col_idx]:
            result["стоимость_единицы"] = {
                "value": row_values[col_idx],
                "column": cost_unit_col["col"],
                "name": cost_unit_col["name"]
            }
    
    if total_cost_col:
        col_idx = total_cost_col["idx"]
        if col_idx < len(row_values) and row_values[col_idx]:
            result["общая_стоимость"] = {
                "value": row_values[col_idx],
                "column": total_cost_col["col"],
                "name": total_cost_col["name"]
            }
    
    return result

