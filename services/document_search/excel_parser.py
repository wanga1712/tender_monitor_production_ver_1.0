"""
MODULE: services.document_search.excel_parser
RESPONSIBILITY: Facade for parsing Excel files (xls/xlsx).
ALLOWED: xlsx_reader, xls_reader, pandas_reader, file_format_detector, row_extractor, logging.
FORBIDDEN: Direct database access.
ERRORS: DocumentSearchError.

Фасад для парсинга Excel файлов.

Класс ExcelParser делегирует работу специализированным модулям:
- file_format_detector: определение формата файла
- xlsx_reader: чтение .xlsx файлов
- xls_reader: чтение .xls файлов
- pandas_reader: чтение через pandas
- row_extractor: извлечение данных из строк
- full_row_extractor: извлечение полных строк с контекстом
- excel_utils: утилиты для работы с Excel
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable

from loguru import logger

from core.exceptions import DocumentSearchError
from services.document_search.file_format_detector import detect_file_format
from services.document_search.xlsx_reader import iter_xlsx_cells
from services.document_search.xls_reader import iter_xls_cells
from services.document_search.pandas_reader import iter_pandas_cells
from services.document_search.xls_row_extractor import extract_xls_row_data
from services.document_search.xlsx_row_extractor import extract_xlsx_row_data
from services.document_search.full_row_extractor import (
    extract_full_row_xls,
    extract_full_row_xlsx,
)

try:
    import xlrd
    XLRD_AVAILABLE = True
except ImportError:
    XLRD_AVAILABLE = False


class ExcelParser:
    """Фасад для парсинга Excel файлов."""

    def iter_workbook_cells(self, file_path: Path) -> Iterable[Dict[str, Any]]:
        """
        Итерация по всем ячейкам Excel (XLSX или XLS) с информацией о позиции.
        
        Поддерживает оба формата:
        - .xlsx через openpyxl
        - .xls через xlrd (с автоматическим fallback, если файл содержит данные .xlsx)
        """
        suffix = file_path.suffix.lower()
        
        if suffix == ".xls":
            yield from iter_xls_cells(
                file_path,
                allow_fallback=True,
                iter_xlsx_cells_func=self._iter_xlsx_cells_wrapper,
                iter_pandas_cells_func=iter_pandas_cells,
            )
            return
        
        try:
            yield from self._iter_xlsx_cells_wrapper(file_path)
        except DocumentSearchError as error:
            if XLRD_AVAILABLE:
                logger.debug(
                    f"openpyxl не смог открыть {file_path} ({error}). Пробую обработать через xlrd."
                )
                yield from iter_xls_cells(
                    file_path,
                    allow_fallback=False,
                    iter_xlsx_cells_func=None,
                    iter_pandas_cells_func=iter_pandas_cells,
                )
            else:
                raise

    def _iter_xlsx_cells_wrapper(self, file_path: Path) -> Iterable[Dict[str, Any]]:
        """Обертка для iter_xlsx_cells с передачей зависимостей."""
        return iter_xlsx_cells(
            file_path,
            iter_xls_cells_func=lambda fp, af: iter_xls_cells(
                fp, af, None, iter_pandas_cells
            ),
            iter_pandas_cells_func=iter_pandas_cells,
        )

    def extract_row_data(self, file_path: Path, sheet_name: str, row_number: int) -> Dict[str, Any]:
        """
        Извлекает данные из строки Excel, включая столбец "Количество" и другие столбцы.
        
        Поддерживает оба формата: .xlsx и .xls
        """
        suffix = file_path.suffix.lower()
        
        try:
            if suffix == ".xls":
                return extract_xls_row_data(
                    file_path,
                    sheet_name,
                    row_number,
                    extract_xlsx_row_data_func=extract_xlsx_row_data,
                )
            else:
                return extract_xlsx_row_data(
                    file_path,
                    sheet_name,
                    row_number,
                    extract_xls_row_data_func=lambda fp, sn, rn: extract_xls_row_data(
                        fp, sn, rn, None
                    ),
                )
        except Exception as error:
            logger.debug(f"Ошибка при извлечении данных строки {row_number} из {sheet_name}: {error}")
            return {}
    
    def extract_full_row_with_context(
        self, 
        file_path: Path, 
        sheet_name: str, 
        row_number: int, 
        found_column: str,
        context_cols: int = 3
    ) -> Dict[str, Any]:
        """
        Извлекает полную строку с соседними ячейками слева и справа, а также имена столбцов.
        
        Args:
            file_path: Путь к файлу
            sheet_name: Имя листа
            row_number: Номер строки (1-based)
            found_column: Буква столбца, где найдено совпадение (например, "F")
            context_cols: Количество столбцов слева и справа для включения в контекст
        
        Returns:
            Dict с полями:
            - full_row: список всех значений строки с именами столбцов
            - left_context: ячейки слева от найденной
            - right_context: ячейки справа от найденной
            - column_names: имена столбцов (из заголовков)
        """
        suffix = file_path.suffix.lower()
        
        try:
            if suffix == ".xls":
                return extract_full_row_xls(
                    file_path,
                    sheet_name,
                    row_number,
                    found_column,
                    context_cols,
                )
            else:
                return extract_full_row_xlsx(
                    file_path,
                    sheet_name,
                    row_number,
                    found_column,
                    context_cols,
                )
        except Exception as error:
            logger.debug(f"Ошибка при извлечении полной строки {row_number} из {sheet_name}: {error}")
            return {}
