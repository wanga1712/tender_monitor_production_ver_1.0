"""
MODULE: services.document_search.pandas_reader
RESPONSIBILITY: Fallback mechanism for reading Excel files using Pandas.
ALLOWED: pandas, logging, excel_utils.
FORBIDDEN: Database access.
ERRORS: DocumentSearchError.

Чтение Excel файлов через pandas с разными движками.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Iterable
import gc

from loguru import logger

from core.exceptions import DocumentSearchError
from services.document_search.excel_utils import xls_col_to_letter, normalize_cell_value, format_cell_display

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def iter_pandas_cells(file_path: Path, engine: str = 'xlrd') -> Iterable[Dict[str, Any]]:
    """Итерация по ячейкам через pandas (резервный метод)."""
    if not PANDAS_AVAILABLE:
        raise DocumentSearchError("Библиотека pandas не установлена для резервного открытия файлов.")
    
    # Список движков для попыток открытия
    engines_to_try = [engine]
    
    # Проверяем совместимость xlrd с pandas перед добавлением
    xlrd_compatible = True
    try:
        import xlrd
        xlrd_version = getattr(xlrd, '__version__', '0.0.0')
        try:
            from packaging import version
            if version.parse(xlrd_version) < version.parse('2.0.1'):
                xlrd_compatible = False
                logger.debug(f"xlrd версия {xlrd_version} несовместима с pandas (требуется >= 2.0.1), пропускаем xlrd через pandas")
        except ImportError:
            # packaging не установлен, пробуем все равно
            pass
    except ImportError:
        xlrd_compatible = False
    
    if engine != 'xlrd' and xlrd_compatible:
        engines_to_try.append('xlrd')
    if engine != 'openpyxl':
        engines_to_try.append('openpyxl')
    # calamine может работать с поврежденными файлами, но проверяем доступность
    if engine != 'calamine':
        try:
            import python_calamine
            engines_to_try.append('calamine')
        except ImportError:
            logger.debug("python-calamine не установлен, пропускаем этот движок")
    
    last_error = None
    failed_engines = []
    for try_engine in engines_to_try:
        try:
            # Проверяем совместимость xlrd с pandas
            if try_engine == 'xlrd' and PANDAS_AVAILABLE:
                try:
                    import xlrd
                    # Проверяем версию xlrd - если старая, pandas может не работать
                    xlrd_version = getattr(xlrd, '__version__', '0.0.0')
                    try:
                        from packaging import version
                        if version.parse(xlrd_version) < version.parse('2.0.1'):
                            # Старая версия xlrd - pandas не будет работать, пропускаем
                            logger.debug(f"xlrd версия {xlrd_version} несовместима с pandas, пропускаем движок xlrd")
                            continue
                    except ImportError:
                        # packaging не установлен, пробуем все равно
                        pass
                except ImportError:
                    pass
            
            # Используем контекстный менеджер для гарантированного закрытия файла
            with pd.ExcelFile(str(file_path), engine=try_engine) as excel_file:
                for sheet_name in excel_file.sheet_names:
                    logger.debug(f"Сканирование листа через pandas ({try_engine}): {sheet_name}")
                    try:
                        df = pd.read_excel(excel_file, sheet_name=sheet_name, engine=try_engine, header=None)
                    except Exception as e:
                        logger.warning(f"Не удалось прочитать лист {sheet_name} через pandas ({try_engine}): {e}")
                        continue
                    
                    for row_idx, row in df.iterrows():
                        for col_idx, value in enumerate(row):
                            if pd.isna(value) or value == "":
                                continue
                            
                            text = normalize_cell_value(value)
                            if text:
                                col_letter = xls_col_to_letter(col_idx)
                                cell_address = f"{sheet_name}!{col_letter}{int(row_idx) + 1}"
                                
                                yield {
                                    "text": text,
                                    "display_text": format_cell_display(value),
                                    "sheet_name": sheet_name,
                                    "row": int(row_idx) + 1,
                                    "column": col_letter,
                                    "cell_address": cell_address,
                                }
                    # Освобождаем память DataFrame после обработки листа
                    del df
                    gc.collect()
            # Если дошли сюда, значит файл успешно открыт
            return
        except Exception as error:
            last_error = error
            failed_engines.append((try_engine, str(error)))
            continue
    
    # Если все движки не сработали - просто выбрасываем исключение
    # Логирование будет на уровне вызывающего кода
    if last_error:
        engines_tried = ", ".join([eng for eng, _ in failed_engines])
        raise DocumentSearchError(f"Не удалось открыть файл через pandas ни одним из движков ({engines_tried}): {last_error}") from last_error
    else:
        raise DocumentSearchError("Не удалось открыть файл через pandas")

