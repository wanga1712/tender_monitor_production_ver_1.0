# -*- coding: utf-8 -*-
"""
MODULE: scripts.test_excel_files_simple
RESPONSIBILITY: Simple testing of Excel files.
ALLOWED: sys, pathlib, services.document_search.excel_parser, services.document_search.file_format_detector, traceback.
FORBIDDEN: None.
ERRORS: None.

Простой скрипт для тестирования Excel файлов.
"""

import sys
from pathlib import Path

# Добавляем путь
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from services.document_search.excel_parser import ExcelParser
    from services.document_search.file_format_detector import detect_file_format
    print("✅ Импорты успешны")
except Exception as e:
    print(f"❌ Ошибка импорта: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Путь к папке
folder_path = r"C:\Users\wangr\YandexDisk\Обмен информацией\Отдел продаж\торги — копия\44fz_475781"
folder = Path(folder_path)

if not folder.exists():
    print(f"❌ Папка не существует: {folder_path}")
    sys.exit(1)

print(f"\n{'=' * 80}")
print(f"Папка: {folder_path}")
print(f"{'=' * 80}\n")

# Находим файлы
excel_files = sorted(list(folder.glob("*.xlsx")) + list(folder.glob("*.xls")))
print(f"Найдено файлов: {len(excel_files)}\n")

if not excel_files:
    print("⚠️ Файлы не найдены")
    sys.exit(0)

# Тестируем
parser = ExcelParser()
success = 0
failed = 0

for file_path in excel_files:
    print(f"\n{'-' * 80}")
    print(f"Файл: {file_path.name}")
    size_mb = file_path.stat().st_size / 1024 / 1024
    print(f"Размер: {size_mb:.2f} МБ")
    
    # Формат
    fmt = detect_file_format(file_path)
    print(f"Формат: {fmt}")
    
    # Открываем
    try:
        cells = 0
        sheets = set()
        print("Открываем...")
        for cell in parser.iter_workbook_cells(file_path):
            cells += 1
            sheets.add(cell.get("sheet_name", "?"))
            if cells >= 50:  # Быстрый тест
                break
        print(f"✅ УСПЕХ: {cells} ячеек, {len(sheets)} листов")
        success += 1
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        failed += 1

print(f"\n{'=' * 80}")
print(f"ИТОГО: Успешно {success}, Ошибок {failed}, Всего {len(excel_files)}")
print(f"{'=' * 80}")

