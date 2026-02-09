# -*- coding: utf-8 -*-
"""
MODULE: scripts.test_excel_direct
RESPONSIBILITY: Direct testing of Excel parsing on a specific folder.
ALLOWED: sys, os, pathlib, services.document_search.excel_parser, services.document_search.file_format_detector, traceback.
FORBIDDEN: None.
ERRORS: None.
"""
import sys
import os
from pathlib import Path

# Добавляем путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Принудительно выводим в консоль
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

print("=" * 80, flush=True)
print("ТЕСТИРОВАНИЕ EXCEL ФАЙЛОВ", flush=True)
print("=" * 80, flush=True)

try:
    print("Импорт модулей...", flush=True)
    from services.document_search.excel_parser import ExcelParser
    from services.document_search.file_format_detector import detect_file_format
    print("✅ Импорты успешны\n", flush=True)
except Exception as e:
    print(f"❌ Ошибка импорта: {e}", flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Путь к папке
folder_path = r"C:\Users\wangr\YandexDisk\Обмен информацией\Отдел продаж\торги — копия\44fz_475781"
folder = Path(folder_path)

print(f"Проверка папки: {folder_path}", flush=True)
if not folder.exists():
    print(f"❌ Папка не существует!", flush=True)
    sys.exit(1)

print(f"✅ Папка существует\n", flush=True)

# Находим файлы
excel_files = sorted(list(folder.glob("*.xlsx")) + list(folder.glob("*.xls")))
print(f"Найдено файлов: {len(excel_files)}\n", flush=True)

if not excel_files:
    print("⚠️ Файлы не найдены", flush=True)
    sys.exit(0)

# Показываем список файлов
for i, f in enumerate(excel_files, 1):
    size_mb = f.stat().st_size / 1024 / 1024
    print(f"{i}. {f.name} ({size_mb:.2f} МБ)", flush=True)

print("\n" + "=" * 80, flush=True)
print("НАЧИНАЕМ ТЕСТИРОВАНИЕ\n", flush=True)

# Тестируем
parser = ExcelParser()
success = 0
failed = 0

for i, file_path in enumerate(excel_files, 1):
    print(f"\n[{i}/{len(excel_files)}] {file_path.name}", flush=True)
    print("-" * 80, flush=True)
    
    size_mb = file_path.stat().st_size / 1024 / 1024
    print(f"Размер: {size_mb:.2f} МБ", flush=True)
    
    # Определяем формат
    fmt = detect_file_format(file_path)
    print(f"Определенный формат: {fmt}", flush=True)
    
    # Пробуем открыть
    try:
        print("Попытка открыть файл...", flush=True)
        cells = 0
        sheets = set()
        
        for cell in parser.iter_workbook_cells(file_path):
            cells += 1
            sheets.add(cell.get("sheet_name", "?"))
            if cells >= 50:  # Быстрый тест
                break
        
        print(f"✅ УСПЕХ: Открыт успешно!", flush=True)
        print(f"   Найдено ячеек: {cells} (показаны первые 50)", flush=True)
        print(f"   Листов: {len(sheets)}", flush=True)
        if sheets:
            print(f"   Имена листов: {', '.join(list(sheets)[:5])}", flush=True)
        success += 1
        
    except Exception as e:
        print(f"❌ ОШИБКА: {e}", flush=True)
        import traceback
        print("Трассировка:", flush=True)
        traceback.print_exc()
        failed += 1

print(f"\n{'=' * 80}", flush=True)
print(f"ИТОГОВЫЙ РЕЗУЛЬТАТ:", flush=True)
print(f"  Успешно: {success}", flush=True)
print(f"  Ошибок: {failed}", flush=True)
print(f"  Всего: {len(excel_files)}", flush=True)
print(f"{'=' * 80}\n", flush=True)

