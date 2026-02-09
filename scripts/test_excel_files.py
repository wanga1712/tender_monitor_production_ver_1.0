"""
MODULE: scripts.test_excel_files
RESPONSIBILITY: Testing opening of Excel files in a specified folder.
ALLOWED: sys, pathlib, services.document_search.excel_parser, services.document_search.file_format_detector, traceback.
FORBIDDEN: None.
ERRORS: None.

Скрипт для тестирования открытия Excel файлов в конкретной папке.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.document_search.excel_parser import ExcelParser
from services.document_search.file_format_detector import detect_file_format


def test_files_in_folder(folder_path: str):
    """Тестирует открытие всех Excel файлов в указанной папке."""
    folder = Path(folder_path)
    if not folder.exists():
        print(f"❌ Папка не существует: {folder_path}")
        return
    
    print(f"\n{'=' * 80}")
    print(f"Тестирование файлов в папке: {folder_path}")
    print(f"{'=' * 80}\n")
    
    excel_files = sorted(list(folder.glob("*.xlsx")) + list(folder.glob("*.xls")))
    
    if not excel_files:
        print("⚠️ Excel файлы не найдены в папке")
        return
    
    print(f"Найдено файлов: {len(excel_files)}\n")
    
    parser = ExcelParser()
    success_count = 0
    failed_count = 0
    
    for file_path in excel_files:
        print(f"\n{'-' * 80}")
        print(f"Файл: {file_path.name}")
        print(f"Размер: {file_path.stat().st_size / 1024 / 1024:.2f} МБ")
        
        # Определяем формат
        detected_format = detect_file_format(file_path)
        print(f"Определенный формат: {detected_format}")
        
        # Пробуем открыть и посчитать ячейки
        try:
            cell_count = 0
            sheet_count = 0
            sheets = set()
            
            print("Попытка открыть файл...")
            for cell in parser.iter_workbook_cells(file_path):
                cell_count += 1
                sheets.add(cell.get("sheet_name", "Unknown"))
                if cell_count >= 100:  # Ограничиваем для скорости теста
                    break
            
            sheet_count = len(sheets)
            print(f"✅ УСПЕХ: Открыт, найдено ячеек: {cell_count}, листов: {sheet_count}")
            if cell_count >= 100:
                print("   (показаны первые 100 ячеек для теста)")
            success_count += 1
            
        except Exception as error:
            print(f"❌ ОШИБКА: {error}")
            import traceback
            traceback.print_exc()
            failed_count += 1
    
    print(f"\n{'=' * 80}")
    print(f"ИТОГО: Успешно: {success_count}, Ошибок: {failed_count}, Всего: {len(excel_files)}")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    # Путь к папке с файлами
    test_folder = r"C:\Users\wangr\YandexDisk\Обмен информацией\Отдел продаж\торги — копия\44fz_475781"
    
    if len(sys.argv) > 1:
        test_folder = sys.argv[1]
    
    test_files_in_folder(test_folder)
