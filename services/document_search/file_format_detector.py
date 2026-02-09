"""
MODULE: services.document_search.file_format_detector
RESPONSIBILITY: Detect file format by magic bytes.
ALLOWED: logging.
FORBIDDEN: Full file parsing (should be fast).
ERRORS: None.

Определение формата файла по magic bytes.
"""

from __future__ import annotations

from pathlib import Path
from loguru import logger


def detect_file_format(file_path: Path) -> str:
    """
    Определяет реальный формат файла по magic bytes.
    
    Returns:
        'xlsx', 'xls', 'csv', 'html', 'xml', 'unknown', или 'corrupted'
    """
    try:
        with open(file_path, 'rb') as f:
            header = f.read(16)  # Читаем больше байтов для лучшего определения
            
        # XLSX файлы начинаются с PK (ZIP signature)
        if header[:2] == b'PK':
            return 'xlsx'
        
        # XLS файлы (старый формат) начинаются с определенных байтов
        # OLE2 format: D0 CF 11 E0 A1 B1 1A E1
        if header[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
            return 'xls'
        
        # CSV файлы могут начинаться с BOM или обычного текста
        # UTF-8 BOM: EF BB BF
        # UTF-16 BOM: FF FE или FE FF
        if header[:3] == b'\xef\xbb\xbf' or header[:2] in (b'\xff\xfe', b'\xfe\xff'):
            # Проверяем, что это текст
            try:
                with open(file_path, 'r', encoding='utf-8-sig') as f:
                    first_line = f.readline()
                    if ',' in first_line or ';' in first_line:
                        return 'csv'
            except:
                pass
        
        # HTML файлы начинаются с <!DOCTYPE или <html
        header_text = header[:100].decode('utf-8', errors='ignore').strip()
        if header_text.startswith('<!DOCTYPE') or header_text.startswith('<html'):
            return 'html'
        
        # XML файлы начинаются с <?xml
        if header_text.startswith('<?xml'):
            return 'xml'
        
        # Попробуем определить по расширению и содержимому
        # Если файл имеет расширение .xlsx, но не ZIP, возможно это бинарный формат
        if file_path.suffix.lower() == '.xlsx':
            # Может быть это старый формат .xls с неправильным расширением
            # Проверяем, не является ли это OLE2 форматом (.xls)
            if header[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
                return 'xls'  # Это на самом деле .xls файл с неправильным расширением
            # Если не ZIP и не OLE2, но файл существует - пробуем определить по другим признакам
            # Многие старые .xls файлы могут иметь другие magic bytes
            # Проверяем, не является ли это бинарным файлом (не текстовым)
            if len(header) >= 2 and header[:2] not in (b'PK', b'\xd0\xcf'):
                # Возможно это старый .xls с другим форматом или поврежденный файл
                # Но скорее всего это .xls с неправильным расширением
                return 'xls'  # Пробуем как .xls
            # Или поврежденный файл
            return 'unknown_xlsx'
        
        if file_path.suffix.lower() == '.xls':
            # Может быть это новый формат .xlsx с неправильным расширением
            if header[:2] == b'PK':
                return 'xlsx'
            return 'unknown_xls'
        
        return 'unknown'
    except Exception as e:
        logger.debug(f"Не удалось определить формат файла {file_path}: {e}")
        return 'corrupted'

