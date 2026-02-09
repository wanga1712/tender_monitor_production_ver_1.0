"""
MODULE: services.document_search.document_parser
RESPONSIBILITY: Unified interface for parsing different document types (Excel, Word, PDF).
ALLOWED: ExcelParser, WordProcessor, PDFProcessor, logging.
FORBIDDEN: Direct file parsing logic (delegate to specific parsers), network access.
ERRORS: DocumentSearchError.

Универсальный парсер для всех типов документов.

Поддерживает:
- Excel файлы (.xlsx, .xls) - через ExcelParser
- Word документы (.docx, .doc) - через WordProcessor
- PDF файлы (обычные и отсканированные) - через PDFProcessor

Автоматически определяет тип файла и использует соответствующий парсер.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from loguru import logger

from core.exceptions import DocumentSearchError
from services.document_search.excel_parser import ExcelParser
from services.document_search.word_processor import WordProcessor
from services.document_search.pdf_processor import PDFProcessor


class DocumentParser:
    """
    Универсальный парсер для всех типов документов.
    
    Автоматически определяет тип документа и использует соответствующий парсер.
    """
    
    def __init__(self):
        """Инициализация парсера документов."""
        self.excel_parser = ExcelParser()
        self.word_processor = WordProcessor()
        self.pdf_processor = PDFProcessor()
        logger.debug("DocumentParser инициализирован")
    
    def detect_document_type(self, file_path: Path) -> str:
        """
        Определение типа документа по расширению и содержимому.
        
        Args:
            file_path: Путь к файлу
            
        Returns:
            Тип документа: 'excel', 'word', 'pdf', 'unknown'
        """
        if not file_path.exists():
            raise DocumentSearchError(f"Файл не существует: {file_path}")
        
        suffix = file_path.suffix.lower()
        
        # Определяем по расширению
        if suffix in ('.xlsx', '.xls'):
            return 'excel'
        elif suffix in ('.docx', '.doc'):
            return 'word'
        elif suffix == '.pdf':
            return 'pdf'
        else:
            # Пробуем определить по magic bytes
            try:
                with open(file_path, 'rb') as f:
                    header = f.read(8)
                
                # Excel XLSX (ZIP signature)
                if header[:2] == b'PK':
                    # Может быть как Excel, так и Word (оба используют ZIP)
                    # Проверяем расширение
                    if suffix in ('.xlsx', '.xls'):
                        return 'excel'
                    elif suffix in ('.docx', '.doc'):
                        return 'word'
                    else:
                        # По magic bytes - проверяем внутреннюю структуру
                        # Для Word .docx должен быть файл [Content_Types].xml
                        # Для Excel .xlsx должен быть файл workbook.xml
                        # Но это требует более глубокой проверки
                        logger.warning(f"Не удалось точно определить тип файла {file_path.name}, предполагаем по расширению")
                        return 'unknown'
                
                # Excel XLS (OLE2)
                if header[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
                    if suffix in ('.xls', '.xlsx'):
                        return 'excel'
                    elif suffix in ('.doc', '.docx'):
                        return 'word'
                    else:
                        return 'unknown'
                
                # PDF
                if header[:4] == b'%PDF':
                    return 'pdf'
                
            except Exception as error:
                logger.debug(f"Ошибка при определении типа файла {file_path.name}: {error}")
            
            return 'unknown'
    
    def extract_text(self, file_path: Path) -> str:
        """
        Извлечение текста из документа любого типа.
        
        Args:
            file_path: Путь к файлу
            
        Returns:
            Извлеченный текст
            
        Raises:
            DocumentSearchError: Если не удалось извлечь текст
        """
        doc_type = self.detect_document_type(file_path)
        
        try:
            if doc_type == 'excel':
                # Для Excel извлекаем все ячейки и формируем текст
                text_parts = []
                for cell_info in self.excel_parser.iter_workbook_cells(file_path):
                    # Excel парсер возвращает 'display_text' (отформатированное значение) и 'text' (нормализованный)
                    # Используем display_text для лучшей читаемости
                    cell_text = cell_info.get('display_text') or cell_info.get('text') or ''
                    if cell_text and str(cell_text).strip():
                        text_parts.append(str(cell_text).strip())
                return "\n".join(text_parts) if text_parts else ""
            
            elif doc_type == 'word':
                return self.word_processor.extract_text(file_path)
            
            elif doc_type == 'pdf':
                return self.pdf_processor.extract_text(file_path)
            
            else:
                raise DocumentSearchError(
                    f"Неподдерживаемый тип документа: {doc_type}. "
                    f"Поддерживаются: Excel (.xlsx, .xls), Word (.docx, .doc), PDF (.pdf)"
                )
                
        except DocumentSearchError:
            raise
        except Exception as error:
            raise DocumentSearchError(
                f"Ошибка при извлечении текста из {file_path.name}: {error}"
            )
    
    def iter_document_cells(
        self, 
        file_path: Path,
        force_ocr: bool = False
    ) -> Iterable[Dict[str, Any]]:
        """
        Универсальный итератор по содержимому документа.
        
        Возвращает структуру, аналогичную Excel ячейкам, для совместимости с поиском.
        
        Args:
            file_path: Путь к файлу
            force_ocr: Принудительно использовать OCR для PDF (если доступен)
            
        Yields:
            Dict с полями:
            - text: текст для поиска
            - display_text: текст для отображения
            - sheet_name: имя листа/страницы
            - row: номер строки
            - column: номер колонки
            - cell_address: адрес ячейки
        """
        doc_type = self.detect_document_type(file_path)
        
        try:
            if doc_type == 'excel':
                # Excel уже возвращает структуру ячеек
                # Парсер возвращает 'text' (нормализованный) и 'display_text' (отформатированный)
                for cell_info in self.excel_parser.iter_workbook_cells(file_path):
                    text_value = cell_info.get('text', '')
                    display_value = cell_info.get('display_text', '') or text_value
                    yield {
                        "text": text_value.lower() if text_value else '',
                        "display_text": display_value,
                        "sheet_name": cell_info.get('sheet_name', ''),
                        "row": cell_info.get('row', 0),
                        "column": cell_info.get('column', ''),
                        "cell_address": cell_info.get('cell_address', ''),
                    }
            
            elif doc_type == 'word':
                # Word использует аналогичную структуру
                yield from self.word_processor.iter_word_cells(file_path)
            
            elif doc_type == 'pdf':
                # PDF использует аналогичную структуру
                # force_ocr передается в extract_text через PDFProcessor
                if force_ocr:
                    # Принудительно используем OCR
                    text = self.pdf_processor.extract_text(file_path, force_ocr=True)
                else:
                    text = self.pdf_processor.extract_text(file_path)
                
                # Формируем структуру ячеек из текста
                lines = text.split('\n')
                sheet_name = file_path.stem
                
                for row_num, line in enumerate(lines, 1):
                    if not line.strip():
                        continue
                    
                    words = line.split()
                    for col_num, word in enumerate(words, 1):
                        yield {
                            "text": word.lower(),
                            "display_text": word,
                            "sheet_name": sheet_name,
                            "row": row_num,
                            "column": col_num,
                            "cell_address": f"{row_num}:{col_num}",
                        }
            
            else:
                raise DocumentSearchError(
                    f"Неподдерживаемый тип документа: {doc_type}. "
                    f"Поддерживаются: Excel (.xlsx, .xls), Word (.docx, .doc), PDF (.pdf)"
                )
                
        except DocumentSearchError:
            raise
        except Exception as error:
            logger.error(f"Ошибка при итерации по документу {file_path.name}: {error}")
            raise DocumentSearchError(
                f"Не удалось обработать документ {file_path.name}: {error}"
            )
    
    def parse_document(
        self,
        file_path: Path,
        force_ocr: bool = False
    ) -> Dict[str, Any]:
        """
        Полный парсинг документа с извлечением всей информации.
        
        Args:
            file_path: Путь к файлу
            force_ocr: Принудительно использовать OCR для PDF
            
        Returns:
            Dict с информацией о документе:
            - type: тип документа
            - text: извлеченный текст
            - cells: список ячеек (для Excel)
            - metadata: метаданные документа
        """
        doc_type = self.detect_document_type(file_path)
        
        # Проверяем неподдерживаемые типы ДО создания результата
        if doc_type == 'unknown':
            raise DocumentSearchError(
                f"Неподдерживаемый тип документа для файла {file_path.name}. "
                f"Поддерживаются: Excel (.xlsx, .xls), Word (.docx, .doc), PDF (.pdf)"
            )
        
        result = {
            "type": doc_type,
            "file_path": str(file_path),
            "file_name": file_path.name,
            "text": "",
            "cells": [],
            "metadata": {}
        }
        
        try:
            # Извлекаем текст
            result["text"] = self.extract_text(file_path)
            
            # Извлекаем структурированные данные
            cells = list(self.iter_document_cells(file_path, force_ocr=force_ocr))
            result["cells"] = cells
            
            # Метаданные
            result["metadata"] = {
                "size": file_path.stat().st_size,
                "suffix": file_path.suffix.lower(),
            }
            
            # Убираем избыточный INFO лог - результат вернется в return
            
        except DocumentSearchError:
            # Пробрасываем DocumentSearchError дальше
            raise
        except Exception as error:
            logger.error(f"Ошибка при парсинге документа {file_path.name}: {error}")
            result["error"] = str(error)
        
        return result

