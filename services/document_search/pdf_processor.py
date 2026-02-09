"""
MODULE: services.document_search.pdf_processor
RESPONSIBILITY: Handle PDF text extraction (text-based and scanned/OCR).
ALLOWED: PyPDF2, pdfplumber, pytesseract, pdf2image, logging, file_lock_handler, core.exceptions, sys, warnings, gc, contextlib.
FORBIDDEN: Direct database access.
ERRORS: DocumentSearchError.

Модуль для обработки PDF файлов.

Поддерживает:
- Обычные PDF (текстовые) - извлечение текста напрямую
- Отсканированные PDF - извлечение текста через OCR
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Iterable
import io
import os
import warnings
import sys
import gc
from contextlib import contextmanager

from loguru import logger

from core.exceptions import DocumentSearchError
from services.document_search.file_lock_handler import handle_file_lock

# Подавляем предупреждения от PDF библиотек о некорректных шрифтах и цветах
warnings.filterwarnings('ignore', message='.*FontBBox.*')
warnings.filterwarnings('ignore', message='.*invalid float value.*')
warnings.filterwarnings('ignore', message='.*Cannot set gray.*')
warnings.filterwarnings('ignore', category=UserWarning, module='pdfplumber')
warnings.filterwarnings('ignore', category=UserWarning, module='PyPDF2')


@contextmanager
def suppress_pdf_warnings():
    """Контекстный менеджер для подавления предупреждений от PDF библиотек."""
    # Подавляем warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        # Перенаправляем stderr для подавления прямого вывода предупреждений
        original_stderr = sys.stderr
        try:
            # Создаем "тихий" stderr, который игнорирует предупреждения PDF
            class PDFWarningFilter:
                def __init__(self, original):
                    self.original = original
                    self.buffer = []
                
                def write(self, text):
                    # Фильтруем известные предупреждения PDF
                    if any(keyword in text for keyword in [
                        'FontBBox', 'invalid float value', 'Cannot set gray',
                        'P37', 'P15', 'P26', 'P30', 'P109', 'P110', 'P136', 'P144'
                    ]):
                        return  # Игнорируем эти предупреждения
                    self.original.write(text)
                
                def flush(self):
                    self.original.flush()
                
                def __getattr__(self, name):
                    return getattr(self.original, name)
            
            sys.stderr = PDFWarningFilter(original_stderr)
            yield
        finally:
            sys.stderr = original_stderr


class PDFProcessor:
    """Класс для обработки PDF файлов."""
    
    def __init__(self):
        """Инициализация процессора PDF."""
        self._check_dependencies()
    
    def _check_dependencies(self) -> None:
        """Проверка наличия необходимых библиотек."""
        try:
            import PyPDF2
            self._pypdf2_available = True
        except ImportError:
            self._pypdf2_available = False
            logger.debug("PyPDF2 не установлен, обработка обычных PDF будет недоступна")
        
        try:
            import pdfplumber
            self._pdfplumber_available = True
        except ImportError:
            self._pdfplumber_available = False
            logger.debug("pdfplumber не установлен, обработка обычных PDF будет недоступна")
        
        try:
            import pytesseract
            from PIL import Image
            import pdf2image
            self._ocr_available = True
            
            # Проверяем и настраиваем путь к Tesseract для Windows
            self._configure_tesseract_path()
            
        except ImportError:
            self._ocr_available = False
            logger.debug("OCR библиотеки не установлены (pytesseract, PIL, pdf2image), обработка отсканированных PDF будет недоступна")
    
    def _configure_tesseract_path(self) -> None:
        """Настройка пути к Tesseract для Windows."""
        import platform
        
        if platform.system() == 'Windows':
            try:
                import pytesseract
                
                # Стандартные пути установки Tesseract в Windows
                possible_paths = [
                    r"C:\Program Files\Tesseract-OCR\tesseract.exe",
                    r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
                    r"C:\Users\{}\AppData\Local\Programs\Tesseract-OCR\tesseract.exe".format(
                        os.environ.get('USERNAME', '')
                    ),
                ]
                
                # Проверяем, установлен ли путь в переменных окружения
                tesseract_cmd = os.environ.get('TESSDATA_PREFIX') or os.environ.get('TESSERACT_CMD')
                
                if tesseract_cmd:
                    possible_paths.insert(0, tesseract_cmd)
                
                # Ищем Tesseract в стандартных путях
                for path in possible_paths:
                    if os.path.exists(path):
                        pytesseract.pytesseract.tesseract_cmd = path
                        logger.debug(f"Найден Tesseract: {path}")
                        return
                
                # Пробуем найти через where/which команду
                try:
                    import shutil
                    tesseract_path = shutil.which('tesseract')
                    if tesseract_path:
                        pytesseract.pytesseract.tesseract_cmd = tesseract_path
                        logger.debug(f"Найден Tesseract через PATH: {tesseract_path}")
                        return
                except Exception:
                    pass
                
                # Если не найден - пробуем использовать по умолчанию
                logger.debug(
                    "Tesseract не найден в стандартных путях. "
                    "Установите Tesseract или укажите путь через переменную окружения TESSERACT_CMD"
                )
                
            except Exception as error:
                logger.debug(f"Ошибка при настройке пути к Tesseract: {error}")
    
    def is_pdf_file(self, file_path: Path) -> bool:
        """Проверка, является ли файл PDF."""
        if not file_path.exists():
            return False
        return file_path.suffix.lower() == ".pdf"
    
    def extract_text_from_pdf(self, file_path: Path) -> str:
        """
        Извлечение текста из обычного PDF файла.
        
        Пробует несколько методов:
        1. pdfplumber (лучшее качество)
        2. PyPDF2 (резервный)
        
        Args:
            file_path: Путь к PDF файлу
            
        Returns:
            Извлеченный текст
            
        Raises:
            DocumentSearchError: Если не удалось извлечь текст
        """
        if not self.is_pdf_file(file_path):
            raise DocumentSearchError(f"Файл {file_path} не является PDF")
        
        # Пробуем pdfplumber (лучшее качество)
        if self._pdfplumber_available:
            try:
                import pdfplumber
                text_parts = []
                # Подавляем предупреждения при открытии PDF
                with suppress_pdf_warnings():
                    # Используем handle_file_lock для обработки блокировок
                    pdf = handle_file_lock(file_path, lambda: pdfplumber.open(file_path))
                try:
                    for page_num, page in enumerate(pdf.pages, 1):
                        try:
                            page_text = page.extract_text()
                            if page_text:
                                text_parts.append(page_text)
                        except Exception as page_error:
                            logger.debug(f"Ошибка при извлечении текста со страницы {page_num} в {file_path.name}: {page_error}")
                            continue
                    
                    if text_parts:
                        full_text = "\n".join(text_parts)
                        logger.info(f"Извлечен текст из PDF {file_path.name} через pdfplumber: {len(full_text)} символов")
                        return full_text
                finally:
                    if hasattr(pdf, 'close'):
                        pdf.close()
            except Exception as error:
                logger.debug(f"pdfplumber не смог обработать {file_path.name}: {error}, пробуем PyPDF2")
        
        # Пробуем PyPDF2 (резервный)
        if self._pypdf2_available:
            try:
                import PyPDF2
                text_parts = []
                # Подавляем предупреждения при открытии PDF
                with suppress_pdf_warnings():
                    # Используем handle_file_lock для обработки блокировок
                    def open_pdf():
                        file = open(file_path, 'rb')
                        return PyPDF2.PdfReader(file), file
                    
                    pdf_reader, file = handle_file_lock(file_path, open_pdf)
                try:
                    for page_num, page in enumerate(pdf_reader.pages, 1):
                        try:
                            page_text = page.extract_text()
                            if page_text:
                                text_parts.append(page_text)
                        except Exception as page_error:
                            logger.debug(f"Ошибка при извлечении текста со страницы {page_num} в {file_path.name}: {page_error}")
                            continue
                    
                    if text_parts:
                        full_text = "\n".join(text_parts)
                        logger.info(f"Извлечен текст из PDF {file_path.name} через PyPDF2: {len(full_text)} символов")
                        return full_text
                finally:
                    if file:
                        file.close()
            except Exception as error:
                logger.debug(f"PyPDF2 не смог обработать {file_path.name}: {error}")
        
        raise DocumentSearchError(f"Не удалось извлечь текст из PDF {file_path.name}")
    
    def extract_text_from_scanned_pdf(self, file_path: Path) -> str:
        """
        Извлечение текста из отсканированного PDF через OCR.
        
        Args:
            file_path: Путь к PDF файлу
            
        Returns:
            Извлеченный текст через OCR
            
        Raises:
            DocumentSearchError: Если OCR недоступен или не удалось извлечь текст
        """
        if not self._ocr_available:
            raise DocumentSearchError(
                "OCR библиотеки не установлены. Установите: pytesseract, Pillow, pdf2image"
            )
        
        if not self.is_pdf_file(file_path):
            raise DocumentSearchError(f"Файл {file_path} не является PDF")
        
        try:
            import pytesseract
            from PIL import Image
            import pdf2image
            import PyPDF2
            
            logger.info(f"Начинаем OCR обработку PDF {file_path.name}...")
            
            # Получаем количество страниц для постраничной обработки
            total_pages = None
            try:
                with open(str(file_path), 'rb') as pdf_file:
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    total_pages = len(pdf_reader.pages)
                    logger.debug(f"PDF {file_path.name} содержит {total_pages} страниц")
            except Exception as page_count_error:
                logger.warning(f"Не удалось определить количество страниц через PyPDF2: {page_count_error}, будем обрабатывать до ошибки")
                # Если не удалось определить количество страниц, будем обрабатывать по одной до ошибки
            
            if total_pages is not None and total_pages == 0:
                raise DocumentSearchError(f"PDF {file_path.name} не содержит страниц")
            
            # Обрабатываем страницы по одной, чтобы не загружать все в память
            text_parts = []
            page_num = 1
            max_pages = total_pages if total_pages is not None else 10000  # Ограничение на случай бесконечного цикла
            
            while page_num <= max_pages:
                try:
                    # Конвертируем только одну страницу за раз
                    page_images = pdf2image.convert_from_path(
                        str(file_path),
                        first_page=page_num,
                        last_page=page_num,
                        dpi=300  # Можно настроить DPI для баланса качества/скорости
                    )
                    
                    if not page_images:
                        # Если количество страниц неизвестно, это может означать конец файла
                        if total_pages is None:
                            logger.debug(f"Страница {page_num} не найдена, завершаем обработку")
                            break
                        else:
                            logger.warning(f"Страница {page_num} не была конвертирована в изображение")
                            page_num += 1
                            continue
                    
                    # Обрабатываем страницу через OCR
                    image = page_images[0]
                    try:
                        page_text = pytesseract.image_to_string(image, lang='rus+eng')
                        if page_text.strip():
                            text_parts.append(page_text)
                            page_info = f"{page_num}/{total_pages}" if total_pages else f"{page_num}"
                            logger.debug(f"OCR страница {page_info}: извлечено {len(page_text)} символов")
                    except Exception as ocr_error:
                        logger.warning(f"Ошибка OCR на странице {page_num} в {file_path.name}: {ocr_error}")
                    
                    # Освобождаем память после обработки страницы
                    del image
                    del page_images
                    gc.collect()
                    
                    page_num += 1
                    
                except Exception as page_error:
                    # Если количество страниц неизвестно и получили ошибку - это может быть конец файла
                    if total_pages is None:
                        logger.debug(f"Ошибка при обработке страницы {page_num}, вероятно конец файла: {page_error}")
                        break
                    else:
                        logger.warning(f"Ошибка при обработке страницы {page_num} в {file_path.name}: {page_error}")
                        page_num += 1
                        continue
            
            if not text_parts:
                raise DocumentSearchError(f"OCR не смог извлечь текст из PDF {file_path.name}")
            
            processed_pages = len(text_parts)
            full_text = "\n".join(text_parts)
            pages_info = f"{processed_pages} страниц" if total_pages is None else f"{processed_pages}/{total_pages} страниц"
            logger.info(f"OCR завершен для PDF {file_path.name}: {len(full_text)} символов из {pages_info}")
            return full_text
            
        except ImportError as import_error:
            raise DocumentSearchError(f"OCR библиотеки не установлены: {import_error}")
        except Exception as error:
            raise DocumentSearchError(f"Ошибка при OCR обработке PDF {file_path.name}: {error}")
    
    def detect_pdf_type(self, file_path: Path) -> str:
        """
        Определение типа PDF (обычный или отсканированный).
        
        Args:
            file_path: Путь к PDF файлу
            
        Returns:
            'text' - обычный PDF с текстом
            'scanned' - отсканированный PDF (требует OCR)
        """
        if not self.is_pdf_file(file_path):
            return 'unknown'
        
        # Пробуем извлечь текст обычным способом
        try:
            text = self.extract_text_from_pdf(file_path)
            # Если извлечено достаточно текста - это обычный PDF
            if text and len(text.strip()) > 50:
                return 'text'
        except Exception:
            pass
        
        # Если не удалось извлечь текст - вероятно отсканированный
        return 'scanned'
    
    def extract_text(self, file_path: Path, force_ocr: bool = False) -> str:
        """
        Универсальный метод извлечения текста из PDF.
        
        Автоматически определяет тип PDF и использует соответствующий метод.
        
        Args:
            file_path: Путь к PDF файлу
            force_ocr: Принудительно использовать OCR даже для обычных PDF
            
        Returns:
            Извлеченный текст
        """
        if force_ocr:
            return self.extract_text_from_scanned_pdf(file_path)
        
        # Пробуем сначала обычный метод
        try:
            return self.extract_text_from_pdf(file_path)
        except DocumentSearchError:
            # Если не получилось - пробуем OCR
            logger.info(f"Обычный метод не сработал для {file_path.name}, пробуем OCR...")
            return self.extract_text_from_scanned_pdf(file_path)
    
    def iter_pdf_cells(self, file_path: Path) -> Iterable[Dict[str, Any]]:
        """
        Итератор по тексту PDF, имитирующий структуру Excel ячеек.
        
        Разбивает текст на "ячейки" (слова/фразы) для совместимости с поиском.
        
        Args:
            file_path: Путь к PDF файлу
            
        Yields:
            Dict с полями:
            - text: извлеченный текст
            - display_text: текст для отображения
            - sheet_name: имя "листа" (используем имя файла)
            - row: номер строки
            - column: номер колонки
            - cell_address: адрес ячейки
        """
        try:
            full_text = self.extract_text(file_path)
        except DocumentSearchError as error:
            logger.error(f"Не удалось извлечь текст из PDF {file_path}: {error}")
            return
        
        # Разбиваем текст на строки и слова для поиска
        lines = full_text.split('\n')
        sheet_name = file_path.stem
        
        for row_num, line in enumerate(lines, 1):
            if not line.strip():
                continue
            
            # Разбиваем строку на слова/фразы
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

