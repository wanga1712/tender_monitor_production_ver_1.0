"""
MODULE: services.document_search.excel_file_tester
RESPONSIBILITY: Verify if an Excel file is readable/valid before processing.
ALLOWED: logging, multiprocessing, ExcelParser, file_format_detector.
FORBIDDEN: Database access.
ERRORS: None.

Вспомогательный модуль для предварительной проверки Excel файлов.

ExcelFileTester пытается открыть документ тем же ExcelParser,
что и основной поиск, чтобы заранее поймать проблемы с форматом.
Логика идентична тестовому скрипту: определяем формат,
читаем ограниченное число ячеек и логируем результат.
"""

from __future__ import annotations

from pathlib import Path
from typing import Set
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FutureTimeoutError
import multiprocessing

from loguru import logger

from services.document_search.excel_parser import ExcelParser
from services.document_search.file_format_detector import detect_file_format


class ExcelFileTester:
    """Проверка Excel файла перед анализом."""

    def __init__(self, max_cells: int = 50, verify_timeout: float = 10.0):
        """
        Инициализация тестера.
        
        Args:
            max_cells: Максимальное количество ячеек для чтения при проверке
            verify_timeout: Таймаут проверки файла в секундах (по умолчанию 10 сек)
        """
        self.max_cells = max_cells
        self.verify_timeout = verify_timeout
        self._parser = ExcelParser()

    @staticmethod
    def _verify_internal(file_path_str: str, max_cells: int) -> bool:
        """
        Внутренний статический метод проверки файла (выполняется в отдельном процессе).
        
        Args:
            file_path_str: Путь к файлу для проверки (строка, так как процессы не могут передавать Path)
            max_cells: Максимальное количество ячеек для чтения
        
        Returns:
            True если файл валиден, False в противном случае
        """
        file_path = Path(file_path_str)
        cells_read = 0
        sheets_seen: Set[str] = set()

        # Создаем новый парсер в процессе (нельзя использовать self._parser)
        parser = ExcelParser()

        try:
            for cell in parser.iter_workbook_cells(file_path):
                cells_read += 1
                sheet = cell.get("sheet_name", "unknown")
                sheets_seen.add(sheet)
                if cells_read >= max_cells:
                    break

            # Уменьшаем уровень логирования - слишком много шума
            # logger.debug(
            #     f"Файл {file_path.name} успешно прошел проверку: "
            #     f"прочитано {cells_read} ячеек, листов {len(sheets_seen)}"
            # )
            return True
        except Exception as error:
            logger.error(
                f"Файл {file_path.name} не прошел проверку ExcelParser: {error}"
            )
            return False

    def verify(self, file_path: Path) -> bool:
        """
        Быстрый прогон по файлу: читаем до max_cells ячеек и убеждаемся,
        что документ корректно открывается.
        
        Использует таймаут для предотвращения зависания на поврежденных файлах.
        
        Args:
            file_path: Путь к файлу для проверки
        
        Returns:
            True если файл валиден, False в противном случае
        """
        if not file_path.exists():
            logger.error(f"Файл {file_path} не существует, пропускаем проверку")
            return False

        detected_format = detect_file_format(file_path)
        size_mb = file_path.stat().st_size / (1024 * 1024)
        logger.debug(
            f"Проверка Excel файла {file_path.name}: размер {size_mb:.2f} МБ, "
            f"определенный формат: {detected_format}, таймаут: {self.verify_timeout} сек"
        )

        # Выполняем проверку в отдельном процессе с таймаутом
        # Используем ProcessPoolExecutor вместо ThreadPoolExecutor, 
        # так как процесс можно принудительно завершить, а поток - нет
        try:
            # Для Windows нужно использовать 'spawn' метод
            if multiprocessing.get_start_method(allow_none=True) != 'spawn':
                try:
                    multiprocessing.set_start_method('spawn', force=True)
                except RuntimeError:
                    pass  # Уже установлен
            
            with ProcessPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._verify_internal, str(file_path), self.max_cells)
                result = future.result(timeout=self.verify_timeout)
                return result
        except FutureTimeoutError:
            logger.warning(
                f"⏱️ Таймаут проверки файла {file_path.name} "
                f"(превышено {self.verify_timeout} сек), считаем файл поврежденным"
            )
            # Принудительно завершаем процесс
            try:
                future.cancel()
            except:
                pass
            return False
        except Exception as error:
            logger.error(
                f"Ошибка при проверке файла {file_path.name}: {error}"
            )
            return False


