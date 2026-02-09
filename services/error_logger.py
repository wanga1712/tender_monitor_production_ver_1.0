"""
MODULE: services.error_logger
RESPONSIBILITY: Specialized logging for file processing errors.
ALLOWED: pathlib, datetime, loguru.
FORBIDDEN: Business logic, DB operations.
ERRORS: None.

Модуль для логирования ошибок обработки файлов в отдельный файл.

Сохраняет ошибки при:
- Распаковке архивов
- Открытии файлов
- Чтении файлов
- Сверке/поиске совпадений
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import Optional
import os
from loguru import logger


class ErrorLogger:
    """Класс для логирования ошибок обработки файлов в отдельный файл."""
    
    def __init__(self, log_file_path: Optional[Path] = None):
        """
        Инициализация логгера ошибок.
        
        Args:
            log_file_path: Путь к файлу для логирования. Если None, используется logs/errors.log
        """
        if log_file_path is None:
            # Создаем директорию logs, если её нет
            logs_dir = Path("logs")
            logs_dir.mkdir(exist_ok=True)
            log_file_path = logs_dir / "file_processing_errors.log"
        
        self.log_file_path = Path(log_file_path)
        self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Инициализируем файл с заголовком, если он новый
        if not self.log_file_path.exists():
            self._write_header()
    
    def _write_header(self) -> None:
        """Записывает заголовок в файл логов."""
        header = f"""
{'='*80}
Лог ошибок обработки файлов
Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*80}

"""
        with open(self.log_file_path, 'w', encoding='utf-8') as f:
            f.write(header)
    
    def log_error(
        self,
        error_type: str,
        file_path: Path,
        error_message: str,
        context: Optional[dict] = None,
    ) -> None:
        """
        Логирует ошибку в файл.
        
        Args:
            error_type: Тип ошибки (например, 'распаковка', 'открытие файла', 'чтение файла', 'сверка')
            file_path: Путь к файлу, с которым произошла ошибка
            error_message: Сообщение об ошибке
            context: Дополнительный контекст (словарь с дополнительной информацией)
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        log_entry = f"\n[{timestamp}] {error_type.upper()}\n"
        log_entry += f"Файл: {file_path}\n"
        log_entry += f"Ошибка: {error_message}\n"
        
        if context:
            log_entry += "Контекст:\n"
            for key, value in context.items():
                log_entry += f"  {key}: {value}\n"
        
        log_entry += "-" * 80 + "\n"
        
        try:
            with open(self.log_file_path, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        except Exception as e:
            logger.error(f"Не удалось записать ошибку в файл {self.log_file_path}: {e}")
    
    def log_extraction_error(
        self,
        archive_path: Path,
        error_message: str,
        archive_type: Optional[str] = None,
    ) -> None:
        """Логирует ошибку распаковки архива."""
        context = {}
        if archive_type:
            context['тип_архива'] = archive_type
        
        self.log_error(
            error_type="распаковка архива",
            file_path=archive_path,
            error_message=error_message,
            context=context if context else None,
        )
    
    def log_file_open_error(
        self,
        file_path: Path,
        error_message: str,
        file_type: Optional[str] = None,
        detected_format: Optional[str] = None,
    ) -> None:
        """Логирует ошибку открытия файла."""
        context = {}
        if file_type:
            context['тип_файла'] = file_type
        if detected_format:
            context['определенный_формат'] = detected_format
        
        self.log_error(
            error_type="открытие файла",
            file_path=file_path,
            error_message=error_message,
            context=context if context else None,
        )
    
    def log_file_read_error(
        self,
        file_path: Path,
        error_message: str,
        sheet_name: Optional[str] = None,
        row_number: Optional[int] = None,
    ) -> None:
        """Логирует ошибку чтения файла."""
        context = {}
        if sheet_name:
            context['лист'] = sheet_name
        if row_number:
            context['строка'] = row_number
        
        self.log_error(
            error_type="чтение файла",
            file_path=file_path,
            error_message=error_message,
            context=context if context else None,
        )
    
    def log_search_error(
        self,
        file_path: Path,
        error_message: str,
        file_size_mb: Optional[float] = None,
        processing_time: Optional[float] = None,
    ) -> None:
        """Логирует ошибку при поиске/сверке в файле."""
        context = {}
        if file_size_mb is not None:
            context['размер_МБ'] = f"{file_size_mb:.2f}"
        if processing_time is not None:
            context['время_обработки_сек'] = f"{processing_time:.1f}"
        
        self.log_error(
            error_type="поиск/сверка",
            file_path=file_path,
            error_message=error_message,
            context=context if context else None,
        )


# Глобальный экземпляр логгера ошибок
_error_logger_instance: Optional[ErrorLogger] = None


def get_error_logger() -> ErrorLogger:
    """Возвращает глобальный экземпляр логгера ошибок."""
    global _error_logger_instance
    if _error_logger_instance is None:
        _error_logger_instance = ErrorLogger()
    return _error_logger_instance


def set_error_logger(logger_instance: ErrorLogger) -> None:
    """Устанавливает глобальный экземпляр логгера ошибок."""
    global _error_logger_instance
    _error_logger_instance = logger_instance

