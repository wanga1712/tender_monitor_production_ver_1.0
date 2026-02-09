"""
Сервис обработки ошибок и повторных попыток.
Отвечает за обработку исключений и управление повторными попытками.
"""

from typing import Callable, Any, Optional
import logging
import time

from services.archive_runner.error_handler import ErrorHandler

logger = logging.getLogger(__name__)


class ErrorHandlingService:
    """Сервис обработки ошибок и управления повторными попытками."""
    
    def __init__(self, max_retries: int = 3, retry_delay: float = 2.0):
        """
        Инициализация сервиса обработки ошибок.
        
        Args:
            max_retries: Максимальное количество повторных попыток
            retry_delay: Задержка между попытками в секундах
        """
        self.error_handler = ErrorHandler(max_retries=max_retries, retry_delay=retry_delay)
    
    def safe_call(self, func: Callable, *args, **kwargs) -> Optional[Any]:
        """
        Безопасный вызов функции с обработкой ошибок и повторными попытками.
        
        Args:
            func: Функция для вызова
            *args: Аргументы функции
            **kwargs: Ключевые аргументы функции
            
        Returns:
            Результат функции или None при ошибке
        """
        try:
            return self.error_handler.safe_call(func, *args, **kwargs)
        except Exception as e:
            logger.error(f"Критическая ошибка в safe_call: {e}")
            return None
    
    def with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        Вызов функции с повторными попытками при ошибках.
        
        Args:
            func: Функция для вызова
            *args: Аргументы функции
            **kwargs: Ключевые аргументы функции
            
        Returns:
            Результат успешного выполнения функции
            
        Raises:
            Exception: Если все попытки завершились ошибкой
        """
        return self.error_handler.with_retry(func, *args, **kwargs)
    
    def handle_database_error(self, error: Exception, operation: str) -> bool:
        """
        Обработка ошибок базы данных с возможностью повторного подключения.
        
        Args:
            error: Исключение базы данных
            operation: Название операции для логирования
            
        Returns:
            True если ошибка обработана и можно продолжить
        """
        try:
            # Логируем ошибку базы данных
            logger.error(f"Ошибка БД при {operation}: {error}")
            
            # Проверяем, является ли ошибка связанной с подключением
            error_msg = str(error).lower()
            connection_errors = [
                'connection', 'connect', 'socket', 'network', 
                'timeout', 'closed', 'reset', 'refused'
            ]
            
            if any(conn_error in error_msg for conn_error in connection_errors):
                logger.warning(f"Обнаружена ошибка подключения при {operation}")
                # Делаем паузу перед повторной попыткой
                time.sleep(self.error_handler.retry_delay)
                return True  # Указываем, что нужно повторить попытку
            
            return False  # Неизвестная ошибка, не повторяем
            
        except Exception as e:
            logger.error(f"Ошибка при обработке ошибки БД: {e}")
            return False
    
    def log_and_notify(self, error: Exception, context: str, level: str = 'error'):
        """
        Логирование ошибки и отправка уведомления при необходимости.
        
        Args:
            error: Исключение для логирования
            context: Контекст ошибки
            level: Уровень логирования ('error', 'warning', 'info')
        """
        try:
            log_method = getattr(logger, level)
            log_method(f"{context}: {error}")
            
            # TODO: Добавить логику отправки уведомлений при критических ошибках
            
        except Exception as e:
            logger.error(f"Ошибка при логировании: {e}")