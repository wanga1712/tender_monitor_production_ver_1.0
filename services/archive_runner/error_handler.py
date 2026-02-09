"""
MODULE: services.archive_runner.error_handler
RESPONSIBILITY:
- Централизованная обработка ошибок и исключений
- Восстановление после сбоев подключения к БД
- Безопасное выполнение операций с обработкой ошибок
ALLOWED:
- Обработка исключений и ошибок
- Логирование через loguru
- Повторные попытки подключения
FORBIDDEN:
- Прямые операции с бизнес-логикой
- Управление файловыми операциями
- Прямые запросы к базе данных
ERRORS:
- Должен поглощать и логировать ошибки, не пробрасывать наружу
"""

import time
from typing import Any, Callable, Optional

from loguru import logger
from core.exceptions import DatabaseConnectionError


class ErrorHandler:
    """Обработчик ошибок и восстановления системы"""

    def __init__(self, max_retries: int = 3, retry_delay: float = 2.0):
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def safe_call(self, func: Callable, *args, **kwargs) -> Any:
        """Безопасный вызов функции с обработкой ошибок"""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
                
            except DatabaseConnectionError as e:
                logger.warning(f"Ошибка подключения к БД (попытка {attempt + 1}/{self.max_retries}): {e}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay)
                
            except Exception as e:
                logger.error(f"Неожиданная ошибка в safe_call: {e}")
                raise

    def handle_db_disconnect(self, error: Exception) -> None:
        """Обработать отключение базы данных"""
        logger.error(f"Критическая ошибка подключения к БД: {error}")
        logger.info("Пытаемся восстановить подключение...")
        
        # Здесь можно добавить логику повторного подключения
        # или уведомления администратора

    def handle_failed_tender(self, tender: dict, error: Exception, 
                           queue_manager: Any = None) -> None:
        """Обработать неудачную обработку торгов"""
        tender_id = tender.get('id')
        registry_type = tender.get('registry_number', 'unknown')[:2] if tender.get('registry_number') else 'unknown'
        
        logger.error(f"Ошибка обработки tender_id={tender_id}: {error}")
        
        if queue_manager:
            # Помечаем торг как проблемный в очереди
            queue_manager.mark_tender_as_failed(tender_id, str(error))

    def ensure_connection(self, check_func: Callable[[], bool]) -> bool:
        """Гарантировать рабочее подключение"""
        for attempt in range(self.max_retries):
            if check_func():
                return True
            
            logger.warning(f"Попытка подключения {attempt + 1}/{self.max_retries} не удалась")
            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay)
        
        return False

    def attempt_reconnect(self, connect_func: Callable[[], Any]) -> Any:
        """Попытаться переподключиться"""
        for attempt in range(self.max_retries):
            try:
                return connect_func()
            except Exception as e:
                logger.warning(f"Попытка переподключения {attempt + 1}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
        
        raise DatabaseConnectionError("Не удалось восстановить подключение после всех попыток")