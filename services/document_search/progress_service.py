"""
Сервис для управления прогрессом операций поиска документов.

Отвечает за:
- Обновление прогресса через callback функции
- Логирование этапов выполнения
- Обработку ошибок при обновлении прогресса
"""

from typing import Optional, Callable
from loguru import logger


class ProgressService:
    """Сервис управления прогрессом операций поиска документов."""

    def __init__(self, progress_callback: Optional[Callable] = None):
        """
        Инициализация сервиса прогресса.
        
        Args:
            progress_callback: Функция для обновления прогресса (stage, progress, detail)
        """
        self.progress_callback = progress_callback

    def update_progress(self, stage: str, progress: int, detail: Optional[str] = None) -> None:
        """
        Обновление прогресса через callback.
        
        Args:
            stage: Этап выполнения
            progress: Процент выполнения (0-100)
            detail: Детальная информация
        """
        if self.progress_callback:
            try:
                self.progress_callback(stage, progress, detail)
            except Exception as error:
                logger.debug(f"Ошибка при обновлении прогресса: {error}")

    def log_stage_start(self, stage: str, message: str) -> None:
        """
        Логирование начала этапа с обновлением прогресса.
        
        Args:
            stage: Название этапа
            message: Сообщение для логирования
        """
        logger.info(message)
        self.update_progress(stage, 0, message)

    def log_stage_progress(self, stage: str, progress: int, current: int, total: int, 
                          item_name: str) -> None:
        """
        Логирование прогресса этапа.
        
        Args:
            stage: Название этапа
            progress: Общий прогресс (0-100)
            current: Текущий элемент
            total: Всего элементов
            item_name: Название текущего элемента
        """
        detail = f"Обработано: {current}/{total} - {item_name}"
        self.update_progress(stage, progress, detail)

    def log_stage_complete(self, stage: str, progress: int, message: str) -> None:
        """
        Логирование завершения этапа.
        
        Args:
            stage: Название этапа
            progress: Прогресс завершения
            message: Сообщение о завершении
        """
        logger.info(message)
        self.update_progress(stage, progress, message)