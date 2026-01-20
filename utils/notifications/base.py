"""
Базовые классы для системы уведомлений.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any, Optional
from datetime import datetime


class NotificationLevel(Enum):
    """Уровни важности уведомлений."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class NotificationProvider(ABC):
    """
    Базовый класс для провайдеров уведомлений.
    Все провайдеры должны наследоваться от этого класса.
    """
    
    def __init__(self, enabled: bool = True):
        """
        Инициализация провайдера.
        
        :param enabled: Включен ли провайдер
        """
        self.enabled = enabled
    
    @abstractmethod
    def send(self, level: NotificationLevel, title: str, message: str, 
             details: Optional[Dict[str, Any]] = None) -> bool:
        """
        Отправляет уведомление.
        
        :param level: Уровень важности
        :param title: Заголовок уведомления
        :param message: Текст сообщения
        :param details: Дополнительные детали (словарь)
        :return: True если отправлено успешно, False в противном случае
        """
        pass
    
    def is_enabled(self) -> bool:
        """Проверяет, включен ли провайдер."""
        return self.enabled
    
    def format_message(self, level: NotificationLevel, title: str, message: str,
                      details: Optional[Dict[str, Any]] = None) -> str:
        """
        Форматирует сообщение для отправки.
        
        :param level: Уровень важности
        :param title: Заголовок
        :param message: Сообщение
        :param details: Дополнительные детали
        :return: Отформатированное сообщение
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level_emoji = {
            NotificationLevel.INFO: "ℹ️",
            NotificationLevel.WARNING: "⚠️",
            NotificationLevel.ERROR: "❌",
            NotificationLevel.CRITICAL: "🚨"
        }
        
        emoji = level_emoji.get(level, "ℹ️")
        formatted = f"{emoji} [{level.value.upper()}] {title}\n"
        formatted += f"Время: {timestamp}\n"
        formatted += f"\n{message}\n"
        
        if details:
            formatted += "\nДетали:\n"
            for key, value in details.items():
                formatted += f"  • {key}: {value}\n"
        
        return formatted
