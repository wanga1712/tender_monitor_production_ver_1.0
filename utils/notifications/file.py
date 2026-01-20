"""
Файловый провайдер для сохранения уведомлений в файлы.
"""

import os
import json
from datetime import datetime
from typing import Optional, Dict, Any

from .base import NotificationProvider, NotificationLevel
from utils.logger_config import get_logger

logger = get_logger()


class FileProvider(NotificationProvider):
    """Провайдер для сохранения уведомлений в файлы."""
    
    def __init__(self, log_dir: str = "notifications", enabled: bool = True):
        """
        Инициализация файлового провайдера.
        
        :param log_dir: Директория для сохранения уведомлений
        :param enabled: Включен ли провайдер
        """
        super().__init__(enabled)
        self.log_dir = log_dir
        self._ensure_directory()
    
    def _ensure_directory(self):
        """Создает директорию для логов, если её нет."""
        try:
            os.makedirs(self.log_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"Ошибка создания директории {self.log_dir}: {e}")
    
    def send(self, level: NotificationLevel, title: str, message: str,
             details: Optional[Dict[str, Any]] = None) -> bool:
        """Сохраняет уведомление в файл."""
        if not self.enabled:
            return False
        
        try:
            timestamp = datetime.now()
            date_str = timestamp.strftime("%Y-%m-%d")
            
            # Файл для текстовых логов
            log_file = os.path.join(self.log_dir, f"notifications_{date_str}.log")
            
            # Файл для JSON логов
            json_file = os.path.join(self.log_dir, f"notifications_{date_str}.json")
            
            # Сохраняем текстовый лог
            formatted_message = self.format_message(level, title, message, details)
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"\n{'='*60}\n")
                f.write(formatted_message)
                f.write(f"\n{'='*60}\n")
            
            # Сохраняем JSON лог
            notification_data = {
                "timestamp": timestamp.isoformat(),
                "level": level.value,
                "title": title,
                "message": message,
                "details": details or {}
            }
            
            # Читаем существующие уведомления
            notifications = []
            if os.path.exists(json_file):
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
                        notifications = json.load(f)
                except:
                    notifications = []
            
            # Добавляем новое уведомление
            notifications.append(notification_data)
            
            # Сохраняем обратно
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(notifications, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"Уведомление сохранено в файл: {title}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения уведомления в файл: {e}")
            return False
