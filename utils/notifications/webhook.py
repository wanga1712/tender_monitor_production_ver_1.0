"""
Webhook провайдер для отправки уведомлений через HTTP POST.
"""

import requests
import json
from datetime import datetime
from typing import Optional, Dict, Any

from .base import NotificationProvider, NotificationLevel
from utils.logger_config import get_logger

logger = get_logger()


class WebhookProvider(NotificationProvider):
    """Провайдер для отправки уведомлений через Webhook."""
    
    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None,
                 enabled: bool = True):
        """
        Инициализация Webhook провайдера.
        
        :param url: URL webhook endpoint
        :param headers: Дополнительные заголовки HTTP
        :param enabled: Включен ли провайдер
        """
        super().__init__(enabled)
        self.url = url
        self.headers = headers or {"Content-Type": "application/json"}
    
    def send(self, level: NotificationLevel, title: str, message: str,
             details: Optional[Dict[str, Any]] = None) -> bool:
        """Отправляет webhook уведомление."""
        if not self.enabled or not self.url:
            return False
        
        try:
            # Формируем payload
            payload = {
                "level": level.value,
                "title": title,
                "message": message,
                "timestamp": str(datetime.now()),
                "details": details or {}
            }
            
            # Отправляем запрос
            response = requests.post(
                self.url,
                json=payload,
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            
            logger.info(f"Webhook уведомление отправлено: {title}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки webhook уведомления: {e}")
            return False
