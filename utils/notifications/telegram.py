"""
Telegram провайдер для отправки уведомлений через Telegram Bot API.
"""

import requests
from typing import Optional, Dict, Any

from .base import NotificationProvider, NotificationLevel
from utils.logger_config import get_logger

logger = get_logger()


class TelegramProvider(NotificationProvider):
    """Провайдер для отправки уведомлений в Telegram."""
    
    API_URL = "https://api.telegram.org/bot{token}/sendMessage"
    
    def __init__(self, bot_token: str, chat_id: str, enabled: bool = True):
        """
        Инициализация Telegram провайдера.
        
        :param bot_token: Токен Telegram бота
        :param chat_id: ID чата для отправки сообщений
        :param enabled: Включен ли провайдер
        """
        super().__init__(enabled)
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = self.API_URL.format(token=bot_token)
    
    def send(self, level: NotificationLevel, title: str, message: str,
             details: Optional[Dict[str, Any]] = None) -> bool:
        """Отправляет Telegram уведомление."""
        if not self.enabled or not self.bot_token or not self.chat_id:
            return False
        
        try:
            # Формируем сообщение
            formatted_message = self.format_message(level, title, message, details)
            
            # Экранируем специальные символы для Markdown
            formatted_message = formatted_message.replace("_", "\\_").replace("*", "\\*")
            
            # Отправляем запрос
            payload = {
                "chat_id": self.chat_id,
                "text": formatted_message,
                "parse_mode": "Markdown"
            }
            
            response = requests.post(self.api_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"Telegram уведомление отправлено: {title}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки Telegram уведомления: {e}")
            return False
