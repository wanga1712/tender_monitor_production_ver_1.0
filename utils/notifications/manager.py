"""
Менеджер уведомлений - центральная точка для отправки уведомлений.
"""

import configparser
from typing import Dict, Any, Optional, List
from datetime import datetime

from .base import NotificationProvider, NotificationLevel
from .email import EmailProvider
from .telegram import TelegramProvider
from .webhook import WebhookProvider
from .file import FileProvider

from utils.logger_config import get_logger

logger = get_logger()


class NotificationManager:
    """
    Менеджер уведомлений.
    Управляет всеми провайдерами и отправляет уведомления через них.
    """
    
    def __init__(self, config_path: str = "config.ini"):
        """
        Инициализация менеджера уведомлений.
        
        :param config_path: Путь к файлу конфигурации
        """
        self.config = self._load_config(config_path)
        self.providers: List[NotificationProvider] = []
        self._initialize_providers()
    
    def _load_config(self, config_path: str) -> configparser.ConfigParser:
        """Загружает конфигурацию."""
        config = configparser.ConfigParser()
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config.read_file(f)
        except Exception as e:
            logger.warning(f"Не удалось загрузить конфигурацию уведомлений: {e}")
        return config
    
    def _initialize_providers(self):
        """Инициализирует провайдеры на основе конфигурации."""
        if not self.config.getboolean("notifications", "enabled", fallback=False):
            logger.info("Уведомления отключены в конфигурации")
            return
        
        channels = self.config.get("notifications", "channels", fallback="").split(",")
        channels = [c.strip() for c in channels if c.strip()]
        
        # Email
        if "email" in channels and self.config.getboolean("notifications.email", "enabled", fallback=False):
            try:
                email_provider = EmailProvider(
                    smtp_host=self.config.get("notifications.email", "smtp_host", fallback=""),
                    smtp_port=self.config.getint("notifications.email", "smtp_port", fallback=587),
                    smtp_user=self.config.get("notifications.email", "smtp_user", fallback=""),
                    smtp_password=self.config.get("notifications.email", "smtp_password", fallback=""),
                    from_email=self.config.get("notifications.email", "from_email", fallback=""),
                    to_emails=self.config.get("notifications.email", "to_emails", fallback="").split(",")
                )
                self.providers.append(email_provider)
                logger.info("Email провайдер инициализирован")
            except Exception as e:
                logger.error(f"Ошибка инициализации Email провайдера: {e}")
        
        # Telegram
        if "telegram" in channels and self.config.getboolean("notifications.telegram", "enabled", fallback=False):
            try:
                telegram_provider = TelegramProvider(
                    bot_token=self.config.get("notifications.telegram", "bot_token", fallback=""),
                    chat_id=self.config.get("notifications.telegram", "chat_id", fallback="")
                )
                self.providers.append(telegram_provider)
                logger.info("Telegram провайдер инициализирован")
            except Exception as e:
                logger.error(f"Ошибка инициализации Telegram провайдера: {e}")
        
        # Webhook
        if "webhook" in channels and self.config.getboolean("notifications.webhook", "enabled", fallback=False):
            try:
                webhook_provider = WebhookProvider(
                    url=self.config.get("notifications.webhook", "url", fallback="")
                )
                self.providers.append(webhook_provider)
                logger.info("Webhook провайдер инициализирован")
            except Exception as e:
                logger.error(f"Ошибка инициализации Webhook провайдера: {e}")
        
        # File (всегда включен для истории)
        try:
            file_provider = FileProvider(
                log_dir=self.config.get("notifications.file", "log_dir", fallback="notifications")
            )
            self.providers.append(file_provider)
            logger.info("File провайдер инициализирован")
        except Exception as e:
            logger.error(f"Ошибка инициализации File провайдера: {e}")
    
    def send(self, level: NotificationLevel, title: str, message: str,
             details: Optional[Dict[str, Any]] = None) -> bool:
        """
        Отправляет уведомление через все активные провайдеры.
        
        :param level: Уровень важности
        :param title: Заголовок
        :param message: Сообщение
        :param details: Дополнительные детали
        :return: True если хотя бы один провайдер отправил успешно
        """
        if not self.providers:
            return False
        
        success = False
        for provider in self.providers:
            if provider.is_enabled():
                try:
                    if provider.send(level, title, message, details):
                        success = True
                except Exception as e:
                    logger.error(f"Ошибка отправки уведомления через {provider.__class__.__name__}: {e}")
        
        return success
    
    def send_critical(self, title: str, message: str, error_details: Optional[str] = None):
        """Отправляет критическое уведомление."""
        details = {"error": error_details} if error_details else None
        return self.send(NotificationLevel.CRITICAL, title, message, details)
    
    def send_error(self, title: str, message: str, details: Optional[Dict[str, Any]] = None):
        """Отправляет уведомление об ошибке."""
        return self.send(NotificationLevel.ERROR, title, message, details)
    
    def send_warning(self, title: str, message: str, details: Optional[Dict[str, Any]] = None):
        """Отправляет предупреждение."""
        return self.send(NotificationLevel.WARNING, title, message, details)
    
    def send_info(self, title: str, message: str, details: Optional[Dict[str, Any]] = None):
        """Отправляет информационное уведомление."""
        return self.send(NotificationLevel.INFO, title, message, details)
    
    def send_daily_report(self, stats: Dict[str, Any]):
        """
        Отправляет ежедневный отчет.
        
        :param stats: Словарь со статистикой
        """
        title = f"Ежедневный отчет за {stats.get('date', datetime.now().strftime('%Y-%m-%d'))}"
        
        message = f"""
📊 СТАТИСТИКА ОБРАБОТКИ:

📅 Обработано дат: {stats.get('dates_processed', 0)}
👥 Заказчики: добавлено {stats.get('customers_added', 0)}
🏢 Подрядчики: добавлено {stats.get('contractors_added', 0)}
📋 Контракты: добавлено {stats.get('contracts_added', 0)}
❌ Ошибок: {stats.get('errors_count', 0)}
⏱️  Время работы: {stats.get('uptime', 'N/A')}
"""
        
        return self.send(NotificationLevel.INFO, title, message, stats)
