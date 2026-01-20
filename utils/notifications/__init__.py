"""
Модуль уведомлений для TenderMonitor.

Поддерживает различные каналы уведомлений:
- Email (SMTP)
- Telegram Bot
- Webhook (HTTP POST)
- SMS (через API)
- Файловые уведомления (JSON/лог)
"""

from .manager import NotificationManager
from .base import NotificationProvider, NotificationLevel

__all__ = ['NotificationManager', 'NotificationProvider', 'NotificationLevel']
