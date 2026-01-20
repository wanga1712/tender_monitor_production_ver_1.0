"""
Email провайдер для отправки уведомлений через SMTP.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional, Dict, Any

from .base import NotificationProvider, NotificationLevel
from utils.logger_config import get_logger

logger = get_logger()


class EmailProvider(NotificationProvider):
    """Провайдер для отправки уведомлений по Email."""
    
    def __init__(self, smtp_host: str, smtp_port: int, smtp_user: str,
                 smtp_password: str, from_email: str, to_emails: List[str],
                 enabled: bool = True):
        """
        Инициализация Email провайдера.
        
        :param smtp_host: SMTP сервер
        :param smtp_port: Порт SMTP
        :param smtp_user: Имя пользователя SMTP
        :param smtp_password: Пароль SMTP
        :param from_email: Email отправителя
        :param to_emails: Список получателей
        :param enabled: Включен ли провайдер
        """
        super().__init__(enabled)
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_email = from_email
        self.to_emails = [email.strip() for email in to_emails if email.strip()]
    
    def send(self, level: NotificationLevel, title: str, message: str,
             details: Optional[Dict[str, Any]] = None) -> bool:
        """Отправляет email уведомление."""
        if not self.enabled or not self.to_emails:
            return False
        
        try:
            # Формируем сообщение
            formatted_message = self.format_message(level, title, message, details)
            
            # Создаем email
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ", ".join(self.to_emails)
            msg['Subject'] = f"[TenderMonitor] {title}"
            
            # Добавляем текст
            msg.attach(MIMEText(formatted_message, 'plain', 'utf-8'))
            
            # Отправляем
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"Email уведомление отправлено: {title}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки email: {e}")
            return False
