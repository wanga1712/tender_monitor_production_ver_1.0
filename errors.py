"""
MODULE: errors
RESPONSIBILITY: Define the project-wide error taxonomy and exception hierarchy.
ALLOWED: Defining exception classes inheriting from AppError.
FORBIDDEN: Business logic, external imports (except standard library).
ERRORS: None (defines errors).

Таксономия ошибок проекта TenderMonitor.
Все исключения должны наследоваться от AppError.
"""


class AppError(Exception):
    """Базовый класс для всех ошибок приложения"""
    pass


class ConfigError(AppError):
    """Ошибки конфигурации (отсутствующие или неверные настройки)"""
    pass


class ParseError(AppError):
    """Ошибки парсинга XML, JSON или других данных"""
    pass


class NetworkError(AppError):
    """Ошибки сети (SOAP, HTTP, stunnel)"""
    pass


class DBError(AppError):
    """Ошибки работы с базой данных PostgreSQL"""
    pass


class FileSystemError(AppError):
    """Ошибки работы с файловой системой"""
    pass


class ValidationError(AppError):
    """Ошибки валидации данных (OKPD, бизнес-правила)"""
    pass


class ServiceError(AppError):
    """Ошибки, связанные с работой сервисов"""
    pass


class SystemdError(AppError):
    """Ошибки взаимодействия с systemd"""
    pass
