"""
MODULE: core.exceptions
RESPONSIBILITY: Define core-specific exception classes.
ALLOWED: Inheriting from AppError or CommercialAppError.
FORBIDDEN: Business logic.
ERRORS: None.

Пользовательские исключения приложения
"""

class CommercialAppError(Exception):
    """Базовое исключение приложения"""
    pass

class DatabaseConnectionError(CommercialAppError):
    """Ошибка подключения к базе данных"""
    pass

class DatabaseQueryError(CommercialAppError):
    """Ошибка выполнения запроса к базе данных"""
    pass

class ConfigurationError(CommercialAppError):
    """Ошибка конфигурации приложения"""
    pass

class DeliveryCalculationError(CommercialAppError):
    """Ошибка расчета доставки"""
    pass

class QuotationError(CommercialAppError):
    """Ошибка работы с коммерческими предложениями"""
    pass


class DocumentSearchError(CommercialAppError):
    """Ошибка поиска и обработки документации"""
    pass


class FileCleanupError(CommercialAppError):
    """Ошибка критической очистки файлов после обработки"""
    pass