"""
Настройка логирования для проекта.
Логи выводятся только в файл, в консоль - только прогресс-бары.
"""
import sys
from loguru import logger

# Удаляем стандартный обработчик loguru (который выводит в консоль)
logger.remove()

# Добавляем обработчик для ошибок в файл
logger.add(
    "errors.log",
    level="ERROR",
    rotation="1 week",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
    backtrace=True,
    diagnose=True
)

# Добавляем обработчик для DEBUG логов (успешные записи в БД)
logger.add(
    "debug.log",
    level="DEBUG",
    rotation="1 day",
    compression="zip",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
    retention="7 days"
)

# Для отладки можно временно включить вывод в консоль
# Раскомментируйте следующие строки, если нужно видеть логи в консоли
# logger.add(
#     sys.stderr,
#     level="INFO",
#     format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
# )

def get_logger():
    """Возвращает настроенный logger."""
    return logger

