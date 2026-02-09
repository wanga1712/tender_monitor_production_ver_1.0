"""
MODULE: logger
RESPONSIBILITY: Centralized Loguru configuration and logger instance provision.
ALLOWED: Configuring loguru, exporting `logger` object.
FORBIDDEN: Business logic, re-configuring logger in other modules.
ERRORS: OSError (if log directory creation fails).

Централизованная настройка логирования через Loguru.
ЗАПРЕЩЕНО настраивать logger в других модулях!
Все модули должны импортировать logger отсюда: from logger import logger
"""
import sys
from pathlib import Path
from loguru import logger

# Удаляем стандартный handler
logger.remove()

# Путь к директории логов
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Консольный вывод (INFO и выше)
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
    colorize=True,
)

# Файл приложения (DEBUG и выше)
logger.add(
    LOG_DIR / "app.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
    rotation="10 MB",
    retention="30 days",
    compression="zip",
)

# Файл ошибок (ERROR и выше)
logger.add(
    LOG_DIR / "errors.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="ERROR",
    rotation="10 MB",
    retention="90 days",
    compression="zip",
)

__all__ = ["logger"]
