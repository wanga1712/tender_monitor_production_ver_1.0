"""
Централизованная конфигурация проекта TenderMonitor.

Правила:
- config.py является единственным источником правды для путей и ключевых настроек
- все пути хранятся как объекты pathlib.Path
- значения берутся из переменных окружения с разумными значениями по умолчанию
- при отсутствии .env в корне проекта создаётся шаблон с русскими комментариями
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

from dotenv import load_dotenv


# Корень проекта (папка, где лежит данный файл)
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent

# Путь к .env файлу приложения
APP_ENV_PATH: Final[Path] = PROJECT_ROOT / ".env"


def _ensure_env_file_exists(env_path: Path) -> None:
    """
    Гарантирует наличие .env файла.

    Если файл отсутствует, создаёт шаблон с примерными переменными окружения
    и русскими комментариями, совместимый с Linux.
    """
    if env_path.exists():
        return

    template_lines = [
        "# Файл переменных окружения для сервиса TenderMonitor",
        "# Значения ниже являются примерами. Обязательно замените их на реальные.",
        "",
        "# === Подключение к PostgreSQL (основная БД приложения) ===",
        "# Формат: postgresql://USER:PASSWORD@HOST:PORT/DB_NAME",
        "TM_DB_DSN=postgresql://tendermonitor:tendermonitor@localhost:5432/tendermonitor",
        "",
        "# === Общие настройки ===",
        "# Режим отображения прогресса: rich | simple",
        "PROGRESS_MODE=rich",
        "",
        "# Лимит памяти процесса в мегабайтах (используется для безопасного рестарта под systemd)",
        "TM_MEMORY_LIMIT_MB=4000",
        "",
        "# Пример дополнительных переменных можно добавлять ниже.",
        "",
    ]

    env_path.write_text("\n".join(template_lines), encoding="utf-8")


# Автоматически создаём .env при первом запуске, если его нет
_ensure_env_file_exists(APP_ENV_PATH)

# Загружаем переменные окружения из .env
load_dotenv(APP_ENV_PATH)


# === Пути к ключевым файлам конфигурации и данным ===

CONFIG_INI_PATH: Final[Path] = Path(
    os.getenv("TM_CONFIG_INI_PATH", PROJECT_ROOT / "config.ini")
)

PROCESSED_DATES_FILE: Final[Path] = Path(
    os.getenv("TM_PROCESSED_DATES_FILE", PROJECT_ROOT / "processed_dates.json")
)

REGION_PROGRESS_FILE: Final[Path] = Path(
    os.getenv("TM_REGION_PROGRESS_FILE", PROJECT_ROOT / "region_progress.json")
)

ERROR_LOG_PATH: Final[Path] = Path(
    os.getenv("TM_ERROR_LOG_PATH", PROJECT_ROOT / "errors.log")
)

DEBUG_LOG_PATH: Final[Path] = Path(
    os.getenv("TM_DEBUG_LOG_PATH", PROJECT_ROOT / "debug.log")
)


# === Настройки памяти / ресурсов ===

DEFAULT_MEMORY_LIMIT_MB: Final[int] = int(os.getenv("TM_MEMORY_LIMIT_MB", "4000"))


def get_db_dsn() -> str:
    """
    Возвращает строку подключения к основной БД TenderMonitor.

    По умолчанию берётся TM_DB_DSN из окружения.
    """
    dsn = os.getenv("TM_DB_DSN")
    if not dsn:
        # Явно даём понять, что переменная не задана.
        raise RuntimeError(
            "Переменная окружения TM_DB_DSN не задана. "
            "Укажите строку подключения к PostgreSQL в .env файле."
        )
    return dsn
