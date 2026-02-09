"""
MODULE: scripts.apply_deal_chat_migration
RESPONSIBILITY: Applying deal chat table migration.
ALLOWED: psycopg2, os, pathlib, dotenv, sys, loguru.
FORBIDDEN: None.
ERRORS: None.

Применение миграции для таблицы deal_chat (чат по сделкам).
"""

import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv
import sys
from loguru import logger

# Загружаем переменные окружения из .env
load_dotenv()


def get_tender_db_connection_params() -> dict:
    """Получение параметров подключения к tender_monitor из переменных окружения."""
    params = {
        "host": os.getenv("TENDER_MONITOR_DB_HOST"),
        "database": os.getenv("TENDER_MONITOR_DB_DATABASE"),
        "user": os.getenv("TENDER_MONITOR_DB_USER"),
        "password": os.getenv("TENDER_MONITOR_DB_PASSWORD"),
        "port": os.getenv("TENDER_MONITOR_DB_PORT", "5432"),
    }

    missing_params = [k for k, v in params.items() if v is None and k != "port"]
    if missing_params:
        raise ValueError(
            f"Отсутствуют обязательные параметры TENDER_MONITOR_DB в .env: {', '.join(missing_params)}"
        )

    return params


def apply_sql_migration(sql_file_path: Path):
    """Применение SQL-миграции к tender_monitor."""
    logger.info(f"Применение миграции из файла: {sql_file_path}")

    conn = None
    try:
        db_params = get_tender_db_connection_params()
        logger.info(
            f"Подключение к PostgreSQL: host={db_params['host']}, port={db_params['port']}, "
            f"db={db_params['database']}, user={db_params['user']}"
        )

        conn = psycopg2.connect(**db_params)
        conn.autocommit = False
        cursor = conn.cursor()

        sql_script = sql_file_path.read_text(encoding="utf-8")

        logger.info("Выполнение SQL-скрипта...")
        cursor.execute(sql_script)

        conn.commit()
        logger.info(f"Миграция успешно применена из {sql_file_path}")
        print(f"✅ Миграция успешно применена из {sql_file_path}")

    except ValueError as ve:
        logger.error(f"Ошибка конфигурации: {ve}")
        print(f"❌ Ошибка конфигурации: {ve}")
        sys.exit(1)
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка БД при применении миграции: {e}", exc_info=True)
        print(f"❌ Ошибка БД при применении миграции: {e}")
        sys.exit(1)
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Неожиданная ошибка при применении миграции: {e}", exc_info=True)
        print(f"❌ Неожиданная ошибка при применении миграции: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logger.info("Соединение с БД закрыто.")


if __name__ == "__main__":
    migration_file = (
        Path(__file__).parent / "sql_queries" / "add_deal_chat_table.sql"
    )
    if not migration_file.exists():
        logger.error(f"Файл миграции не найден: {migration_file}")
        print(f"❌ Файл миграции не найден: {migration_file}")
        sys.exit(1)

    apply_sql_migration(migration_file)

