"""
MODULE: scripts.apply_deal_item_migration
RESPONSIBILITY: Applying deal item type migration.
ALLOWED: psycopg2, os, dotenv, loguru.
FORBIDDEN: None.
ERRORS: None.

Скрипт для применения миграции добавления item_type в таблицу deal_item.
"""

import psycopg2
import os
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

DB_HOST = os.getenv("TENDER_MONITOR_DB_HOST")
DB_DATABASE = os.getenv("TENDER_MONITOR_DB_DATABASE")
DB_USER = os.getenv("TENDER_MONITOR_DB_USER")
DB_PASSWORD = os.getenv("TENDER_MONITOR_DB_PASSWORD")
DB_PORT = os.getenv("TENDER_MONITOR_DB_PORT")

MIGRATION_FILE = "scripts/sql_queries/add_item_type_to_deal_item.sql"


def apply_migration():
    """Применение миграции для добавления item_type в deal_item."""
    logger.info(f"Попытка применения миграции из {MIGRATION_FILE}...")
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = conn.cursor()

        with open(MIGRATION_FILE, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        cursor.execute(sql_script)
        conn.commit()
        logger.success(f"Миграция из {MIGRATION_FILE} успешно применена.")
        
        # Проверяем, что поле добавлено
        cursor.execute("""
            SELECT column_name, data_type, column_default
            FROM information_schema.columns
            WHERE table_name = 'deal_item' AND column_name = 'item_type'
        """)
        result = cursor.fetchone()
        if result:
            logger.info(f"Поле item_type добавлено: {result}")
        else:
            logger.warning("Поле item_type не найдено после миграции!")

    except Exception as e:
        logger.error(f"Ошибка при применении миграции из {MIGRATION_FILE}: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    apply_migration()

