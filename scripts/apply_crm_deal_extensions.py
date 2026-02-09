"""
MODULE: scripts.apply_crm_deal_extensions
RESPONSIBILITY: Applying CRM deal extensions migration.
ALLOWED: os, sys, pathlib, psycopg2, dotenv, loguru.
FORBIDDEN: None.
ERRORS: None.

Применение миграции add_crm_deal_extensions.sql к БД tender_monitor.

Скрипт использует .env (переменные TENDER_MONITOR_DB_*) — те же, что и приложение.
"""

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from loguru import logger


def apply_crm_deal_extensions() -> None:
    """Применяет SQL-скрипт scripts/sql_queries/add_crm_deal_extensions.sql к tender_monitor."""
    # Загружаем .env, чтобы получить TENDER_MONITOR_DB_*
    load_dotenv()

    host = os.getenv("TENDER_MONITOR_DB_HOST")
    database = os.getenv("TENDER_MONITOR_DB_DATABASE")
    user = os.getenv("TENDER_MONITOR_DB_USER")
    password = os.getenv("TENDER_MONITOR_DB_PASSWORD")
    port = os.getenv("TENDER_MONITOR_DB_PORT", "5432")

    if not all([host, database, user, password]):
        print(
            "❌ Не заданы параметры подключения к tender_monitor в .env: "
            "TENDER_MONITOR_DB_HOST, TENDER_MONITOR_DB_DATABASE, "
            "TENDER_MONITOR_DB_USER, TENDER_MONITOR_DB_PASSWORD"
        )
        sys.exit(1)

    sql_path = Path(__file__).parent / "sql_queries" / "add_crm_deal_extensions.sql"
    if not sql_path.exists():
        print(f"❌ Не найден файл миграции: {sql_path}")
        sys.exit(1)

    print("=" * 80)
    print("Применение миграции CRM-расширений к БД tender_monitor")
    print("=" * 80)
    print(f"Файл миграции: {sql_path}")
    print(f"Подключение: host={host}, db={database}, user={user}, port={port}")
    print()

    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )
        conn.autocommit = False
        with conn.cursor() as cur:
            sql_text = sql_path.read_text(encoding="utf-8")
            cur.execute(sql_text)
        conn.commit()
        print("✅ Миграция успешно применена.")
    except Exception as exc:
        if conn:
            conn.rollback()
        print(f"❌ Ошибка при применении миграции: {exc}")
        logger.error(f"Ошибка при применении миграции add_crm_deal_extensions: {exc}", exc_info=True)
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("Соединение с БД закрыто.")


if __name__ == "__main__":
    apply_crm_deal_extensions()


