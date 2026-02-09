"""
MODULE: scripts.apply_tender_statuses_migration
RESPONSIBILITY: Applying tender statuses migration (main script).
ALLOWED: psycopg2, psycopg2.extras, os, dotenv, pathlib, loguru, typing.
FORBIDDEN: None.
ERRORS: None.

Скрипт для применения миграции статусов закупок

Выполняет SQL миграцию для добавления системы статусов закупок,
чтобы исключать "плохие" записи из поиска и ускорить запросы.

⚠️ ВАЖНО: Данные НЕ удаляются! Скрипт только:
- Создает таблицу статусов
- Добавляет столбец status_id в таблицы закупок
- Обновляет значения status_id для существующих записей
- Создает индексы для ускорения поиска

Все существующие данные остаются в БД без изменений.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from pathlib import Path
from loguru import logger
from typing import Optional

# Настройка логирования
logger.add("logs/migration.log", rotation="10 MB", level="INFO")

# Загружаем переменные окружения
load_dotenv()


def get_tender_db_connection():
    """Получение подключения к базе данных tender_monitor"""
    host = os.getenv("TENDER_MONITOR_DB_HOST")
    database = os.getenv("TENDER_MONITOR_DB_DATABASE")
    user = os.getenv("TENDER_MONITOR_DB_USER")
    password = os.getenv("TENDER_MONITOR_DB_PASSWORD")
    port = os.getenv("TENDER_MONITOR_DB_PORT", "5432")
    
    if not all([host, database, user, password]):
        raise ValueError(
            "Не все параметры подключения к БД tender_monitor заданы в .env файле. "
            "Требуются: TENDER_MONITOR_DB_HOST, TENDER_MONITOR_DB_DATABASE, "
            "TENDER_MONITOR_DB_USER, TENDER_MONITOR_DB_PASSWORD"
        )
    
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        logger.info(f"Успешное подключение к БД {database}")
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        raise


def read_sql_file(file_path: Path) -> str:
    """Чтение SQL файла"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Ошибка чтения SQL файла {file_path}: {e}")
        raise


def apply_migration(conn, sql_content: str):
    """Применение миграции"""
    try:
        # Включаем вывод NOTICE сообщений для отслеживания прогресса
        conn.set_session(autocommit=False)
        cursor = conn.cursor()
        
        # Включаем вывод NOTICE в Python
        cursor.execute("SET client_min_messages TO NOTICE")
        
        # Выполняем миграцию
        logger.info("Начало применения миграции...")
        logger.info("Это может занять несколько минут для больших таблиц (22 млн записей)...")
        logger.info("Прогресс будет отображаться ниже:")
        logger.info("-" * 60)
        
        # Выполняем SQL и перехватываем NOTICE сообщения
        cursor.execute(sql_content)
        
        # Получаем все NOTICE сообщения
        conn.commit()
        
        logger.info("-" * 60)
        logger.info("Миграция выполнена!")
        
        # Получаем статистику
        logger.info("Получение статистики по статусам...")
        
        # Статистика для 44ФЗ
        cursor.execute("""
            SELECT 
                ts.name as status_name,
                COUNT(*) as count
            FROM reestr_contract_44_fz r
            LEFT JOIN tender_statuses ts ON r.status_id = ts.id
            GROUP BY ts.name, ts.id
            ORDER BY ts.id
        """)
        
        logger.info("\n=== Статистика по статусам (reestr_contract_44_fz) ===")
        for row in cursor.fetchall():
            status_name = row[0] or "Без статуса"
            count = row[1]
            logger.info(f"  {status_name}: {count} записей")
        
        # Статистика для 223ФЗ
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN r.status_id IS NULL THEN 'Без статуса (используются в поиске)'
                    ELSE ts.name 
                END as status_name,
                COUNT(*) as count
            FROM reestr_contract_223_fz r
            LEFT JOIN tender_statuses ts ON r.status_id = ts.id
            GROUP BY r.status_id, ts.name
            ORDER BY r.status_id NULLS FIRST
        """)
        
        logger.info("\n=== Статистика по статусам (reestr_contract_223_fz) ===")
        for row in cursor.fetchall():
            status_name = row[0]
            count = row[1]
            logger.info(f"  {status_name}: {count} записей")
        
        conn.commit()
        logger.info("\n✅ Миграция успешно применена!")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Ошибка при применении миграции: {e}")
        raise
    finally:
        cursor.close()


def main():
    """Главная функция"""
    logger.info("=" * 60)
    logger.info("Применение миграции: Добавление статусов закупок")
    logger.info("=" * 60)
    
    # Путь к SQL файлу миграции
    script_dir = Path(__file__).parent
    sql_file = script_dir / "add_tender_statuses_migration.sql"
    
    if not sql_file.exists():
        logger.error(f"SQL файл миграции не найден: {sql_file}")
        return
    
    try:
        # Подключаемся к БД
        conn = get_tender_db_connection()
        
        # Читаем SQL файл
        sql_content = read_sql_file(sql_file)
        
        # Применяем миграцию
        apply_migration(conn, sql_content)
        
        logger.info("\n" + "=" * 60)
        logger.info("Миграция завершена успешно!")
        logger.info("=" * 60)
        logger.info("\nСледующие шаги:")
        logger.info("1. Обновите запросы в сервисах для использования статусов")
        logger.info("2. Исключите записи с status_id = 4 (Плохие) из поиска")
        logger.info("3. Для 44ФЗ используйте статусы 1, 2, 3")
        logger.info("4. Для 223ФЗ используйте только записи без статуса (status_id IS NULL)")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Соединение с БД закрыто")


if __name__ == "__main__":
    main()

