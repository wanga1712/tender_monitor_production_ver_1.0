"""
MODULE: scripts.ensure_tender_tables
RESPONSIBILITY: Checking and creating tender_document_matches and tender_document_match_details tables.
ALLOWED: sys, pathlib, loguru, config.settings, core.tender_database, core.exceptions.
FORBIDDEN: None.
ERRORS: None.

Скрипт для проверки и создания необходимых таблиц в базе данных tender_monitor.

Проверяет существование таблиц:
- tender_document_matches
- tender_document_match_details

И создает их, если они не существуют.
"""

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from config.settings import config
from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseConnectionError

# SQL для создания таблицы tender_document_matches (если её нет)
CREATE_TENDER_DOCUMENT_MATCHES_TABLE = """
CREATE TABLE IF NOT EXISTS tender_document_matches (
    id SERIAL PRIMARY KEY,
    tender_id INTEGER NOT NULL,
    registry_type VARCHAR(10) NOT NULL,
    match_count INTEGER NOT NULL DEFAULT 0,
    match_percentage NUMERIC(5, 2) NOT NULL DEFAULT 0.0,
    processing_time_seconds NUMERIC(10, 2),
    total_files_processed INTEGER DEFAULT 0,
    total_size_bytes BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tender_id, registry_type)
);

CREATE INDEX IF NOT EXISTS idx_tender_document_matches_tender_id 
ON tender_document_matches(tender_id, registry_type);
"""

# SQL для создания таблицы tender_document_match_details
CREATE_TENDER_DOCUMENT_MATCH_DETAILS_TABLE = """
CREATE TABLE IF NOT EXISTS tender_document_match_details (
    id SERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES tender_document_matches(id) ON DELETE CASCADE,
    product_name TEXT NOT NULL,
    score NUMERIC(5, 2) NOT NULL DEFAULT 0.0,
    sheet_name TEXT,
    row_index INTEGER,
    column_letter TEXT,
    cell_address TEXT,
    source_file TEXT,
    matched_text TEXT,
    matched_display_text TEXT,
    matched_keywords TEXT[],
    row_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_match_details_match_id 
ON tender_document_match_details(match_id);
"""


def table_exists(db_manager: TenderDatabaseManager, table_name: str) -> bool:
    """Проверяет существование таблицы в базе данных."""
    try:
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """
        result = db_manager.execute_query(query, (table_name,))
        return result[0].get('exists', False) if result else False
    except Exception as error:
        logger.error(f"Ошибка при проверке существования таблицы {table_name}: {error}")
        return False


def create_table(db_manager: TenderDatabaseManager, sql: str, table_name: str) -> bool:
    """Создает таблицу в базе данных."""
    try:
        # Разбиваем SQL на отдельные команды (CREATE TABLE и CREATE INDEX)
        commands = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]
        
        for command in commands:
            if command:
                # Для DDL команд используем execute_query (он тоже выполняет команды)
                db_manager.execute_query(command)
        
        logger.info(f"✅ Таблица {table_name} успешно создана или уже существует")
        return True
    except Exception as error:
        logger.error(f"❌ Ошибка при создании таблицы {table_name}: {error}")
        return False


def main():
    """Основная функция."""
    logger.info("Проверка и создание необходимых таблиц в базе данных tender_monitor...")
    logger.info("=" * 80)
    
    # Подключаемся к БД tender_monitor
    try:
        tender_db = TenderDatabaseManager(config.tender_database)
        tender_db.connect()
        logger.info("✅ Подключение к БД tender_monitor установлено")
    except DatabaseConnectionError as error:
        logger.error(f"❌ Ошибка подключения к БД: {error}")
        sys.exit(1)
    
    try:
        # Проверяем и создаем таблицу tender_document_matches
        if not table_exists(tender_db, "tender_document_matches"):
            logger.info("Таблица tender_document_matches не найдена, создаю...")
            create_table(tender_db, CREATE_TENDER_DOCUMENT_MATCHES_TABLE, "tender_document_matches")
        else:
            logger.info("✅ Таблица tender_document_matches уже существует")
        
        # Проверяем и создаем таблицу tender_document_match_details
        if not table_exists(tender_db, "tender_document_match_details"):
            logger.info("Таблица tender_document_match_details не найдена, создаю...")
            create_table(tender_db, CREATE_TENDER_DOCUMENT_MATCH_DETAILS_TABLE, "tender_document_match_details")
        else:
            logger.info("✅ Таблица tender_document_match_details уже существует")
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Все необходимые таблицы проверены и готовы к использованию")
        
    except Exception as error:
        logger.error(f"❌ Критическая ошибка: {error}")
        sys.exit(1)
    finally:
        tender_db.disconnect()
        logger.info("Соединение с БД закрыто")


if __name__ == "__main__":
    # Настройка логирования
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="{time:HH:mm:ss} | {level: <8} | {message}",
        colorize=True
    )
    
    main()

