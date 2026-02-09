"""
MODULE: scripts.apply_error_reason_migration
RESPONSIBILITY: Applying error reason field migration.
ALLOWED: sys, os, pathlib, core.tender_database, config.settings, loguru.
FORBIDDEN: None.
ERRORS: None.

Скрипт для применения миграции - добавление поля error_reason в таблицу tender_document_matches
"""

import sys
import os
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Устанавливаем переменные окружения из предоставленных данных
os.environ['TENDER_MONITOR_DB_HOST'] = '100.122.104.106'
os.environ['TENDER_MONITOR_DB_DATABASE'] = 'tender_monitor'
os.environ['TENDER_MONITOR_DB_USER'] = 'postgres'
os.environ['TENDER_MONITOR_DB_PASSWORD'] = '0IFz3_'
os.environ['TENDER_MONITOR_DB_PORT'] = '5432'

from core.tender_database import TenderDatabaseManager
from config.settings import Config
from loguru import logger

def apply_migration():
    """Применяет миграцию для добавления поля error_reason"""
    try:
        logger.info("Загружаем конфигурацию БД tender_monitor...")
        settings = Config()
        config = settings.tender_database

        logger.info("Проверяем подключение к БД tender_monitor...")
        db_manager = TenderDatabaseManager(config)

        # Проверяем конфигурацию
        config = db_manager._config
        logger.info(f"Конфигурация БД: host={config.host}, database={config.database}, user={config.user}, port={config.port}")

        db_manager.connect()
        logger.info("✅ Подключение к БД успешно установлено")

        logger.info("Применяю миграцию: добавление поля error_reason...")

        # Читаем SQL скрипт миграции
        migration_file = project_root / "scripts" / "add_error_reason_to_tender_document_matches.sql"
        if not migration_file.exists():
            logger.error(f"Файл миграции не найден: {migration_file}")
            return False

        with open(migration_file, 'r', encoding='utf-8') as f:
            migration_sql = f.read()

        logger.info(f"SQL миграции: {migration_sql[:100]}...")

        # Применяем миграцию
        db_manager.execute_update(migration_sql)

        logger.info("✅ Миграция успешно применена")

        # Проверяем, что поле добавлено
        check_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = 'tender_document_matches'
            AND column_name = 'error_reason'
        """
        result = db_manager.execute_query(check_query)

        if result:
            logger.info(f"✅ Поле error_reason успешно добавлено: {result[0]}")
        else:
            logger.error("❌ Поле error_reason не найдено после миграции")
            return False

        db_manager.disconnect()
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка при применении миграции: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = apply_migration()
    sys.exit(0 if success else 1)

