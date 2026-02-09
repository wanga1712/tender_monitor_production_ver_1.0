"""
MODULE: scripts.apply_file_errors_migration
RESPONSIBILITY: Applying table migrations for file errors.
ALLOWED: sys, pathlib, loguru, core.tender_database, config.settings, core.exceptions.
FORBIDDEN: None.
ERRORS: None.

Скрипт для применения миграций:
1. Добавление поля has_error в таблицу tender_document_matches
2. Создание таблицу tender_document_file_errors для хранения информации об ошибках файлов
"""

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from core.tender_database import TenderDatabaseManager
from config.settings import config
from core.exceptions import DatabaseConnectionError


def apply_migration():
    """Применяет миграции для поддержки статуса ошибок и таблицы ошибок файлов."""
    
    if not config.tender_database:
        logger.error("❌ Конфигурация БД tender_monitor не задана в .env файле!")
        sys.exit(1)
    
    try:
        db_manager = TenderDatabaseManager(config.tender_database)
        db_manager.connect()
        logger.info("✅ Подключение к БД tender_monitor установлено")
    except DatabaseConnectionError as error:
        logger.error(f"❌ Ошибка подключения к БД tender_monitor: {error}")
        sys.exit(1)
    
    try:
        # 1. Добавляем поле has_error
        logger.info("📝 Применение миграции: добавление поля has_error...")
        migration_1_path = project_root / "scripts" / "add_has_error_to_tender_document_matches.sql"
        if migration_1_path.exists():
            with open(migration_1_path, "r", encoding="utf-8") as f:
                migration_1_sql = f.read()
            db_manager.execute_update(migration_1_sql)
            logger.info("✅ Поле has_error добавлено в таблицу tender_document_matches")
        else:
            logger.warning(f"⚠️ Файл миграции не найден: {migration_1_path}")
        
        # 2. Создаем таблицу tender_document_file_errors
        logger.info("📝 Применение миграции: создание таблицы tender_document_file_errors...")
        migration_2_path = project_root / "scripts" / "create_tender_document_file_errors_table.sql"
        if migration_2_path.exists():
            with open(migration_2_path, "r", encoding="utf-8") as f:
                migration_2_sql = f.read()
            db_manager.execute_update(migration_2_sql)
            logger.info("✅ Таблица tender_document_file_errors создана")
        else:
            logger.warning(f"⚠️ Файл миграции не найден: {migration_2_path}")
        
        logger.info("✅ Все миграции применены успешно")
        
    except Exception as error:
        logger.error(f"❌ Ошибка при применении миграций: {error}")
        logger.exception("Детали ошибки:")
        sys.exit(1)
    finally:
        try:
            db_manager.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    apply_migration()
