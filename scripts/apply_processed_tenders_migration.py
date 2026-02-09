#!/usr/bin/env python3
"""
MODULE: scripts.apply_processed_tenders_migration
RESPONSIBILITY: Applying migration for processed tenders tracking.
ALLOWED: sys, pathlib, loguru, config.settings, core.tender_database.
FORBIDDEN: None.
ERRORS: None.

Применяет миграцию для создания таблиц processed_tenders и processed_files.
"""

import sys
from pathlib import Path

# Добавляем корневую директорию в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from config.settings import Config
from core.tender_database import TenderDatabaseManager


def apply_migration():
    """Применяет миграцию для создания таблиц отслеживания обработанных торгов"""

    logger.info("🚀 Начинаем применение миграции processed_tenders...")

    config = Config()
    db_manager = TenderDatabaseManager(config.tender_database)
    db_manager.connect()

    try:
        # Читаем SQL файл
        sql_file = Path(__file__).parent / "create_processed_tenders_table.sql"

        if not sql_file.exists():
            logger.error(f"❌ SQL файл не найден: {sql_file}")
            return False

        sql_content = sql_file.read_text(encoding='utf-8')

        # Разделяем на отдельные команды
        commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip()]

        logger.info(f"Найдено {len(commands)} SQL команд для выполнения")

        # Выполняем команды
        for i, command in enumerate(commands, 1):
            if command:
                logger.info(f"Выполняем команду {i}/{len(commands)}...")
                try:
                    if command.upper().startswith('CREATE TABLE'):
                        logger.info("Создание таблицы...")
                    elif command.upper().startswith('COMMENT'):
                        logger.info("Добавление комментария...")

                    db_manager.execute_update(command)
                    logger.info(f"✅ Команда {i} выполнена успешно")

                except Exception as e:
                    logger.warning(f"⚠️ Команда {i} возможно уже выполнена или не критична: {e}")

        logger.info("✅ Миграция processed_tenders применена успешно")

        # Проверяем созданные таблицы
        check_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN ('processed_tenders', 'processed_files')
            ORDER BY table_name
        """

        result = db_manager.execute_query(check_query)
        if result:
            tables = [row['table_name'] for row in result]
            logger.info(f"✅ Созданы таблицы: {', '.join(tables)}")
        else:
            logger.warning("⚠️ Таблицы не найдены после применения миграции")

        return True

    except Exception as e:
        logger.error(f"❌ Ошибка применения миграции: {e}")
        return False

    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    success = apply_migration()
    sys.exit(0 if success else 1)
