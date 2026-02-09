"""
MODULE: scripts.create_deal_chat_table
RESPONSIBILITY: Creating the deal_chat table in the database.
ALLOWED: sys, pathlib, core.tender_database, config.settings, loguru.
FORBIDDEN: None.
ERRORS: None.

Скрипт для создания таблицы deal_chat в базе данных tender_monitor.
Запустите этот скрипт, если таблица не существует.
"""

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.tender_database import TenderDatabaseManager
from config.settings import config
from loguru import logger


def create_deal_chat_table():
    """Создание таблицы deal_chat в базе данных."""
    try:
        db_manager = TenderDatabaseManager(config.tender_database)
        
        # Читаем SQL из файла
        sql_file = project_root / "scripts" / "sql_queries" / "add_deal_chat_table.sql"
        
        if not sql_file.exists():
            logger.error(f"Файл SQL миграции не найден: {sql_file}")
            return False
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Разбиваем скрипт на отдельные команды (убираем BEGIN/COMMIT, выполняем по отдельности)
        commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip() and not cmd.strip().upper().startswith('BEGIN') and not cmd.strip().upper().startswith('COMMIT')]
        
        # Выполняем SQL команды по отдельности
        logger.info("Создание таблицы deal_chat...")
        for cmd in commands:
            if cmd:
                try:
                    db_manager.execute_query(cmd, ())
                    logger.debug(f"Выполнена команда: {cmd[:50]}...")
                except Exception as e:
                    # Игнорируем ошибки "уже существует" для CREATE TABLE IF NOT EXISTS и CREATE INDEX IF NOT EXISTS
                    if "already exists" not in str(e).lower() and "does not exist" not in str(e).lower():
                        logger.warning(f"Предупреждение при выполнении команды: {e}")
                    else:
                        logger.debug(f"Команда уже выполнена (пропуск): {cmd[:50]}...")
        
        logger.info("✅ Таблица deal_chat успешно создана!")
        return True
        
    except Exception as exc:
        logger.error(f"❌ Ошибка при создании таблицы deal_chat: {exc}", exc_info=True)
        return False


if __name__ == "__main__":
    print("Создание таблицы deal_chat...")
    success = create_deal_chat_table()
    if success:
        print("✅ Готово! Таблица создана.")
        sys.exit(0)
    else:
        print("❌ Ошибка при создании таблицы. Проверьте логи.")
        sys.exit(1)

