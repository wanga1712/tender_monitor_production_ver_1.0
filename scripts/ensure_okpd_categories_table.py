"""
MODULE: scripts.ensure_okpd_categories_table
RESPONSIBILITY: Checking and creating okpd_categories table and category_id column.
ALLOWED: sys, pathlib, loguru, config.settings, core.tender_database, core.exceptions.
FORBIDDEN: None.
ERRORS: None.

Скрипт для проверки и создания таблицы okpd_categories и модификации okpd_from_users.

Проверяет существование таблицы okpd_categories и поля category_id в okpd_from_users,
и создает их, если они не существуют.
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


def column_exists(db_manager: TenderDatabaseManager, table_name: str, column_name: str) -> bool:
    """Проверяет существование колонки в таблице."""
    try:
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                AND column_name = %s
            )
        """
        result = db_manager.execute_query(query, (table_name, column_name))
        return result[0].get('exists', False) if result else False
    except Exception as error:
        logger.error(f"Ошибка при проверке существования колонки {column_name} в таблице {table_name}: {error}")
        return False


def execute_sql_file(db_manager: TenderDatabaseManager, sql_file: Path) -> bool:
    """Выполняет SQL скрипт из файла."""
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Разбиваем SQL на отдельные команды
        commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip() and not cmd.strip().startswith('--')]
        
        for command in commands:
            if command:
                db_manager.execute_query(command)
        
        logger.info(f"✅ SQL скрипт {sql_file.name} успешно выполнен")
        return True
    except Exception as error:
        logger.error(f"❌ Ошибка при выполнении SQL скрипта {sql_file.name}: {error}")
        return False


def main():
    """Основная функция."""
    logger.info("Проверка и создание таблицы okpd_categories...")
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
        sql_file = Path(__file__).parent / "create_okpd_categories_table.sql"
        
        # Сначала создаем таблицу okpd_categories, если её нет
        if not table_exists(tender_db, "okpd_categories"):
            logger.info("Таблица okpd_categories не найдена, создаю...")
            create_table_query = """
                CREATE TABLE okpd_categories (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_user_category_name UNIQUE (user_id, name)
                );
            """
            try:
                tender_db.execute_query(create_table_query)
                logger.info("✅ Таблица okpd_categories успешно создана")
            except Exception as error:
                logger.error(f"❌ Ошибка при создании таблицы okpd_categories: {error}")
                sys.exit(1)
        else:
            logger.info("✅ Таблица okpd_categories уже существует")
        
        # Создаем индексы для okpd_categories
        try:
            index_query = "CREATE INDEX IF NOT EXISTS idx_okpd_categories_user_id ON okpd_categories(user_id);"
            tender_db.execute_query(index_query)
        except Exception as error:
            logger.warning(f"Предупреждение при создании индекса для okpd_categories: {error}")
        
        # Теперь добавляем поле category_id в okpd_from_users, если его нет
        try:
            if not column_exists(tender_db, "okpd_from_users", "category_id"):
                logger.info("Поле category_id в таблице okpd_from_users не найдено, добавляю...")
                alter_query = """
                    ALTER TABLE okpd_from_users 
                    ADD COLUMN category_id INTEGER REFERENCES okpd_categories(id) ON DELETE SET NULL;
                """
                tender_db.execute_query(alter_query)
                logger.info("✅ Поле category_id успешно добавлено в okpd_from_users")
            else:
                logger.info("✅ Поле category_id уже существует в okpd_from_users")
        except Exception as error:
            logger.error(f"❌ Ошибка при добавлении поля category_id: {error}")
            # Если ошибка, но поле может уже существовать, продолжаем
            logger.warning("Продолжаем выполнение, возможно поле уже существует")
        
        # Создаем индекс для category_id
        try:
            index_query = "CREATE INDEX IF NOT EXISTS idx_okpd_from_users_category_id ON okpd_from_users(category_id);"
            tender_db.execute_query(index_query)
        except Exception as error:
            logger.warning(f"Предупреждение при создании индекса для category_id: {error}")
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ Все необходимые таблицы и поля проверены и готовы к использованию")
        
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

