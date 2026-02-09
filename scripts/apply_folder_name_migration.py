"""
MODULE: scripts.apply_folder_name_migration
RESPONSIBILITY: Applying folder_name migration to database.
ALLOWED: sys, pathlib, config.settings, core.tender_database, loguru.
FORBIDDEN: None.
ERRORS: None.

Скрипт для применения миграции folder_name в БД.

Автоматически применяет SQL миграцию для создания поля folder_name
в таблице tender_document_matches.
"""

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import config
from core.tender_database import TenderDatabaseManager
from loguru import logger


def apply_migration(db_manager: TenderDatabaseManager) -> bool:
    """
    Применяет миграцию для создания поля folder_name.
    
    Args:
        db_manager: Менеджер БД
        
    Returns:
        True если миграция применена успешно, False в противном случае
    """
    try:
        logger.info("Применение миграции для создания поля folder_name...")
        
        # Используем прямой доступ к connection для DDL команд с таймаутом
        connection = db_manager._connection
        if not connection or connection.closed:
            raise Exception("Нет подключения к БД")
        
        with connection.cursor() as cursor:
            # Устанавливаем таймаут для команды (5 секунд)
            cursor.execute("SET statement_timeout = '5s'")
            connection.commit()
            
            # 1. Добавляем поле folder_name
            logger.info("Шаг 1: Добавление поля folder_name...")
            alter_table_sql = """
                ALTER TABLE tender_document_matches 
                ADD COLUMN IF NOT EXISTS folder_name VARCHAR(255)
            """
            cursor.execute(alter_table_sql)
            connection.commit()
            logger.info("✅ Поле folder_name добавлено")
            
            # 2. Создаем индекс
            logger.info("Шаг 2: Создание индекса...")
            create_index_sql = """
                CREATE INDEX IF NOT EXISTS idx_tender_matches_folder_name 
                ON tender_document_matches(folder_name) 
                WHERE folder_name IS NOT NULL
            """
            cursor.execute(create_index_sql)
            connection.commit()
            logger.info("✅ Индекс создан")
            
            # 3. Добавляем комментарий
            logger.info("Шаг 3: Добавление комментария...")
            comment_sql = """
                COMMENT ON COLUMN tender_document_matches.folder_name IS 
                'Название папки с файлами закупки (например: 44fz_12345 или 44fz_12345_won). Используется для предотвращения повторной обработки файлов.'
            """
            cursor.execute(comment_sql)
            connection.commit()
            logger.info("✅ Комментарий добавлен")
            
            # Сбрасываем таймаут
            cursor.execute("SET statement_timeout = DEFAULT")
            connection.commit()
        
        logger.info("✅ Миграция применена успешно")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка при применении миграции: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # Пытаемся сбросить таймаут
        try:
            connection = db_manager._connection
            if connection and not connection.closed:
                with connection.cursor() as cursor:
                    cursor.execute("SET statement_timeout = DEFAULT")
                    connection.commit()
        except:
            pass
        return False


def check_column_exists(db_manager: TenderDatabaseManager) -> bool:
    """
    Проверяет, существует ли поле folder_name в таблице.
    
    Args:
        db_manager: Менеджер БД
        
    Returns:
        True если поле существует, False в противном случае
    """
    try:
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'tender_document_matches' 
                AND column_name = 'folder_name'
            )
        """
        result = db_manager.execute_query(query)
        return result[0].get('exists', False) if result else False
    except Exception as e:
        logger.error(f"Ошибка при проверке существования поля: {e}")
        return False


def main():
    """Основная функция."""
    logger.info("=" * 80)
    logger.info("Применение миграции folder_name в БД")
    logger.info("=" * 80)
    
    # Подключаемся к БД
    try:
        tender_db = TenderDatabaseManager(config.tender_database)
        tender_db.connect()
        logger.info("✅ Подключение к БД установлено")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        sys.exit(1)
    
    try:
        # Проверяем, существует ли поле
        if check_column_exists(tender_db):
            logger.info("✅ Поле folder_name уже существует в таблице tender_document_matches")
            logger.info("Миграция не требуется")
        else:
            logger.info("Поле folder_name не найдено, применяем миграцию...")
            if apply_migration(tender_db):
                logger.info("")
                logger.info("=" * 80)
                logger.info("✅ Миграция применена успешно!")
                logger.info("Теперь можно запустить: python scripts/migrate_folder_names_to_db.py")
                logger.info("=" * 80)
            else:
                logger.error("❌ Не удалось применить миграцию")
                sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
    finally:
        tender_db.disconnect()
        logger.info("✅ Готово!")


if __name__ == "__main__":
    main()

