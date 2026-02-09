"""
MODULE: scripts.apply_folder_name_migration_direct
RESPONSIBILITY: Direct application of folder_name migration using raw psycopg2 connection.
ALLOWED: sys, pathlib, psycopg2, psycopg2.extensions, config.settings, loguru.
FORBIDDEN: None.
ERRORS: None.

Прямое применение миграции folder_name через psycopg2.

Этот скрипт использует прямое подключение к БД, минуя TenderDatabaseManager,
чтобы избежать возможных проблем с блокировками.
"""

import sys
from pathlib import Path
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import config
from loguru import logger


def main():
    """Основная функция."""
    logger.info("=" * 80)
    logger.info("Применение миграции folder_name в БД (прямое подключение)")
    logger.info("=" * 80)
    
    # Получаем параметры подключения
    db_config = config.tender_database
    logger.info(f"Подключение к БД: {db_config.host}:{db_config.port}/{db_config.database}")
    
    try:
        # Прямое подключение к БД
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password
        )
        
        # Устанавливаем autocommit для DDL команд
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        logger.info("✅ Подключение к БД установлено")
        
        with conn.cursor() as cursor:
            # Проверяем, существует ли поле
            logger.info("Проверка существования поля folder_name...")
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'tender_document_matches' 
                    AND column_name = 'folder_name'
                )
            """)
            has_field = cursor.fetchone()[0]
            
            if has_field:
                logger.info("✅ Поле folder_name уже существует")
                logger.info("Миграция не требуется")
            else:
                logger.info("Поле folder_name не найдено, применяем миграцию...")
                
                # 1. Добавляем поле
                logger.info("Шаг 1: Добавление поля folder_name...")
                cursor.execute("""
                    ALTER TABLE tender_document_matches 
                    ADD COLUMN IF NOT EXISTS folder_name VARCHAR(255)
                """)
                logger.info("✅ Поле folder_name добавлено")
                
                # 2. Создаем индекс
                logger.info("Шаг 2: Создание индекса...")
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tender_matches_folder_name 
                    ON tender_document_matches(folder_name) 
                    WHERE folder_name IS NOT NULL
                """)
                logger.info("✅ Индекс создан")
                
                # 3. Добавляем комментарий
                logger.info("Шаг 3: Добавление комментария...")
                cursor.execute("""
                    COMMENT ON COLUMN tender_document_matches.folder_name IS 
                    'Название папки с файлами закупки (например: 44fz_12345 или 44fz_12345_won). Используется для предотвращения повторной обработки файлов.'
                """)
                logger.info("✅ Комментарий добавлен")
                
                logger.info("✅ Миграция применена успешно!")
        
        conn.close()
        logger.info("✅ Готово!")
        
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        sys.exit(1)
    except psycopg2.Error as e:
        logger.error(f"❌ Ошибка выполнения SQL: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()

