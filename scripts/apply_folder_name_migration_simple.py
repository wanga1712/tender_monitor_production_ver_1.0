"""
MODULE: scripts.apply_folder_name_migration_simple
RESPONSIBILITY: Simplified prompt-based application of folder_name migration.
ALLOWED: sys, pathlib, config.settings, core.tender_database, loguru.
FORBIDDEN: None.
ERRORS: None.

Упрощенный скрипт для применения миграции folder_name.

Если скрипт зависает, выполните SQL команды вручную через psql или pgAdmin.
"""

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import config
from core.tender_database import TenderDatabaseManager
from loguru import logger


def main():
    """Основная функция."""
    logger.info("=" * 80)
    logger.info("Применение миграции folder_name в БД (упрощенная версия)")
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
        check_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'tender_document_matches' 
                AND column_name = 'folder_name'
            )
        """
        result = tender_db.execute_query(check_query)
        has_field = result[0].get('exists', False) if result else False
        
        if has_field:
            logger.info("✅ Поле folder_name уже существует в таблице tender_document_matches")
            logger.info("Миграция не требуется")
        else:
            logger.warning("⚠️  Поле folder_name не найдено")
            logger.info("")
            logger.info("Выполните следующие SQL команды вручную через psql или pgAdmin:")
            logger.info("")
            logger.info("-" * 80)
            logger.info("ALTER TABLE tender_document_matches ADD COLUMN IF NOT EXISTS folder_name VARCHAR(255);")
            logger.info("")
            logger.info("CREATE INDEX IF NOT EXISTS idx_tender_matches_folder_name")
            logger.info("ON tender_document_matches(folder_name) WHERE folder_name IS NOT NULL;")
            logger.info("")
            logger.info("COMMENT ON COLUMN tender_document_matches.folder_name IS")
            logger.info("'Название папки с файлами закупки (например: 44fz_12345 или 44fz_12345_won). Используется для предотвращения повторной обработки файлов.';")
            logger.info("-" * 80)
            logger.info("")
            logger.info("Или выполните файл: scripts/add_folder_name_to_tender_document_matches.sql")
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        tender_db.disconnect()
        logger.info("✅ Готово!")


if __name__ == "__main__":
    main()

