import os
import sys
from pathlib import Path

# Добавляем корень проекта в sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from database_work.database_connection import DatabaseManager
from utils.logger_config import get_logger

logger = get_logger()

def apply_migration():
    db_manager = DatabaseManager()
    sql_file = Path(__file__).parent / "create_doc_processing_tables.sql"
    
    if not sql_file.exists():
        logger.error(f"Файл миграции не найден: {sql_file}")
        return

    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_script = f.read()
        
        with db_manager.get_cursor() as cursor:
            cursor.execute(sql_script)
            db_manager.connection.commit()
            logger.info("Миграция таблиц обработки документов успешно применена.")
            print("✅ Миграция успешно применена.")
            
    except Exception as e:
        logger.error(f"Ошибка при применении миграции: {e}")
        db_manager.connection.rollback()
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    apply_migration()
