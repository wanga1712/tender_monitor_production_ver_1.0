"""
MODULE: scripts.apply_folder_name_migration_final
RESPONSIBILITY: Robust application of folder_name migration handling locks and idle transactions.
ALLOWED: sys, pathlib, psycopg2, psycopg2.extensions, config.settings, loguru, time.
FORBIDDEN: None.
ERRORS: None.

Применение миграции folder_name с завершением зависших транзакций.
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


def terminate_idle_transactions(conn):
    """Завершает зависшие транзакции (idle in transaction)."""
    with conn.cursor() as cursor:
        # Находим зависшие транзакции
        cursor.execute("""
            SELECT pid, query_start, state_change
            FROM pg_stat_activity
            WHERE datname = current_database()
            AND state = 'idle in transaction'
            AND pid != pg_backend_pid()
        """)
        
        idle_transactions = cursor.fetchall()
        
        if idle_transactions:
            logger.warning(f"⚠️  Найдено {len(idle_transactions)} зависших транзакций")
            for pid, query_start, state_change in idle_transactions:
                logger.info(f"  Завершаем транзакцию PID {pid} (начата: {query_start})")
                try:
                    cursor.execute(f"SELECT pg_terminate_backend({pid})")
                    logger.info(f"  ✅ Транзакция PID {pid} завершена")
                except Exception as e:
                    logger.warning(f"  ⚠️  Не удалось завершить PID {pid}: {e}")
        else:
            logger.info("✅ Зависших транзакций не найдено")


def wait_for_locks(conn, max_wait_seconds=30):
    """Ожидает освобождения блокировок на таблице."""
    import time
    
    logger.info("Ожидание освобождения блокировок на таблице tender_document_matches...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM pg_locks 
                WHERE relation = 'tender_document_matches'::regclass
                AND mode = 'AccessExclusiveLock'
                AND NOT granted
            """)
            
            waiting_locks = cursor.fetchone()[0]
            
            if waiting_locks == 0:
                logger.info("✅ Блокировки освобождены")
                return True
            
            logger.info(f"  Ожидаем... осталось блокировок: {waiting_locks}")
            time.sleep(2)
    
    logger.warning(f"⚠️  Блокировки не освободились за {max_wait_seconds} секунд")
    return False


def apply_migration(conn):
    """Применяет миграцию."""
    with conn.cursor() as cursor:
        # Проверяем, существует ли поле
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
            return True
        
        logger.info("Применение миграции...")
        
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
        
        return True


def main():
    """Основная функция."""
    logger.info("=" * 80)
    logger.info("Применение миграции folder_name (с обработкой блокировок)")
    logger.info("=" * 80)
    
    db_config = config.tender_database
    
    try:
        # Подключаемся с autocommit для DDL
        conn = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        logger.info("✅ Подключение к БД установлено")
        
        # Завершаем зависшие транзакции
        terminate_idle_transactions(conn)
        
        # Ждем освобождения блокировок
        if not wait_for_locks(conn, max_wait_seconds=10):
            logger.error("❌ Не удалось получить блокировку на таблицу")
            logger.error("   Закройте все открытые соединения к БД (DBeaver, приложение и т.д.)")
            logger.error("   Затем выполните миграцию вручную через psql или DBeaver")
            return False
        
        # Применяем миграцию
        if apply_migration(conn):
            logger.info("✅ Миграция применена успешно!")
            return True
        else:
            logger.error("❌ Ошибка при применении миграции")
            return False
        
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        return False
    except psycopg2.Error as e:
        logger.error(f"❌ Ошибка выполнения SQL: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

