#!/usr/bin/env python3
"""Разовая очистка дубликатов - удаление уже перенесенных контрактов из основной таблицы"""
import os
import sys
from pathlib import Path
import time

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent))

from database_work.database_connection import DatabaseManager
from dotenv import load_dotenv
from loguru import logger

# Загружаем переменные окружения
env_path = Path(__file__).parent / "database_work" / "db_credintials.env"
load_dotenv(env_path)

def cleanup_duplicates():
    """Удаляет дубликаты - контракты, которые уже есть в completed, но еще в основной таблице"""
    db = DatabaseManager()
    try:
        logger.info("=" * 60)
        logger.info("ОЧИСТКА ДУБЛИКАТОВ - Удаление перенесенных контрактов")
        logger.info("=" * 60)
        
        # Получаем ID всех завершенных контрактов, которые есть в completed
        logger.info("Получаю список дубликатов...")
        db.cursor.execute("""
            SELECT r.id FROM reestr_contract_44_fz r
            INNER JOIN reestr_contract_44_fz_completed c ON r.id = c.id
            WHERE r.delivery_end_date IS NOT NULL 
            AND r.delivery_end_date < CURRENT_DATE
            ORDER BY r.id;
        """)
        ids_to_delete = [row[0] for row in db.cursor.fetchall()]
        
        total_count = len(ids_to_delete)
        logger.info(f"Найдено дубликатов для удаления: {total_count:,}")
        
        if total_count == 0:
            logger.info("Дубликатов не найдено. Очистка не требуется.")
            return
        
        # Удаляем батчами по 10 (меньше для избежания блокировок)
        delete_batch_size = 10
        total_deleted = 0
        total_batches = (total_count + delete_batch_size - 1) // delete_batch_size
        
        logger.info(f"Начинаю удаление батчами по {delete_batch_size} записей...")
        start_time = time.time()
        
        for i in range(0, total_count, delete_batch_size):
            delete_batch = ids_to_delete[i:i+delete_batch_size]
            batch_num = i // delete_batch_size + 1
            
            try:
                # Устанавливаем таймаут для запроса
                db.cursor.execute("SET statement_timeout = '30s'")
                
                # Проверяем блокировки перед удалением
                db.cursor.execute("""
                    SELECT count(*) FROM pg_locks 
                    WHERE relation = 'reestr_contract_44_fz'::regclass 
                    AND locktype = 'relation';
                """)
                lock_count = db.cursor.fetchone()[0]
                
                if lock_count > 20:
                    logger.warning(f"Батч {batch_num}: много блокировок ({lock_count}), жду 1 сек...")
                    time.sleep(1)
                
                ids_placeholder = ','.join(['%s'] * len(delete_batch))
                delete_query = f"""
                    DELETE FROM reestr_contract_44_fz
                    WHERE id IN ({ids_placeholder})
                """
                
                batch_start = time.time()
                db.cursor.execute(delete_query, delete_batch)
                batch_deleted = db.cursor.rowcount
                batch_duration = time.time() - batch_start
                
                total_deleted += batch_deleted
                db.connection.commit()
                
                # Сбрасываем таймаут
                db.cursor.execute("SET statement_timeout = '0'")
                
                # Логируем каждый 10-й батч или каждый батч в первых 100
                if batch_num % 10 == 0 or batch_num <= 100 or batch_num == total_batches:
                    elapsed = time.time() - start_time
                    rate = total_deleted / elapsed if elapsed > 0 else 0
                    remaining = total_count - total_deleted
                    eta = remaining / rate if rate > 0 else 0
                    logger.info(
                        f"Батч {batch_num}/{total_batches}: удалено {batch_deleted}, "
                        f"всего {total_deleted:,}/{total_count:,} "
                        f"({total_deleted*100/total_count:.1f}%), "
                        f"скорость: {rate:.0f} зап/сек, "
                        f"осталось ~{int(eta)} сек, "
                        f"время батча: {batch_duration:.2f}с"
                    )
                    
            except Exception as e:
                error_msg = str(e)
                db.connection.rollback()
                db.cursor.execute("SET statement_timeout = '0'")
                
                if "timeout" in error_msg.lower() or "statement_timeout" in error_msg.lower():
                    logger.warning(f"Таймаут батча {batch_num}, пропускаю...")
                else:
                    logger.error(f"Ошибка удаления батча {batch_num}: {error_msg[:200]}")
                continue
        
        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"ОЧИСТКА ЗАВЕРШЕНА")
        logger.info(f"Удалено: {total_deleted:,} из {total_count:,} дубликатов")
        logger.info(f"Время: {elapsed:.1f} сек ({elapsed/60:.1f} мин)")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        db.connection.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    cleanup_duplicates()
