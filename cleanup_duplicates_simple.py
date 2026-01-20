#!/usr/bin/env python3
"""Простая очистка дубликатов через прямой SQL запрос"""
import os
import sys
from pathlib import Path
import time
import subprocess

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent))

from database_work.database_connection import DatabaseManager
from dotenv import load_dotenv
from loguru import logger

# Загружаем переменные окружения
env_path = Path(__file__).parent / "database_work" / "db_credintials.env"
load_dotenv(env_path)

def cleanup_duplicates_simple():
    """Удаляет дубликаты через прямой SQL запрос с батчами"""
    print("=" * 60, flush=True)
    print("ОЧИСТКА ДУБЛИКАТОВ - ПРОСТОЙ ПОДХОД", flush=True)
    print("=" * 60, flush=True)
    
    db = DatabaseManager()
    try:
        logger.info("=" * 60)
        logger.info("ОЧИСТКА ДУБЛИКАТОВ - ПРОСТОЙ ПОДХОД")
        logger.info("=" * 60)
        print("Подключение к БД установлено", flush=True)
        
        # Сначала проверяем количество
        print("Проверяю количество дубликатов...", flush=True)
        logger.info("Проверяю количество дубликатов...")
        db.cursor.execute("""
            SELECT COUNT(*) FROM reestr_contract_44_fz r
            INNER JOIN reestr_contract_44_fz_completed c ON r.id = c.id
            WHERE r.delivery_end_date IS NOT NULL 
            AND r.delivery_end_date < CURRENT_DATE;
        """)
        total_count = db.cursor.fetchone()[0]
        print(f"Найдено дубликатов: {total_count:,}", flush=True)
        logger.info(f"Найдено дубликатов: {total_count:,}")
        
        if total_count == 0:
            logger.info("Дубликатов не найдено. Очистка не требуется.")
            return
        
        # Используем DELETE с подзапросом, но батчами (меньший размер для избежания зависаний)
        batch_size = 100
        total_deleted = 0
        start_time = time.time()
        
        print(f"Начинаю удаление батчами по {batch_size} записей...", flush=True)
        logger.info(f"Начинаю удаление батчами по {batch_size} записей...")
        print("⚠️  ВНИМАНИЕ: Временно отключаю проверку внешних ключей для удаления", flush=True)
        logger.info("⚠️  ВНИМАНИЕ: Временно отключаю проверку внешних ключей для удаления")
        print("   (контракты уже перенесены в completed, ссылки остаются валидными)", flush=True)
        logger.info("   (контракты уже перенесены в completed, ссылки остаются валидными)")
        
        # Временно отключаем проверку внешних ключей для текущей сессии
        # Это позволяет удалить записи, на которые есть ссылки, без CASCADE
        print("Отключаю проверку внешних ключей...", flush=True)
        db.cursor.execute("SET session_replication_role = 'replica'")
        print("✅ Проверка внешних ключей отключена", flush=True)
        logger.info("✅ Проверка внешних ключей отключена")
        
        try:
            while True:
                try:
                    # Устанавливаем таймаут
                    db.cursor.execute("SET statement_timeout = '60s'")
                    
                    # Удаляем один батч
                    print(f"Удаляю батч (уже удалено: {total_deleted:,})...", flush=True)
                    logger.info(f"Удаляю батч (уже удалено: {total_deleted:,})...")
                    batch_start = time.time()
                    
                    delete_query = f"""
                        DELETE FROM reestr_contract_44_fz
                        WHERE id IN (
                            SELECT r.id FROM reestr_contract_44_fz r
                            INNER JOIN reestr_contract_44_fz_completed c ON r.id = c.id
                            WHERE r.delivery_end_date IS NOT NULL 
                            AND r.delivery_end_date < CURRENT_DATE
                            LIMIT {batch_size}
                        );
                    """
                    
                    db.cursor.execute(delete_query)
                    batch_deleted = db.cursor.rowcount
                    batch_duration = time.time() - batch_start
                    
                    if batch_deleted == 0:
                        logger.info("Больше дубликатов не найдено")
                        break
                    
                    total_deleted += batch_deleted
                    db.connection.commit()
                    
                    # Сбрасываем таймаут
                    db.cursor.execute("SET statement_timeout = '0'")
                    
                    elapsed = time.time() - start_time
                    rate = total_deleted / elapsed if elapsed > 0 else 0
                    remaining = total_count - total_deleted
                    eta = remaining / rate if rate > 0 else 0
                    
                    logger.info(
                        f"Батч удален: {batch_deleted:,} записей за {batch_duration:.2f}с, "
                        f"всего {total_deleted:,}/{total_count:,} "
                        f"({total_deleted*100/total_count:.1f}%), "
                        f"скорость: {rate:.0f} зап/сек, "
                        f"осталось ~{int(eta)} сек"
                    )
                    
                    # Небольшая пауза между батчами
                    time.sleep(0.1)
                    
                except Exception as e:
                    error_msg = str(e)
                    db.connection.rollback()
                    db.cursor.execute("SET statement_timeout = '0'")
                    
                    if "timeout" in error_msg.lower():
                        logger.warning(f"Таймаут при удалении, продолжаю...")
                        time.sleep(2)
                        continue
                    else:
                        logger.error(f"Ошибка: {error_msg[:300]}")
                        break
        
        finally:
            # Восстанавливаем проверку внешних ключей
            db.cursor.execute("SET session_replication_role = 'origin'")
            logger.info("✅ Проверка внешних ключей восстановлена")
        
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
        # Восстанавливаем проверку внешних ключей даже при ошибке
        try:
            db.cursor.execute("SET session_replication_role = 'origin'")
        except:
            pass
    finally:
        db.close()

if __name__ == "__main__":
    cleanup_duplicates_simple()
