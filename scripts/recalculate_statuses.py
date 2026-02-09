"""
MODULE: scripts.recalculate_statuses
RESPONSIBILITY: Recalculating tender statuses according to new rules.
ALLOWED: psycopg2, os, dotenv, sys, time, loguru.
FORBIDDEN: None.
ERRORS: None.

Пересчет статусов для всех записей в БД согласно новым правилам

Правила присвоения статусов:
1. НОВЫЕ (status_id = 1): end_date >= CURRENT_DATE
2. РАБОТА КОМИССИИ (status_id = 2): end_date < CURRENT_DATE 
   И end_date >= CURRENT_DATE - 90 дней 
   И delivery_end_date IS NULL
3. РАЗЫГРАННЫЕ (status_id = 3): delivery_end_date IS NOT NULL 
   И delivery_end_date >= CURRENT_DATE + 90 дней
4. ПЛОХИЕ (status_id = 4): все остальные, которые не подходят под условия выше

ВАЖНО: Все статусы перезаписываются, включая "Плохие" (status_id = 4)
"""

import psycopg2
import os
from dotenv import load_dotenv
import sys
import time
from loguru import logger

load_dotenv()

logger.add("logs/recalculate_statuses.log", rotation="10 MB", level="INFO")

BATCH_SIZE = 10000


def get_connection():
    """Получение подключения к БД"""
    return psycopg2.connect(
        host=os.getenv("TENDER_MONITOR_DB_HOST"),
        database=os.getenv("TENDER_MONITOR_DB_DATABASE"),
        user=os.getenv("TENDER_MONITOR_DB_USER"),
        password=os.getenv("TENDER_MONITOR_DB_PASSWORD"),
        port=os.getenv("TENDER_MONITOR_DB_PORT", "5432"),
        connect_timeout=10
    )


def recalculate_batch(cursor, table_name, status_id, condition, status_name):
    """Пересчет статусов батчами (включая уже обработанные)"""
    total_updated = 0
    batch_num = 0
    conn = cursor.connection
    
    print(f"\n🔄 Пересчет статуса '{status_name}' (status_id={status_id}) для {table_name}...")
    logger.info(f"Начало пересчета статуса '{status_name}' для {table_name}")
    sys.stdout.flush()
    
    start_time = time.time()
    
    while True:
        batch_num += 1
        
        # Обновляем только записи, которые соответствуют условию И НЕ имеют нужный статус
        # Это предотвращает зацикливание
        query = f"""
            WITH batch AS (
                SELECT id FROM {table_name}
                WHERE {condition}
                  AND (status_id IS NULL OR status_id != %s)
                ORDER BY id DESC
                LIMIT {BATCH_SIZE}
            )
            UPDATE {table_name} r
            SET status_id = %s
            FROM batch b
            WHERE r.id = b.id
            RETURNING r.id
        """
        
        batch_start = time.time()
        cursor.execute(query, (status_id, status_id))  # Передаем status_id дважды: для WHERE и для SET
        updated_ids = cursor.fetchall()
        updated = len(updated_ids)
        
        if updated == 0:
            break
        
        conn.commit()
        
        total_updated += updated
        elapsed = time.time() - batch_start
        elapsed_total = time.time() - start_time
        rate = updated / elapsed if elapsed > 0 else 0
        
        if updated_ids:
            last_id = min(row[0] for row in updated_ids)
        else:
            last_id = None
        
        print(
            f"  Батч #{batch_num}: обновлено {updated:,} записей "
            f"(всего: {total_updated:,}, время: {elapsed:.2f} сек, "
            f"скорость: {rate:,.0f} записей/сек, последний ID: {last_id})"
        )
        sys.stdout.flush()
        logger.info(
            f"Батч #{batch_num}: обновлено {updated:,} записей "
            f"(всего: {total_updated:,}, скорость: {rate:,.0f} записей/сек)"
        )
        
        if batch_num % 5 == 0:
            time.sleep(0.05)
    
    elapsed_total = time.time() - start_time
    print(f"✅ Статус '{status_name}' присвоен {total_updated:,} записям за {elapsed_total/60:.1f} минут")
    logger.info(f"Завершен пересчет статуса '{status_name}': {total_updated:,} записей за {elapsed_total/60:.1f} минут")
    return total_updated


def recalculate_statuses_44fz(cursor):
    """Пересчет статусов для 44ФЗ в правильном порядке"""
    print("\n" + "=" * 70)
    print("ПЕРЕСЧЕТ СТАТУСОВ ДЛЯ 44ФЗ")
    print("=" * 70)
    print("ВАЖНО: Порядок имеет значение!")
    print("Сначала проверяем более специфичные условия")
    print(f"Размер батча: {BATCH_SIZE:,} записей\n")
    sys.stdout.flush()
    
    total_updated = 0
    start_time_total = time.time()
    
    # 1. Разыграна (status_id = 3) - ПЕРВЫМ (самое специфичное условие)
    # delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + 90 дней
    total_updated += recalculate_batch(
        cursor, "reestr_contract_44_fz", 3,
        "delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'",
        "Разыграна"
    )
    
    # 2. Работа комиссии (status_id = 2) - ВТОРЫМ
    # end_date < CURRENT_DATE 
    # И end_date >= CURRENT_DATE - 90 дней
    # И delivery_end_date IS NULL
    # И НЕ имеет delivery_end_date >= CURRENT_DATE + 90 дней (уже обработано как "Разыграна")
    total_updated += recalculate_batch(
        cursor, "reestr_contract_44_fz", 2,
        """end_date IS NOT NULL 
           AND end_date < CURRENT_DATE 
           AND end_date >= CURRENT_DATE - INTERVAL '90 days'
           AND delivery_end_date IS NULL
           AND NOT (delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days')""",
        "Работа комиссии"
    )
    
    # 3. Новая (status_id = 1) - ТРЕТЬИМ
    # end_date >= CURRENT_DATE
    # И НЕ имеет delivery_end_date >= CURRENT_DATE + 90 дней (уже обработано как "Разыграна")
    total_updated += recalculate_batch(
        cursor, "reestr_contract_44_fz", 1,
        """end_date IS NOT NULL 
           AND end_date >= CURRENT_DATE
           AND NOT (delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days')""",
        "Новая"
    )
    
    # 4. Плохие (status_id = 4) - ПОСЛЕДНИМ (все остальные)
    # Все записи, которые не соответствуют ни одному из "хороших" статусов
    total_updated += recalculate_batch(
        cursor, "reestr_contract_44_fz", 4,
        """NOT (
               (delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days')
               OR (end_date IS NOT NULL 
                   AND end_date < CURRENT_DATE 
                   AND end_date >= CURRENT_DATE - INTERVAL '90 days'
                   AND delivery_end_date IS NULL)
               OR (end_date IS NOT NULL AND end_date >= CURRENT_DATE)
           )""",
        "Плохие"
    )
    
    elapsed_total = time.time() - start_time_total
    print(f"\n✅ Всего обновлено для 44ФЗ: {total_updated:,} записей за {elapsed_total/60:.1f} минут")
    logger.info(f"Пересчет 44ФЗ завершен: {total_updated:,} записей за {elapsed_total/60:.1f} минут")
    return total_updated


def recalculate_statuses_223fz(cursor):
    """Пересчет статусов для 223ФЗ в правильном порядке"""
    print("\n" + "=" * 70)
    print("ПЕРЕСЧЕТ СТАТУСОВ ДЛЯ 223ФЗ")
    print("=" * 70)
    print("ВАЖНО: Порядок имеет значение!")
    print("Сначала проверяем более специфичные условия")
    print(f"Размер батча: {BATCH_SIZE:,} записей\n")
    sys.stdout.flush()
    
    total_updated = 0
    start_time_total = time.time()
    
    # 1. Разыграна (status_id = 3) - ПЕРВЫМ (самое специфичное условие)
    # delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + 90 дней
    total_updated += recalculate_batch(
        cursor, "reestr_contract_223_fz", 3,
        "delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'",
        "Разыграна"
    )
    
    # 2. Работа комиссии (status_id = 2) - ВТОРЫМ
    # end_date < CURRENT_DATE 
    # И end_date >= CURRENT_DATE - 90 дней
    # И delivery_end_date IS NULL
    # И НЕ имеет delivery_end_date >= CURRENT_DATE + 90 дней (уже обработано как "Разыграна")
    total_updated += recalculate_batch(
        cursor, "reestr_contract_223_fz", 2,
        """end_date IS NOT NULL 
           AND end_date < CURRENT_DATE 
           AND end_date >= CURRENT_DATE - INTERVAL '90 days'
           AND delivery_end_date IS NULL
           AND NOT (delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days')""",
        "Работа комиссии"
    )
    
    # 3. Новая (status_id = 1) - ТРЕТЬИМ
    # end_date >= CURRENT_DATE
    # И НЕ имеет delivery_end_date >= CURRENT_DATE + 90 дней (уже обработано как "Разыграна")
    total_updated += recalculate_batch(
        cursor, "reestr_contract_223_fz", 1,
        """end_date IS NOT NULL 
           AND end_date >= CURRENT_DATE
           AND NOT (delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days')""",
        "Новая"
    )
    
    # 4. Плохие (status_id = 4) - ПОСЛЕДНИМ (все остальные)
    # Все записи, которые не соответствуют ни одному из "хороших" статусов
    total_updated += recalculate_batch(
        cursor, "reestr_contract_223_fz", 4,
        """NOT (
               (delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days')
               OR (end_date IS NOT NULL 
                   AND end_date < CURRENT_DATE 
                   AND end_date >= CURRENT_DATE - INTERVAL '90 days'
                   AND delivery_end_date IS NULL)
               OR (end_date IS NOT NULL AND end_date >= CURRENT_DATE)
           )""",
        "Плохие"
    )
    
    elapsed_total = time.time() - start_time_total
    print(f"\n✅ Всего обновлено для 223ФЗ: {total_updated:,} записей за {elapsed_total/60:.1f} минут")
    logger.info(f"Пересчет 223ФЗ завершен: {total_updated:,} записей за {elapsed_total/60:.1f} минут")
    return total_updated


def recalculate_statuses():
    """Пересчет статусов для всех реестров (44ФЗ и 223ФЗ)"""
    conn = get_connection()
    cursor = conn.cursor()
    conn.set_session(autocommit=False)
    
    try:
        print("=" * 70)
        print("ПОЛНЫЙ ПЕРЕСЧЕТ СТАТУСОВ ДЛЯ ВСЕХ ЗАПИСЕЙ")
        print("=" * 70)
        print("Этот скрипт перезапишет ВСЕ статусы согласно новым правилам")
        print("Включая статус 'Плохие' (status_id = 4)")
        print(f"Размер батча: {BATCH_SIZE:,} записей")
        print()
        sys.stdout.flush()
        
        total_start_time = time.time()
        
        # Пересчитываем статусы для 44ФЗ
        total_44fz = recalculate_statuses_44fz(cursor)
        
        # Пересчитываем статусы для 223ФЗ
        total_223fz = recalculate_statuses_223fz(cursor)
        
        total_elapsed = time.time() - total_start_time
        total_all = total_44fz + total_223fz
        
        print("\n" + "=" * 70)
        print("ИТОГИ ПЕРЕСЧЕТА")
        print("=" * 70)
        print(f"44ФЗ: {total_44fz:,} записей")
        print(f"223ФЗ: {total_223fz:,} записей")
        print(f"Всего: {total_all:,} записей")
        print(f"Время выполнения: {total_elapsed/60:.1f} минут")
        print("=" * 70)
        logger.info(f"Полный пересчет завершен: {total_all:,} записей (44ФЗ: {total_44fz:,}, 223ФЗ: {total_223fz:,}) за {total_elapsed/60:.1f} минут")
        
        cursor.close()
        conn.close()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем (Ctrl+C)")
        if conn:
            conn.rollback()
        logger.warning("Пересчет прерван пользователем")
        sys.exit(1)
    except Exception as e:
        error_msg = f"Ошибка: {e}"
        print(f"\n❌ {error_msg}")
        logger.error(error_msg, exc_info=True)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    recalculate_statuses()
