"""
MODULE: scripts.quick_check
RESPONSIBILITY: Fast check of migration status.
ALLOWED: psycopg2, os, dotenv.
FORBIDDEN: None.
ERRORS: None.

Быстрая проверка состояния миграции
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv("TENDER_MONITOR_DB_HOST"),
        database=os.getenv("TENDER_MONITOR_DB_DATABASE"),
        user=os.getenv("TENDER_MONITOR_DB_USER"),
        password=os.getenv("TENDER_MONITOR_DB_PASSWORD"),
        port=os.getenv("TENDER_MONITOR_DB_PORT", "5432"),
        connect_timeout=3
    )
    cursor = conn.cursor()
    
    # Быстрая проверка активных запросов
    cursor.execute("""
        SELECT COUNT(*) 
        FROM pg_stat_activity 
        WHERE state != 'idle' 
          AND query NOT LIKE '%pg_stat_activity%'
    """)
    active = cursor.fetchone()[0]
    print(f"Активных запросов: {active}")
    
    # Быстрая проверка блокировок
    cursor.execute("""
        SELECT COUNT(*) 
        FROM pg_locks 
        WHERE relation::regclass::text IN ('reestr_contract_44_fz', 'reestr_contract_223_fz')
    """)
    locks = cursor.fetchone()[0]
    print(f"Блокировок на таблицах: {locks}")
    
    # Очень быстрая проверка прогресса (только первые 1000 записей)
    cursor.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE status_id IS NOT NULL) as with_status,
            COUNT(*) FILTER (WHERE status_id IS NULL) as without_status
        FROM reestr_contract_44_fz 
        LIMIT 1000
    """)
    sample = cursor.fetchone()
    if sample[0] + sample[1] > 0:
        pct = (sample[0] / (sample[0] + sample[1]) * 100)
        print(f"Прогресс (выборка 1000): {pct:.1f}% имеют статус")
    
    cursor.close()
    conn.close()
    print("✅ Подключение работает")
    
except Exception as e:
    print(f"❌ Ошибка: {e}")

