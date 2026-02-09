"""
MODULE: scripts.check_migration_progress_fast
RESPONSIBILITY: Fast migration progress check using sampling.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv, loguru, time, sys, traceback.
FORBIDDEN: None.
ERRORS: None.

Быстрая проверка прогресса миграции (без COUNT на всех записях)

Использует приблизительные подсчеты и выборки для быстрой проверки.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from loguru import logger
import time

load_dotenv()


def get_tender_db_connection():
    """Получение подключения к базе данных tender_monitor"""
    host = os.getenv("TENDER_MONITOR_DB_HOST")
    database = os.getenv("TENDER_MONITOR_DB_DATABASE")
    user = os.getenv("TENDER_MONITOR_DB_USER")
    password = os.getenv("TENDER_MONITOR_DB_PASSWORD")
    port = os.getenv("TENDER_MONITOR_DB_PORT", "5432")
    
    return psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
        connect_timeout=5  # Таймаут подключения
    )


def check_progress_fast():
    """Быстрая проверка прогресса (без полного COUNT)"""
    conn = get_tender_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        print("\n" + "=" * 60)
        print("Быстрая проверка прогресса миграции")
        print("=" * 60)
        
        # Проверка активных запросов (быстро)
        print("\n📊 Активные запросы:")
        cursor.execute("""
            SELECT 
                pid,
                state,
                query_start,
                now() - query_start as duration,
                LEFT(query, 60) as query_preview
            FROM pg_stat_activity
            WHERE state != 'idle'
              AND query NOT LIKE '%pg_stat_activity%'
            ORDER BY query_start
            LIMIT 5
        """)
        
        active_queries = cursor.fetchall()
        if active_queries:
            for q in active_queries:
                print(f"  PID {q['pid']}: {q['state']} (длительность: {q['duration']})")
                if q['query_preview']:
                    print(f"    {q['query_preview']}...")
        else:
            print("  ⚠️  Нет активных запросов")
        
        # БЫСТРАЯ проверка прогресса через приблизительный подсчет
        # Используем выборку вместо полного COUNT
        print("\n📈 Приблизительный прогресс (выборка 10,000 записей):")
        
        # Для 44ФЗ - проверяем выборку
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE status_id IS NOT NULL) as with_status,
                COUNT(*) FILTER (WHERE status_id IS NULL) as without_status,
                COUNT(*) FILTER (WHERE status_id = 1) as status_new,
                COUNT(*) FILTER (WHERE status_id = 2) as status_commission,
                COUNT(*) FILTER (WHERE status_id = 3) as status_won,
                COUNT(*) FILTER (WHERE status_id = 4) as status_bad
            FROM (
                SELECT status_id 
                FROM reestr_contract_44_fz 
                TABLESAMPLE SYSTEM (0.1)  -- 0.1% выборка (быстро!)
                LIMIT 10000
            ) sample
        """)
        
        sample_44fz = cursor.fetchone()
        if sample_44fz:
            total_sample = sample_44fz['with_status'] + sample_44fz['without_status']
            if total_sample > 0:
                progress_pct = (sample_44fz['with_status'] / total_sample * 100)
                print(f"  reestr_contract_44_fz (выборка):")
                print(f"    С статусом: ~{progress_pct:.1f}% (приблизительно)")
                print(f"    └─ Новая: {sample_44fz['status_new']}")
                print(f"    └─ Работа комиссии: {sample_44fz['status_commission']}")
                print(f"    └─ Разыграна: {sample_44fz['status_won']}")
                print(f"    └─ Плохие: {sample_44fz['status_bad']}")
        
        # Для 223ФЗ
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE status_id IS NOT NULL) as with_status,
                COUNT(*) FILTER (WHERE status_id IS NULL) as without_status,
                COUNT(*) FILTER (WHERE status_id = 4) as status_bad
            FROM (
                SELECT status_id 
                FROM reestr_contract_223_fz 
                TABLESAMPLE SYSTEM (0.1)
                LIMIT 10000
            ) sample
        """)
        
        sample_223fz = cursor.fetchone()
        if sample_223fz:
            total_sample = sample_223fz['with_status'] + sample_223fz['without_status']
            if total_sample > 0:
                progress_pct = (sample_223fz['with_status'] / total_sample * 100)
                print(f"  reestr_contract_223_fz (выборка):")
                print(f"    С статусом: ~{progress_pct:.1f}% (приблизительно)")
                print(f"    └─ Плохие: {sample_223fz['status_bad']}")
        
        # Проверка блокировок (быстро)
        print("\n🔒 Блокировки таблиц:")
        cursor.execute("""
            SELECT 
                locktype,
                relation::regclass as table_name,
                mode,
                granted
            FROM pg_locks
            WHERE relation::regclass::text IN ('reestr_contract_44_fz', 'reestr_contract_223_fz')
            LIMIT 10
        """)
        
        locks = cursor.fetchall()
        if locks:
            for lock in locks:
                print(f"  {lock['table_name']}: {lock['mode']} ({'granted' if lock['granted'] else 'waiting'})")
        else:
            print("  Нет блокировок")
        
        # Проверка через pg_stat_user_tables (быстро, использует статистику)
        print("\n📊 Статистика из системных таблиц:")
        cursor.execute("""
            SELECT 
                schemaname,
                tablename,
                n_tup_upd as updates,
                last_vacuum,
                last_autovacuum
            FROM pg_stat_user_tables
            WHERE tablename IN ('reestr_contract_44_fz', 'reestr_contract_223_fz')
        """)
        
        stats = cursor.fetchall()
        for stat in stats:
            print(f"  {stat['tablename']}: обновлений с момента последнего VACUUM: {stat['updates']:,}")
        
        print("\n" + "=" * 60)
        print("💡 Совет: Если миграция висит более 1 часа, используйте версию с батчами")
        print("   python scripts/apply_tender_statuses_migration_batched.py")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--watch":
        print("Режим мониторинга (Ctrl+C для выхода)")
        try:
            while True:
                check_progress_fast()
                print("\n⏳ Ожидание 10 секунд...")
                time.sleep(10)
                print("\n" * 2)
        except KeyboardInterrupt:
            print("\n\nМониторинг остановлен")
    else:
        check_progress_fast()

