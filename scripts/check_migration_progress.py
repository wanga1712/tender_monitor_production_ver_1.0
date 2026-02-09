"""
MODULE: scripts.check_migration_progress
RESPONSIBILITY: Monitoring progress of the active migration.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv, loguru, time, sys.
FORBIDDEN: None.
ERRORS: None.

Скрипт для проверки прогресса миграции статусов

Запустите в отдельном терминале, пока основная миграция выполняется.
Показывает:
- Сколько записей уже обновлено
- Сколько осталось
- Активные запросы в БД
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
        port=port
    )


def check_progress():
    """Проверка прогресса миграции"""
    conn = get_tender_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        print("\n" + "=" * 60)
        print("Проверка прогресса миграции статусов")
        print("=" * 60)
        
        # Проверка активных запросов
        cursor.execute("""
            SELECT 
                pid,
                state,
                query_start,
                now() - query_start as duration,
                LEFT(query, 80) as query_preview
            FROM pg_stat_activity
            WHERE state != 'idle'
              AND query NOT LIKE '%pg_stat_activity%'
            ORDER BY query_start
        """)
        
        active_queries = cursor.fetchall()
        if active_queries:
            print("\n📊 Активные запросы в БД:")
            for q in active_queries:
                print(f"  PID {q['pid']}: {q['state']} (длительность: {q['duration']})")
                print(f"    {q['query_preview']}...")
        else:
            print("\n⚠️  Нет активных запросов (миграция может быть завершена)")
        
        # Прогресс для 44ФЗ
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN status_id IS NOT NULL THEN 1 END) as with_status,
                COUNT(CASE WHEN status_id IS NULL THEN 1 END) as without_status,
                COUNT(CASE WHEN status_id = 1 THEN 1 END) as status_new,
                COUNT(CASE WHEN status_id = 2 THEN 1 END) as status_commission,
                COUNT(CASE WHEN status_id = 3 THEN 1 END) as status_won,
                COUNT(CASE WHEN status_id = 4 THEN 1 END) as status_bad
            FROM reestr_contract_44_fz
        """)
        
        stats_44fz = cursor.fetchone()
        if stats_44fz:
            total = stats_44fz['total']
            with_status = stats_44fz['with_status']
            progress_pct = (with_status / total * 100) if total > 0 else 0
            
            print(f"\n📈 Прогресс для reestr_contract_44_fz:")
            print(f"  Всего записей: {total:,}")
            print(f"  С статусом: {with_status:,} ({progress_pct:.1f}%)")
            print(f"  Без статуса: {stats_44fz['without_status']:,}")
            print(f"  └─ Новая: {stats_44fz['status_new']:,}")
            print(f"  └─ Работа комиссии: {stats_44fz['status_commission']:,}")
            print(f"  └─ Разыграна: {stats_44fz['status_won']:,}")
            print(f"  └─ Плохие: {stats_44fz['status_bad']:,}")
        
        # Прогресс для 223ФЗ
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN status_id IS NOT NULL THEN 1 END) as with_status,
                COUNT(CASE WHEN status_id IS NULL THEN 1 END) as without_status,
                COUNT(CASE WHEN status_id = 4 THEN 1 END) as status_bad
            FROM reestr_contract_223_fz
        """)
        
        stats_223fz = cursor.fetchone()
        if stats_223fz:
            total = stats_223fz['total']
            with_status = stats_223fz['with_status']
            progress_pct = (with_status / total * 100) if total > 0 else 0
            
            print(f"\n📈 Прогресс для reestr_contract_223_fz:")
            print(f"  Всего записей: {total:,}")
            print(f"  С статусом: {with_status:,} ({progress_pct:.1f}%)")
            print(f"  Без статуса: {stats_223fz['without_status']:,}")
            print(f"  └─ Плохие: {stats_223fz['status_bad']:,}")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--watch":
        # Режим мониторинга (обновление каждые 5 секунд)
        print("Режим мониторинга (Ctrl+C для выхода)")
        try:
            while True:
                check_progress()
                print("\n⏳ Ожидание 5 секунд...")
                time.sleep(5)
                print("\n" * 2)
        except KeyboardInterrupt:
            print("\n\nМониторинг остановлен")
    else:
        # Однократная проверка
        check_progress()

