"""
MODULE: scripts.check_db_locks
RESPONSIBILITY: Checking active database locks.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv.
FORBIDDEN: None.
ERRORS: None.

Проверка блокировок в БД
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

def check_locks():
    """Проверка активных блокировок"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("TENDER_MONITOR_DB_HOST"),
            database=os.getenv("TENDER_MONITOR_DB_DATABASE"),
            user=os.getenv("TENDER_MONITOR_DB_USER"),
            password=os.getenv("TENDER_MONITOR_DB_PASSWORD"),
            port=os.getenv("TENDER_MONITOR_DB_PORT", "5432"),
            connect_timeout=5
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=" * 70)
        print("ПРОВЕРКА БЛОКИРОВОК В БД")
        print("=" * 70)
        
        # Проверяем активные запросы
        cursor.execute("""
            SELECT 
                pid,
                state,
                now() - query_start as duration,
                wait_event_type,
                wait_event,
                LEFT(query, 100) as query_preview
            FROM pg_stat_activity
            WHERE state != 'idle'
              AND query NOT LIKE '%pg_stat_activity%'
            ORDER BY query_start
        """)
        
        active = cursor.fetchall()
        if active:
            print(f"\n⚠️  Найдено активных запросов: {len(active)}")
            for q in active:
                print(f"\nPID {q['pid']}:")
                print(f"  Состояние: {q['state']}")
                print(f"  Длительность: {q['duration']}")
                if q['wait_event_type']:
                    print(f"  Ожидание: {q['wait_event_type']} - {q['wait_event']}")
                if q['query_preview']:
                    print(f"  Запрос: {q['query_preview']}...")
        else:
            print("\n✅ Нет активных запросов")
        
        # Проверяем блокировки
        cursor.execute("""
            SELECT 
                l.locktype,
                l.relation::regclass as table_name,
                l.mode,
                l.granted,
                a.pid,
                a.state,
                LEFT(a.query, 100) as query_preview
            FROM pg_locks l
            JOIN pg_stat_activity a ON l.pid = a.pid
            WHERE l.relation IS NOT NULL
            ORDER BY l.granted, l.pid
        """)
        
        locks = cursor.fetchall()
        if locks:
            print(f"\n⚠️  Найдено блокировок: {len(locks)}")
            for lock in locks:
                granted = "✅ Разрешена" if lock['granted'] else "❌ Ожидает"
                print(f"\n{granted}:")
                print(f"  Таблица: {lock['table_name']}")
                print(f"  Режим: {lock['mode']}")
                print(f"  PID: {lock['pid']}")
                print(f"  Состояние: {lock['state']}")
                if lock['query_preview']:
                    print(f"  Запрос: {lock['query_preview']}...")
        else:
            print("\n✅ Нет блокировок")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    check_locks()

