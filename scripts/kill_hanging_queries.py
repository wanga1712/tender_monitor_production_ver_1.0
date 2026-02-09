"""
MODULE: scripts.kill_hanging_queries
RESPONSIBILITY: Terminating hanging database queries.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv.
FORBIDDEN: None.
ERRORS: None.

Убить зависшие запросы (ОСТОРОЖНО!)
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

def kill_queries():
    """Убить все активные запросы (кроме системных)"""
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
        print("ПОИСК ЗАВИСШИХ ЗАПРОСОВ")
        print("=" * 70)
        
        # Находим зависшие запросы
        cursor.execute("""
            SELECT 
                pid,
                state,
                now() - query_start as duration,
                LEFT(query, 100) as query_preview
            FROM pg_stat_activity
            WHERE state != 'idle'
              AND query NOT LIKE '%pg_stat_activity%'
              AND pid != pg_backend_pid()
            ORDER BY query_start
        """)
        
        queries = cursor.fetchall()
        
        if not queries:
            print("✅ Нет зависших запросов")
            return
        
        print(f"\n⚠️  Найдено зависших запросов: {len(queries)}")
        for q in queries:
            print(f"\nPID {q['pid']}:")
            print(f"  Длительность: {q['duration']}")
            print(f"  Запрос: {q['query_preview']}...")
        
        print("\n" + "=" * 70)
        response = input("Убить все эти запросы? (yes/no): ")
        
        if response.lower() != 'yes':
            print("Отменено")
            return
        
        # Убиваем запросы
        killed = 0
        for q in queries:
            try:
                cursor.execute(f"SELECT pg_terminate_backend({q['pid']})")
                result = cursor.fetchone()
                if result[0]:
                    print(f"✅ Убит PID {q['pid']}")
                    killed += 1
                else:
                    print(f"⚠️  Не удалось убить PID {q['pid']}")
            except Exception as e:
                print(f"❌ Ошибка при убийстве PID {q['pid']}: {e}")
        
        conn.commit()
        print(f"\n✅ Убито запросов: {killed} из {len(queries)}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    kill_queries()

