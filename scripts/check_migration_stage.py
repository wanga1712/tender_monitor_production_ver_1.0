"""
MODULE: scripts.check_migration_stage
RESPONSIBILITY: Checking the current stage of the migration process.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv.
FORBIDDEN: None.
ERRORS: None.

Проверка на каком этапе остановилась миграция
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

def check_migration_stage():
    """Проверка этапа миграции"""
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
        print("ПРОВЕРКА ЭТАПА МИГРАЦИИ")
        print("=" * 70)
        
        # 1. Проверка таблицы статусов
        print("\n1. Таблица tender_statuses:")
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name = 'tender_statuses'
            ) as exists
        """)
        statuses_exists = cursor.fetchone()['exists']
        print(f"   {'✅ Создана' if statuses_exists else '❌ НЕ создана'}")
        
        if statuses_exists:
            cursor.execute("SELECT id, name FROM tender_statuses ORDER BY id")
            statuses = cursor.fetchall()
            print(f"   Статусы в таблице: {len(statuses)}")
            for s in statuses:
                print(f"     - {s['id']}: {s['name']}")
        
        # 2. Проверка столбца status_id в reestr_contract_44_fz
        print("\n2. Столбец status_id в reestr_contract_44_fz:")
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'public'
              AND table_name = 'reestr_contract_44_fz' 
              AND column_name = 'status_id'
        """)
        col_44fz = cursor.fetchone()
        if col_44fz:
            print(f"   ✅ Существует (тип: {col_44fz['data_type']}, nullable: {col_44fz['is_nullable']})")
        else:
            print("   ❌ НЕ создан")
        
        # 3. Проверка столбца status_id в reestr_contract_223_fz
        print("\n3. Столбец status_id в reestr_contract_223_fz:")
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'public'
              AND table_name = 'reestr_contract_223_fz' 
              AND column_name = 'status_id'
        """)
        col_223fz = cursor.fetchone()
        if col_223fz:
            print(f"   ✅ Существует (тип: {col_223fz['data_type']}, nullable: {col_223fz['is_nullable']})")
        else:
            print("   ❌ НЕ создан")
        
        # 4. Проверка внешних ключей
        print("\n4. Внешние ключи:")
        cursor.execute("""
            SELECT 
                conname as constraint_name,
                conrelid::regclass as table_name
            FROM pg_constraint
            WHERE conname IN (
                'fk_reestr_contract_44_fz_status_id',
                'fk_reestr_contract_223_fz_status_id'
            )
        """)
        fks = cursor.fetchall()
        for fk in fks:
            print(f"   ✅ {fk['constraint_name']} на {fk['table_name']}")
        if not fks:
            print("   ❌ Внешние ключи НЕ созданы")
        
        # 5. Проверка активных запросов
        print("\n5. Активные запросы:")
        cursor.execute("""
            SELECT 
                pid,
                state,
                now() - query_start as duration,
                LEFT(query, 60) as query_preview
            FROM pg_stat_activity
            WHERE state != 'idle'
              AND query NOT LIKE '%pg_stat_activity%'
            ORDER BY query_start
        """)
        active = cursor.fetchall()
        if active:
            print(f"   Найдено активных запросов: {len(active)}")
            for q in active:
                print(f"     PID {q['pid']}: {q['state']} ({q['duration']})")
                if q['query_preview']:
                    print(f"       {q['query_preview']}...")
        else:
            print("   ⚠️  НЕТ активных запросов")
        
        # 6. Итоговый вывод
        print("\n" + "=" * 70)
        print("ВЫВОД:")
        
        if not statuses_exists:
            print("❌ Миграция НЕ началась или остановилась на ШАГЕ 1 (создание таблицы статусов)")
        elif not col_44fz or not col_223fz:
            print("⚠️  Миграция остановилась на ШАГЕ 2-3 (создание столбцов)")
        elif not fks:
            print("⚠️  Миграция остановилась на ШАГЕ 2-3 (создание внешних ключей)")
        elif not active:
            print("⚠️  Миграция остановилась на ШАГЕ 4-5 (обновление статусов)")
            print("   Возможно, UPDATE завис или завершился с ошибкой")
        else:
            print("✅ Миграция работает - есть активные запросы")
        
        print("=" * 70)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        print("   Возможно, БД перегружена или миграция блокирует подключения")


if __name__ == "__main__":
    check_migration_stage()

