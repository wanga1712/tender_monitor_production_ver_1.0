"""
MODULE: scripts.check_migration_detailed
RESPONSIBILITY: Detailed status check for tender status migration.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv, loguru, time, traceback.
FORBIDDEN: None.
ERRORS: None.

Детальная проверка миграции статусов

Показывает:
- Все активные запросы
- Блокировки таблиц
- Прогресс миграции (быстро, через выборку)
- Использование ресурсов
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
        connect_timeout=5
    )


def check_migration_detailed():
    """Детальная проверка миграции"""
    conn = get_tender_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        print("\n" + "=" * 70)
        print("ДЕТАЛЬНАЯ ПРОВЕРКА МИГРАЦИИ СТАТУСОВ")
        print("=" * 70)
        
        # 1. Проверка активных запросов
        print("\n📊 АКТИВНЫЕ ЗАПРОСЫ В БД:")
        print("-" * 70)
        cursor.execute("""
            SELECT 
                pid,
                usename,
                application_name,
                state,
                query_start,
                now() - query_start as duration,
                wait_event_type,
                wait_event,
                LEFT(query, 100) as query_preview
            FROM pg_stat_activity
            WHERE state != 'idle'
            ORDER BY query_start
        """)
        
        active_queries = cursor.fetchall()
        if active_queries:
            for q in active_queries:
                print(f"\n  PID: {q['pid']}")
                print(f"  Пользователь: {q['usename']}")
                print(f"  Приложение: {q['application_name']}")
                print(f"  Состояние: {q['state']}")
                print(f"  Длительность: {q['duration']}")
                if q['wait_event_type']:
                    print(f"  Ожидание: {q['wait_event_type']} - {q['wait_event']}")
                if q['query_preview']:
                    print(f"  Запрос: {q['query_preview']}...")
        else:
            print("  ⚠️  НЕТ АКТИВНЫХ ЗАПРОСОВ - миграция может быть завершена или зависла")
        
        # 2. Проверка блокировок
        print("\n\n🔒 БЛОКИРОВКИ ТАБЛИЦ:")
        print("-" * 70)
        cursor.execute("""
            SELECT 
                l.locktype,
                l.relation::regclass as table_name,
                l.mode,
                l.granted,
                l.pid,
                a.usename,
                a.query_start,
                now() - a.query_start as query_duration,
                LEFT(a.query, 80) as query_preview
            FROM pg_locks l
            LEFT JOIN pg_stat_activity a ON l.pid = a.pid
            WHERE l.relation::regclass::text IN ('reestr_contract_44_fz', 'reestr_contract_223_fz')
            ORDER BY l.granted DESC, l.pid
        """)
        
        locks = cursor.fetchall()
        if locks:
            for lock in locks:
                status = "✅ Разрешена" if lock['granted'] else "⏳ Ожидает"
                print(f"\n  Таблица: {lock['table_name']}")
                print(f"  Блокировка: {lock['mode']} ({status})")
                print(f"  PID: {lock['pid']}")
                if lock['usename']:
                    print(f"  Пользователь: {lock['usename']}")
                if lock['query_duration']:
                    print(f"  Длительность запроса: {lock['query_duration']}")
                if lock['query_preview']:
                    print(f"  Запрос: {lock['query_preview']}...")
        else:
            print("  ✅ Нет блокировок на таблицах закупок")
        
        # 3. Быстрая проверка прогресса (через выборку)
        print("\n\n📈 ПРОГРЕСС МИГРАЦИИ (приблизительный, через выборку):")
        print("-" * 70)
        
        # Для 44ФЗ
        cursor.execute("""
            WITH sample AS (
                SELECT status_id 
                FROM reestr_contract_44_fz 
                TABLESAMPLE SYSTEM (0.1)
                LIMIT 10000
            )
            SELECT 
                COUNT(*) FILTER (WHERE status_id IS NOT NULL) as with_status,
                COUNT(*) FILTER (WHERE status_id IS NULL) as without_status,
                COUNT(*) FILTER (WHERE status_id = 1) as status_new,
                COUNT(*) FILTER (WHERE status_id = 2) as status_commission,
                COUNT(*) FILTER (WHERE status_id = 3) as status_won,
                COUNT(*) FILTER (WHERE status_id = 4) as status_bad,
                COUNT(*) as total_sample
            FROM sample
        """)
        
        sample_44fz = cursor.fetchone()
        if sample_44fz and sample_44fz['total_sample'] > 0:
            total = sample_44fz['total_sample']
            with_status = sample_44fz['with_status']
            progress_pct = (with_status / total * 100) if total > 0 else 0
            
            print(f"\n  reestr_contract_44_fz (выборка {total:,} записей):")
            print(f"    Прогресс: {progress_pct:.1f}% записей имеют статус")
            print(f"    └─ С статусом: {with_status:,}")
            print(f"    └─ Без статуса: {sample_44fz['without_status']:,}")
            print(f"    Распределение по статусам:")
            print(f"      • Новая (1): {sample_44fz['status_new']:,}")
            print(f"      • Работа комиссии (2): {sample_44fz['status_commission']:,}")
            print(f"      • Разыграна (3): {sample_44fz['status_won']:,}")
            print(f"      • Плохие (4): {sample_44fz['status_bad']:,}")
        
        # Для 223ФЗ
        cursor.execute("""
            WITH sample AS (
                SELECT status_id 
                FROM reestr_contract_223_fz 
                TABLESAMPLE SYSTEM (0.1)
                LIMIT 10000
            )
            SELECT 
                COUNT(*) FILTER (WHERE status_id IS NOT NULL) as with_status,
                COUNT(*) FILTER (WHERE status_id IS NULL) as without_status,
                COUNT(*) FILTER (WHERE status_id = 4) as status_bad,
                COUNT(*) as total_sample
            FROM sample
        """)
        
        sample_223fz = cursor.fetchone()
        if sample_223fz and sample_223fz['total_sample'] > 0:
            total = sample_223fz['total_sample']
            with_status = sample_223fz['with_status']
            progress_pct = (with_status / total * 100) if total > 0 else 0
            
            print(f"\n  reestr_contract_223_fz (выборка {total:,} записей):")
            print(f"    Прогресс: {progress_pct:.1f}% записей имеют статус")
            print(f"    └─ С статусом: {with_status:,}")
            print(f"    └─ Без статуса: {sample_223fz['without_status']:,}")
            print(f"    └─ Плохие (4): {sample_223fz['status_bad']:,}")
        
        # 4. Проверка статистики обновлений
        print("\n\n📊 СТАТИСТИКА ОБНОВЛЕНИЙ (из системных таблиц):")
        print("-" * 70)
        cursor.execute("""
            SELECT 
                schemaname,
                tablename,
                n_tup_upd as total_updates,
                n_tup_hot_upd as hot_updates,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables
            WHERE tablename IN ('reestr_contract_44_fz', 'reestr_contract_223_fz')
        """)
        
        table_stats = cursor.fetchall()
        for stat in table_stats:
            print(f"\n  Таблица: {stat['tablename']}")
            print(f"    Всего обновлений: {stat['total_updates']:,}")
            print(f"    HOT обновлений: {stat['hot_updates']:,}")
            if stat['last_autovacuum']:
                print(f"    Последний VACUUM: {stat['last_autovacuum']}")
            if stat['last_autoanalyze']:
                print(f"    Последний ANALYZE: {stat['last_autoanalyze']}")
        
        # 5. Проверка существования таблицы статусов
        print("\n\n✅ ПРОВЕРКА СТРУКТУРЫ:")
        print("-" * 70)
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'tender_statuses'
            ) as statuses_table_exists
        """)
        exists = cursor.fetchone()['statuses_table_exists']
        print(f"  Таблица tender_statuses: {'✅ Существует' if exists else '❌ Не найдена'}")
        
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'reestr_contract_44_fz' 
              AND column_name = 'status_id'
        """)
        col_44fz = cursor.fetchone()
        print(f"  Столбец status_id в reestr_contract_44_fz: {'✅ Существует' if col_44fz else '❌ Не найден'}")
        
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'reestr_contract_223_fz' 
              AND column_name = 'status_id'
        """)
        col_223fz = cursor.fetchone()
        print(f"  Столбец status_id в reestr_contract_223_fz: {'✅ Существует' if col_223fz else '❌ Не найден'}")
        
        # 6. Итоговый вывод
        print("\n\n" + "=" * 70)
        if active_queries:
            print("✅ МИГРАЦИЯ РАБОТАЕТ - есть активные запросы")
            print("   Подождите завершения (может занять еще 10-30 минут)")
        else:
            print("⚠️  НЕТ АКТИВНЫХ ЗАПРОСОВ")
            print("   Миграция может быть завершена или зависла")
            print("   Проверьте логи: logs/migration.log")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    check_migration_detailed()

