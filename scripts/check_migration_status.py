"""
MODULE: scripts.check_migration_status
RESPONSIBILITY: Checking migration status and record counts.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv, traceback.
FORBIDDEN: None.
ERRORS: None.

Проверка статуса миграции - сколько записей обработано
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

def check_status():
    """Проверка статистики по статусам"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("TENDER_MONITOR_DB_HOST"),
            database=os.getenv("TENDER_MONITOR_DB_DATABASE"),
            user=os.getenv("TENDER_MONITOR_DB_USER"),
            password=os.getenv("TENDER_MONITOR_DB_PASSWORD"),
            port=os.getenv("TENDER_MONITOR_DB_PORT", "5432"),
            connect_timeout=10
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=" * 70)
        print("СТАТИСТИКА МИГРАЦИИ СТАТУСОВ")
        print("=" * 70)
        
        # Общая статистика по 44ФЗ
        print("\n📊 reestr_contract_44_fz:")
        cursor.execute("""
            SELECT 
                COUNT(*)::bigint as total,
                COUNT(status_id)::bigint as with_status,
                COUNT(*)::bigint - COUNT(status_id)::bigint as without_status
            FROM reestr_contract_44_fz
        """)
        stats_44fz = cursor.fetchone()
        print(f"  Всего записей: {stats_44fz['total']:,}")
        print(f"  С статусом: {stats_44fz['with_status']:,}")
        print(f"  Без статуса: {stats_44fz['without_status']:,}")
        
        # Статусы по типам для 44ФЗ
        cursor.execute("""
            SELECT 
                ts.name as status_name,
                COUNT(*)::bigint as count
            FROM reestr_contract_44_fz r
            LEFT JOIN tender_statuses ts ON r.status_id = ts.id
            GROUP BY ts.name, ts.id
            ORDER BY ts.id NULLS FIRST
        """)
        statuses_44fz = cursor.fetchall()
        print("\n  Распределение по статусам:")
        for stat in statuses_44fz:
            status_name = stat['status_name'] or "Без статуса"
            count = stat['count']
            percent = (count / stats_44fz['total'] * 100) if stats_44fz['total'] > 0 else 0
            print(f"    {status_name}: {count:,} ({percent:.1f}%)")
        
        # Общая статистика по 223ФЗ
        print("\n📊 reestr_contract_223_fz:")
        cursor.execute("""
            SELECT 
                COUNT(*)::bigint as total,
                COUNT(status_id)::bigint as with_status,
                COUNT(*)::bigint - COUNT(status_id)::bigint as without_status
            FROM reestr_contract_223_fz
        """)
        stats_223fz = cursor.fetchone()
        print(f"  Всего записей: {stats_223fz['total']:,}")
        print(f"  С статусом: {stats_223fz['with_status']:,}")
        print(f"  Без статуса: {stats_223fz['without_status']:,}")
        
        # Статусы по типам для 223ФЗ
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN r.status_id IS NULL THEN 'Без статуса (используются в поиске)'
                    ELSE ts.name 
                END as status_name,
                COUNT(*)::bigint as count
            FROM reestr_contract_223_fz r
            LEFT JOIN tender_statuses ts ON r.status_id = ts.id
            GROUP BY r.status_id, ts.name
            ORDER BY r.status_id NULLS FIRST
        """)
        statuses_223fz = cursor.fetchall()
        print("\n  Распределение по статусам:")
        for stat in statuses_223fz:
            status_name = stat['status_name']
            count = stat['count']
            percent = (count / stats_223fz['total'] * 100) if stats_223fz['total'] > 0 else 0
            print(f"    {status_name}: {count:,} ({percent:.1f}%)")
        
        # Проверка условий для необработанных записей 44ФЗ
        if stats_44fz['without_status'] > 0:
            print("\n🔍 Проверка условий для необработанных записей 44ФЗ:")
            
            # Новая
            cursor.execute("""
                SELECT COUNT(*)::bigint as count
                FROM reestr_contract_44_fz
                WHERE status_id IS NULL
                  AND end_date IS NOT NULL 
                  AND end_date <= CURRENT_DATE
            """)
            new_count = cursor.fetchone()['count']
            print(f"  'Новая': {new_count:,} записей")
            
            # Работа комиссии
            cursor.execute("""
                SELECT COUNT(*)::bigint as count
                FROM reestr_contract_44_fz
                WHERE status_id IS NULL
                  AND end_date IS NOT NULL 
                  AND end_date > CURRENT_DATE 
                  AND end_date <= CURRENT_DATE + INTERVAL '90 days'
            """)
            commission_count = cursor.fetchone()['count']
            print(f"  'Работа комиссии': {commission_count:,} записей")
            
            # Разыграна
            cursor.execute("""
                SELECT COUNT(*)::bigint as count
                FROM reestr_contract_44_fz
                WHERE status_id IS NULL
                  AND delivery_end_date IS NOT NULL 
                  AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
            """)
            won_count = cursor.fetchone()['count']
            print(f"  'Разыграна': {won_count:,} записей")
            
            # Плохие
            cursor.execute("""
                SELECT COUNT(*)::bigint as count
                FROM reestr_contract_44_fz
                WHERE status_id IS NULL
                  AND delivery_end_date IS NULL
            """)
            bad_count = cursor.fetchone()['count']
            print(f"  'Плохие': {bad_count:,} записей")
        
        print("\n" + "=" * 70)
        print("ВЫВОД:")
        
        if stats_44fz['with_status'] == stats_44fz['total'] and stats_223fz['with_status'] == stats_223fz['total']:
            print("✅ ВСЕ ЗАПИСИ ОБРАБОТАНЫ!")
            print("   Миграция полностью завершена.")
        elif stats_44fz['with_status'] > 0 or stats_223fz['with_status'] > 0:
            print(f"⚠️  Частично обработано:")
            print(f"   44ФЗ: {stats_44fz['with_status']:,} из {stats_44fz['total']:,} ({stats_44fz['with_status']/stats_44fz['total']*100:.1f}%)")
            print(f"   223ФЗ: {stats_223fz['with_status']:,} из {stats_223fz['total']:,} ({stats_223fz['with_status']/stats_223fz['total']*100:.1f}%)")
            print(f"\n   Осталось обработать:")
            print(f"   44ФЗ: {stats_44fz['without_status']:,} записей")
            print(f"   223ФЗ: {stats_223fz['without_status']:,} записей")
        else:
            print("❌ ЗАПИСИ НЕ ОБРАБОТАНЫ!")
            print("   Запустите скрипт миграции для обновления статусов.")
        
        print("=" * 70)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_status()

