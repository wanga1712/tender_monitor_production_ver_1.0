"""
MODULE: scripts.check_won_status_conditions
RESPONSIBILITY: Checking conditions specific to 'Won' status.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv, traceback.
FORBIDDEN: None.
ERRORS: None.

Проверка условий для статуса 'Разыграна'
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

def check_won_conditions():
    """Проверка условий для статуса 'Разыграна'"""
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
        print("ПРОВЕРКА УСЛОВИЙ ДЛЯ СТАТУСА 'РАЗЫГРАНА'")
        print("=" * 70)
        
        # Общая статистика по delivery_end_date
        print("\n📊 Статистика по delivery_end_date в reestr_contract_44_fz:")
        cursor.execute("""
            SELECT 
                COUNT(*)::bigint as total,
                COUNT(delivery_end_date)::bigint as with_delivery_date,
                COUNT(*)::bigint - COUNT(delivery_end_date)::bigint as without_delivery_date
            FROM reestr_contract_44_fz
        """)
        stats = cursor.fetchone()
        print(f"  Всего записей: {stats['total']:,}")
        print(f"  С delivery_end_date: {stats['with_delivery_date']:,}")
        print(f"  Без delivery_end_date: {stats['without_delivery_date']:,}")
        
        # Проверка текущего условия для "Разыграна"
        print("\n🔍 Проверка условия для 'Разыграна':")
        print("   Условие: delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'")
        
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL 
              AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
        """)
        won_count = cursor.fetchone()['count']
        print(f"  Записей, соответствующих условию: {won_count:,}")
        
        # Проверка альтернативных условий
        print("\n🔍 Альтернативные условия:")
        
        # Вариант 1: delivery_end_date >= CURRENT_DATE (любая будущая дата)
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL 
              AND delivery_end_date >= CURRENT_DATE
        """)
        won_v1 = cursor.fetchone()['count']
        print(f"  1. delivery_end_date >= CURRENT_DATE: {won_v1:,} записей")
        
        # Вариант 2: delivery_end_date > CURRENT_DATE (строго в будущем)
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL 
              AND delivery_end_date > CURRENT_DATE
        """)
        won_v2 = cursor.fetchone()['count']
        print(f"  2. delivery_end_date > CURRENT_DATE: {won_v2:,} записей")
        
        # Вариант 3: delivery_end_date >= CURRENT_DATE - 90 days (включая прошлые 90 дней)
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL 
              AND delivery_end_date >= CURRENT_DATE - INTERVAL '90 days'
        """)
        won_v3 = cursor.fetchone()['count']
        print(f"  3. delivery_end_date >= CURRENT_DATE - 90 days: {won_v3:,} записей")
        
        # Вариант 4: delivery_end_date IS NOT NULL (все с датой поставки)
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL
        """)
        won_v4 = cursor.fetchone()['count']
        print(f"  4. delivery_end_date IS NOT NULL (все): {won_v4:,} записей")
        
        # Примеры записей с delivery_end_date
        print("\n📋 Примеры записей с delivery_end_date:")
        cursor.execute("""
            SELECT 
                id,
                end_date,
                delivery_end_date,
                CURRENT_DATE as today,
                (delivery_end_date - CURRENT_DATE)::integer as days_diff
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL
            ORDER BY delivery_end_date DESC
            LIMIT 10
        """)
        examples = cursor.fetchall()
        for ex in examples:
            days_diff = ex['days_diff'] if ex['days_diff'] is not None else 0
            print(f"  ID {ex['id']}: end_date={ex['end_date']}, delivery_end_date={ex['delivery_end_date']}, разница: {days_diff} дней")
        
        # Проверка записей, которые уже имеют статус
        print("\n📊 Записи с установленными статусами:")
        cursor.execute("""
            SELECT 
                ts.name as status_name,
                COUNT(*)::bigint as count,
                COUNT(CASE WHEN r.delivery_end_date IS NOT NULL THEN 1 END)::bigint as with_delivery_date
            FROM reestr_contract_44_fz r
            LEFT JOIN tender_statuses ts ON r.status_id = ts.id
            GROUP BY ts.name, ts.id
            ORDER BY ts.id NULLS FIRST
        """)
        statuses = cursor.fetchall()
        for stat in statuses:
            status_name = stat['status_name'] or "Без статуса"
            count = stat['count']
            with_delivery = stat['with_delivery_date']
            print(f"  {status_name}: {count:,} записей (из них с delivery_end_date: {with_delivery:,})")
        
        print("\n" + "=" * 70)
        print("ВЫВОД:")
        if won_count == 0:
            print("❌ Текущее условие не находит записей для статуса 'Разыграна'")
            print("   Возможно, условие слишком строгое или таких записей нет.")
            if won_v4 > 0:
                print(f"   Но есть {won_v4:,} записей с delivery_end_date IS NOT NULL")
                print("   Нужно пересмотреть условие для статуса 'Разыграна'")
        else:
            print(f"✅ Условие находит {won_count:,} записей")
        print("=" * 70)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_won_conditions()

