"""
MODULE: scripts.check_status_conditions
RESPONSIBILITY: Verifying conditions for setting different statuses.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv, traceback.
FORBIDDEN: None.
ERRORS: None.

Проверка условий для установки статусов
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

def check_conditions():
    """Проверка условий для каждого статуса"""
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
        print("ПРОВЕРКА УСЛОВИЙ ДЛЯ УСТАНОВКИ СТАТУСОВ")
        print("=" * 70)
        
        print("\n📋 УСЛОВИЯ ДЛЯ СТАТУСОВ 44ФЗ:")
        print("-" * 70)
        
        # 1. Разыграна (status_id = 3)
        print("\n1. СТАТУС 'РАЗЫГРАНА' (status_id = 3):")
        print("   Условие: delivery_end_date IS NOT NULL")
        print("            AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'")
        
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL
              AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
        """)
        won_count = cursor.fetchone()['count']
        print(f"   Записей, соответствующих условию: {won_count:,}")
        
        # Примеры
        cursor.execute("""
            SELECT 
                id,
                end_date,
                delivery_end_date,
                (delivery_end_date - CURRENT_DATE)::integer as days_diff
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL
              AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
            ORDER BY delivery_end_date
            LIMIT 5
        """)
        examples = cursor.fetchall()
        print("   Примеры записей:")
        for ex in examples:
            days_diff = ex['days_diff'] if ex['days_diff'] is not None else 0
            print(f"     ID {ex['id']}: end_date={ex['end_date']}, delivery_end_date={ex['delivery_end_date']}, разница: {days_diff} дней")
        
        # 2. Плохие (status_id = 4)
        print("\n2. СТАТУС 'ПЛОХИЕ' (status_id = 4):")
        print("   Условие: delivery_end_date IS NULL")
        
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NULL
        """)
        bad_count = cursor.fetchone()['count']
        print(f"   Записей, соответствующих условию: {bad_count:,}")
        
        # 3. Работа комиссии (status_id = 2)
        print("\n3. СТАТУС 'РАБОТА КОМИССИИ' (status_id = 2):")
        print("   Условие: end_date IS NOT NULL")
        print("            AND end_date > CURRENT_DATE")
        print("            AND end_date <= CURRENT_DATE + INTERVAL '90 days'")
        print("            AND (delivery_end_date IS NULL OR delivery_end_date < CURRENT_DATE + INTERVAL '90 days')")
        
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE end_date IS NOT NULL
              AND end_date > CURRENT_DATE
              AND end_date <= CURRENT_DATE + INTERVAL '90 days'
              AND (delivery_end_date IS NULL OR delivery_end_date < CURRENT_DATE + INTERVAL '90 days')
        """)
        commission_count = cursor.fetchone()['count']
        print(f"   Записей, соответствующих условию: {commission_count:,}")
        
        # 4. Новая (status_id = 1)
        print("\n4. СТАТУС 'НОВАЯ' (status_id = 1):")
        print("   Условие: end_date IS NOT NULL")
        print("            AND end_date <= CURRENT_DATE")
        print("            AND (delivery_end_date IS NULL OR delivery_end_date < CURRENT_DATE + INTERVAL '90 days')")
        
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE end_date IS NOT NULL
              AND end_date <= CURRENT_DATE
              AND (delivery_end_date IS NULL OR delivery_end_date < CURRENT_DATE + INTERVAL '90 days')
        """)
        new_count = cursor.fetchone()['count']
        print(f"   Записей, соответствующих условию: {new_count:,}")
        
        # Проверка пересечений
        print("\n" + "=" * 70)
        print("ПРОВЕРКА ПЕРЕСЕЧЕНИЙ УСЛОВИЙ:")
        print("=" * 70)
        
        # Записи, которые попадают и в "Разыграна" и в "Новая"
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL
              AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
              AND end_date IS NOT NULL
              AND end_date <= CURRENT_DATE
        """)
        overlap_won_new = cursor.fetchone()['count']
        print(f"\nЗаписи, попадающие в 'Разыграна' И 'Новая': {overlap_won_new:,}")
        if overlap_won_new > 0:
            print("  ⚠️  ЕСТЬ ПЕРЕСЕЧЕНИЕ! Нужно исключить из 'Новая' записи с delivery_end_date >= CURRENT_DATE + 90 дней")
        
        # Записи, которые попадают и в "Разыграна" и в "Работа комиссии"
        cursor.execute("""
            SELECT COUNT(*)::bigint as count
            FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL
              AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
              AND end_date IS NOT NULL
              AND end_date > CURRENT_DATE
              AND end_date <= CURRENT_DATE + INTERVAL '90 days'
        """)
        overlap_won_commission = cursor.fetchone()['count']
        print(f"\nЗаписи, попадающие в 'Разыграна' И 'Работа комиссии': {overlap_won_commission:,}")
        if overlap_won_commission > 0:
            print("  ⚠️  ЕСТЬ ПЕРЕСЕЧЕНИЕ! Нужно исключить из 'Работа комиссии' записи с delivery_end_date >= CURRENT_DATE + 90 дней")
        
        # Итоговая сумма
        total_by_conditions = won_count + bad_count + commission_count + new_count
        cursor.execute("SELECT COUNT(*)::bigint as total FROM reestr_contract_44_fz")
        total_records = cursor.fetchone()['total']
        
        print("\n" + "=" * 70)
        print("ИТОГО:")
        print("=" * 70)
        print(f"Всего записей в таблице: {total_records:,}")
        print(f"Сумма по условиям: {total_by_conditions:,}")
        print(f"Разница: {abs(total_records - total_by_conditions):,}")
        
        if total_by_conditions > total_records:
            print("\n⚠️  ПРОБЛЕМА: Сумма по условиям больше общего количества записей!")
            print("   Это означает, что есть записи, которые попадают под несколько условий.")
        elif total_by_conditions < total_records:
            print("\n⚠️  ПРОБЛЕМА: Сумма по условиям меньше общего количества записей!")
            print("   Это означает, что есть записи, которые не попадают ни под одно условие.")
        else:
            print("\n✅ Все записи покрыты условиями, пересечений нет.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_conditions()

