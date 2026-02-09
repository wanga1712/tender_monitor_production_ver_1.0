"""
MODULE: scripts.verify_won_status
RESPONSIBILITY: Verifying the correctness of the 'Won' status.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv.
FORBIDDEN: None.
ERRORS: None.

Проверка правильности статуса 'Разыграна'
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("TENDER_MONITOR_DB_HOST"),
    database=os.getenv("TENDER_MONITOR_DB_DATABASE"),
    user=os.getenv("TENDER_MONITOR_DB_USER"),
    password=os.getenv("TENDER_MONITOR_DB_PASSWORD"),
    port=os.getenv("TENDER_MONITOR_DB_PORT", "5432")
)
cursor = conn.cursor(cursor_factory=RealDictCursor)

print("=" * 70)
print("ПРОВЕРКА СТАТУСА 'РАЗЫГРАНА'")
print("=" * 70)

# Условие для "Разыграна": delivery_end_date >= CURRENT_DATE + 90 дней
print("\n📋 Условие для статуса 'Разыграна':")
print("   delivery_end_date IS NOT NULL")
print("   AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'")
print("   (не менее 90 дней от текущей даты в будущем)")

# Все записи со статусом "Разыграна"
cursor.execute("""
    SELECT COUNT(*)::bigint as total_won
    FROM reestr_contract_44_fz
    WHERE status_id = 3
""")
total_won = cursor.fetchone()['total_won']
print(f"\n📊 Всего записей со статусом 'Разыграна': {total_won:,}")

# Записи со статусом "Разыграна", которые соответствуют условию
cursor.execute("""
    SELECT COUNT(*)::bigint as correct_won
    FROM reestr_contract_44_fz
    WHERE status_id = 3
      AND delivery_end_date IS NOT NULL
      AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
""")
correct_won = cursor.fetchone()['correct_won']
print(f"✅ Записей, соответствующих условию: {correct_won:,}")

# Записи со статусом "Разыграна", которые НЕ соответствуют условию
cursor.execute("""
    SELECT COUNT(*)::bigint as wrong_won
    FROM reestr_contract_44_fz
    WHERE status_id = 3
      AND (
          delivery_end_date IS NULL
          OR delivery_end_date < CURRENT_DATE + INTERVAL '90 days'
      )
""")
wrong_won = cursor.fetchone()['wrong_won']
print(f"❌ Записей, НЕ соответствующих условию: {wrong_won:,}")

# Записи, которые должны быть "Разыграна", но не имеют этого статуса
cursor.execute("""
    SELECT COUNT(*)::bigint as should_be_won
    FROM reestr_contract_44_fz
    WHERE status_id != 3
      AND delivery_end_date IS NOT NULL
      AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
      AND status_id != 4  -- Исключаем "Плохие"
""")
should_be_won = cursor.fetchone()['should_be_won']
print(f"⚠️  Записей, которые должны быть 'Разыграна', но не имеют статуса: {should_be_won:,}")

# Примеры записей со статусом "Разыграна"
cursor.execute("""
    SELECT 
        id,
        end_date,
        delivery_end_date,
        (delivery_end_date - CURRENT_DATE)::integer as days_until_delivery,
        CURRENT_DATE as today
    FROM reestr_contract_44_fz
    WHERE status_id = 3
    ORDER BY delivery_end_date
    LIMIT 10
""")
examples = cursor.fetchall()

print("\n📋 Примеры записей со статусом 'Разыграна' (первые 10):")
for ex in examples:
    days = ex['days_until_delivery'] if ex['days_until_delivery'] is not None else 0
    print(f"  ID {ex['id']}: end_date={ex['end_date']}, delivery_end_date={ex['delivery_end_date']}, "
          f"дней до поставки: {days}")

# Примеры записей, которые должны быть "Разыграна", но не имеют статуса
if should_be_won > 0:
    cursor.execute("""
        SELECT 
            id,
            status_id,
            end_date,
            delivery_end_date,
            (delivery_end_date - CURRENT_DATE)::integer as days_until_delivery
        FROM reestr_contract_44_fz
        WHERE status_id != 3
          AND delivery_end_date IS NOT NULL
          AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
          AND status_id != 4
        ORDER BY delivery_end_date
        LIMIT 10
    """)
    should_be_examples = cursor.fetchall()
    
    print("\n⚠️  Примеры записей, которые должны быть 'Разыграна' (первые 10):")
    for ex in should_be_examples:
        days = ex['days_until_delivery'] if ex['days_until_delivery'] is not None else 0
        status_name = {1: 'Новая', 2: 'Работа комиссии', 4: 'Плохие'}.get(ex['status_id'], f"status_id={ex['status_id']}")
        print(f"  ID {ex['id']}: статус={status_name}, end_date={ex['end_date']}, "
              f"delivery_end_date={ex['delivery_end_date']}, дней до поставки: {days}")

print("\n" + "=" * 70)
if wrong_won == 0 and should_be_won == 0:
    print("✅ ВСЕ ЗАПИСИ СО СТАТУСОМ 'РАЗЫГРАНА' СООТВЕТСТВУЮТ УСЛОВИЮ!")
else:
    print(f"⚠️  Найдены проблемы:")
    if wrong_won > 0:
        print(f"   - {wrong_won:,} записей со статусом 'Разыграна' не соответствуют условию")
    if should_be_won > 0:
        print(f"   - {should_be_won:,} записей должны быть 'Разыграна', но не имеют статуса")
print("=" * 70)

cursor.close()
conn.close()

