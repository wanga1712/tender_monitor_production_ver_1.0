"""
MODULE: scripts.check_bad_status_condition
RESPONSIBILITY: Verifying logic conditions for 'Bad' status.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv.
FORBIDDEN: None.
ERRORS: None.

Проверка условия для статуса 'Плохие'
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
print("УСЛОВИЕ ДЛЯ СТАТУСА 'ПЛОХИЕ' (status_id = 4)")
print("=" * 70)

print("\n📋 Для 44ФЗ:")
print("   Основное условие: delivery_end_date IS NULL")
print("   НО исключаются записи, которые подходят под другие статусы:")
print("   - НЕ подходит под 'Работа комиссии' (end_date > CURRENT_DATE AND end_date <= CURRENT_DATE + 90 дней)")
print("   - НЕ подходит под 'Новая' (end_date <= CURRENT_DATE)")

print("\n📋 Для 223ФЗ:")
print("   Условие: end_date > CURRENT_DATE + 180 дней")

# Статистика для 44ФЗ
print("\n" + "=" * 70)
print("СТАТИСТИКА ДЛЯ 44ФЗ")
print("=" * 70)

# Все записи со статусом "Плохие"
cursor.execute("""
    SELECT COUNT(*)::bigint as total_bad
    FROM reestr_contract_44_fz
    WHERE status_id = 4
""")
total_bad = cursor.fetchone()['total_bad']
print(f"\n📊 Всего записей со статусом 'Плохие': {total_bad:,}")

# Записи со статусом "Плохие", которые соответствуют условию
cursor.execute("""
    SELECT COUNT(*)::bigint as correct_bad
    FROM reestr_contract_44_fz
    WHERE status_id = 4
      AND delivery_end_date IS NULL
      AND NOT (
          end_date IS NOT NULL 
          AND (
              (end_date > CURRENT_DATE AND end_date <= CURRENT_DATE + INTERVAL '90 days')
              OR end_date <= CURRENT_DATE
          )
      )
""")
correct_bad = cursor.fetchone()['correct_bad']
print(f"✅ Записей, соответствующих полному условию: {correct_bad:,}")

# Записи с delivery_end_date IS NULL (основное условие)
cursor.execute("""
    SELECT COUNT(*)::bigint as null_delivery
    FROM reestr_contract_44_fz
    WHERE delivery_end_date IS NULL
""")
null_delivery = cursor.fetchone()['null_delivery']
print(f"📋 Всего записей с delivery_end_date IS NULL: {null_delivery:,}")

# Из них со статусом "Плохие"
cursor.execute("""
    SELECT COUNT(*)::bigint as bad_with_null
    FROM reestr_contract_44_fz
    WHERE status_id = 4
      AND delivery_end_date IS NULL
""")
bad_with_null = cursor.fetchone()['bad_with_null']
print(f"   Из них со статусом 'Плохие': {bad_with_null:,}")

# Записи с delivery_end_date IS NULL, которые НЕ имеют статус "Плохие" (подходят под другие статусы)
cursor.execute("""
    SELECT COUNT(*)::bigint as null_not_bad
    FROM reestr_contract_44_fz
    WHERE delivery_end_date IS NULL
      AND status_id != 4
      AND status_id IS NOT NULL
""")
null_not_bad = cursor.fetchone()['null_not_bad']
print(f"   С delivery_end_date IS NULL, но НЕ 'Плохие' (под другие статусы): {null_not_bad:,}")

# Примеры записей со статусом "Плохие"
cursor.execute("""
    SELECT 
        id,
        end_date,
        delivery_end_date,
        CASE 
            WHEN end_date IS NULL THEN 'end_date IS NULL'
            WHEN end_date <= CURRENT_DATE THEN 'end_date <= CURRENT_DATE (Новая)'
            WHEN end_date > CURRENT_DATE AND end_date <= CURRENT_DATE + INTERVAL '90 days' THEN 'Работа комиссии'
            ELSE 'end_date > CURRENT_DATE + 90 дней'
        END as category
    FROM reestr_contract_44_fz
    WHERE status_id = 4
    ORDER BY id DESC
    LIMIT 10
""")
examples = cursor.fetchall()

print("\n📋 Примеры записей со статусом 'Плохие' (первые 10):")
for ex in examples:
    print(f"  ID {ex['id']}: end_date={ex['end_date']}, delivery_end_date={ex['delivery_end_date']}, "
          f"категория: {ex['category']}")

# Статистика для 223ФЗ
print("\n" + "=" * 70)
print("СТАТИСТИКА ДЛЯ 223ФЗ")
print("=" * 70)

cursor.execute("""
    SELECT COUNT(*)::bigint as total_bad_223
    FROM reestr_contract_223_fz
    WHERE status_id = 4
""")
total_bad_223 = cursor.fetchone()['total_bad_223']
print(f"\n📊 Всего записей со статусом 'Плохие': {total_bad_223:,}")

cursor.execute("""
    SELECT COUNT(*)::bigint as correct_bad_223
    FROM reestr_contract_223_fz
    WHERE status_id = 4
      AND end_date IS NOT NULL
      AND end_date > CURRENT_DATE + INTERVAL '180 days'
""")
correct_bad_223 = cursor.fetchone()['correct_bad_223']
print(f"✅ Записей, соответствующих условию (end_date > CURRENT_DATE + 180 дней): {correct_bad_223:,}")

print("\n" + "=" * 70)
print("ИТОГО")
print("=" * 70)
print(f"44ФЗ: {total_bad:,} записей со статусом 'Плохие'")
print(f"223ФЗ: {total_bad_223:,} записей со статусом 'Плохие'")
print("=" * 70)

cursor.close()
conn.close()

