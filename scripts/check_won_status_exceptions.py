"""
MODULE: scripts.check_won_status_exceptions
RESPONSIBILITY: Checking exceptions and edge cases for 'Won' status logic.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv.
FORBIDDEN: None.
ERRORS: None.

Проверка условий и исключений для статуса 'Разыграна'
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
print("УСЛОВИЕ И ИСКЛЮЧЕНИЯ ДЛЯ СТАТУСА 'РАЗЫГРАНА' (status_id = 3)")
print("=" * 70)

print("\n📋 Основное условие:")
print("   delivery_end_date IS NOT NULL")
print("   AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'")
print("   (не менее 90 дней от текущей даты в будущем)")

print("\n📋 Исключения:")
print("   - status_id != 4 (не перезаписываем 'Плохие')")
print("   - Проверяется ПЕРВЫМ (самое приоритетное условие)")

# Проверяем, есть ли записи, которые подходят под условие "Разыграна", но имеют другой статус
print("\n" + "=" * 70)
print("ПРОВЕРКА ИСКЛЮЧЕНИЙ")
print("=" * 70)

# Записи, которые подходят под условие "Разыграна", но имеют статус "Плохие"
cursor.execute("""
    SELECT COUNT(*)::bigint as count
    FROM reestr_contract_44_fz
    WHERE status_id = 4
      AND delivery_end_date IS NOT NULL
      AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
""")
bad_but_should_be_won = cursor.fetchone()['count']
print(f"\n📊 Записей со статусом 'Плохие', но подходящих под 'Разыграна': {bad_but_should_be_won:,}")

if bad_but_should_be_won > 0:
    print("   ⚠️  Это невозможно по логике (Плохие = delivery_end_date IS NULL)")
    cursor.execute("""
        SELECT id, end_date, delivery_end_date, status_id
        FROM reestr_contract_44_fz
        WHERE status_id = 4
          AND delivery_end_date IS NOT NULL
          AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
        LIMIT 5
    """)
    examples = cursor.fetchall()
    print("   Примеры проблемных записей:")
    for ex in examples:
        print(f"     ID {ex['id']}: delivery_end_date={ex['delivery_end_date']}, status_id={ex['status_id']}")

# Записи, которые подходят под условие "Разыграна", но имеют статус "Новая"
cursor.execute("""
    SELECT COUNT(*)::bigint as count
    FROM reestr_contract_44_fz
    WHERE status_id = 1
      AND delivery_end_date IS NOT NULL
      AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
""")
new_but_should_be_won = cursor.fetchone()['count']
print(f"\n📊 Записей со статусом 'Новая', но подходящих под 'Разыграна': {new_but_should_be_won:,}")

if new_but_should_be_won > 0:
    print("   ⚠️  Эти записи должны быть 'Разыграна'")
    cursor.execute("""
        SELECT id, end_date, delivery_end_date, status_id
        FROM reestr_contract_44_fz
        WHERE status_id = 1
          AND delivery_end_date IS NOT NULL
          AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
        LIMIT 5
    """)
    examples = cursor.fetchall()
    print("   Примеры:")
    for ex in examples:
        print(f"     ID {ex['id']}: end_date={ex['end_date']}, delivery_end_date={ex['delivery_end_date']}")

# Записи, которые подходят под условие "Разыграна", но имеют статус "Работа комиссии"
cursor.execute("""
    SELECT COUNT(*)::bigint as count
    FROM reestr_contract_44_fz
    WHERE status_id = 2
      AND delivery_end_date IS NOT NULL
      AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
""")
commission_but_should_be_won = cursor.fetchone()['count']
print(f"\n📊 Записей со статусом 'Работа комиссии', но подходящих под 'Разыграна': {commission_but_should_be_won:,}")

if commission_but_should_be_won > 0:
    print("   ⚠️  Эти записи должны быть 'Разыграна'")
    cursor.execute("""
        SELECT id, end_date, delivery_end_date, status_id
        FROM reestr_contract_44_fz
        WHERE status_id = 2
          AND delivery_end_date IS NOT NULL
          AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
        LIMIT 5
    """)
    examples = cursor.fetchall()
    print("   Примеры:")
    for ex in examples:
        print(f"     ID {ex['id']}: end_date={ex['end_date']}, delivery_end_date={ex['delivery_end_date']}")

# Все записи, которые подходят под условие "Разыграна"
cursor.execute("""
    SELECT COUNT(*)::bigint as count
    FROM reestr_contract_44_fz
    WHERE delivery_end_date IS NOT NULL
      AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
""")
all_should_be_won = cursor.fetchone()['count']
print(f"\n📊 Всего записей, подходящих под условие 'Разыграна': {all_should_be_won:,}")

# Из них со статусом "Разыграна"
cursor.execute("""
    SELECT COUNT(*)::bigint as count
    FROM reestr_contract_44_fz
    WHERE status_id = 3
      AND delivery_end_date IS NOT NULL
      AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
""")
correct_won = cursor.fetchone()['count']
print(f"   Из них со статусом 'Разыграна': {correct_won:,}")

wrong_status = all_should_be_won - correct_won
print(f"   С неправильным статусом: {wrong_status:,}")

print("\n" + "=" * 70)
print("ИТОГ")
print("=" * 70)
if bad_but_should_be_won == 0 and new_but_should_be_won == 0 and commission_but_should_be_won == 0:
    print("✅ Все записи, подходящие под условие 'Разыграна', имеют правильный статус!")
    print("✅ Исключение работает правильно (не перезаписываем 'Плохие')")
else:
    print("⚠️  Найдены записи с неправильным статусом:")
    if new_but_should_be_won > 0:
        print(f"   - {new_but_should_be_won:,} записей 'Новая' должны быть 'Разыграна'")
    if commission_but_should_be_won > 0:
        print(f"   - {commission_but_should_be_won:,} записей 'Работа комиссии' должны быть 'Разыграна'")
print("=" * 70)

cursor.close()
conn.close()

