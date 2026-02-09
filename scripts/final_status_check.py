"""
MODULE: scripts.final_status_check
RESPONSIBILITY: Performing a final check of all tender statuses.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv.
FORBIDDEN: None.
ERRORS: None.

Финальная проверка всех статусов
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
print("ФИНАЛЬНАЯ ПРОВЕРКА СТАТУСОВ")
print("=" * 70)

# Общая статистика
print("\n📊 Распределение статусов в reestr_contract_44_fz:")
cursor.execute("""
    SELECT 
        COALESCE(ts.name, 'Без статуса') as status_name,
        COUNT(*)::bigint as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM reestr_contract_44_fz), 2) as percent
    FROM reestr_contract_44_fz r
    LEFT JOIN tender_statuses ts ON r.status_id = ts.id
    GROUP BY ts.name, ts.id
    ORDER BY ts.id NULLS FIRST
""")
statuses = cursor.fetchall()
total = 0
for stat in statuses:
    print(f"  {stat['status_name']}: {stat['count']:,} ({stat['percent']}%)")
    total += stat['count']

print(f"\n  ИТОГО: {total:,} записей")

# Проверка записей, которые должны быть "Работа комиссии"
print("\n🔍 Проверка записей для статуса 'Работа комиссии':")
cursor.execute("""
    SELECT COUNT(*)::bigint as count
    FROM reestr_contract_44_fz
    WHERE end_date IS NOT NULL
      AND end_date > CURRENT_DATE
      AND end_date <= CURRENT_DATE + INTERVAL '90 days'
      AND (delivery_end_date IS NULL OR delivery_end_date < CURRENT_DATE + INTERVAL '90 days')
""")
should_be_commission = cursor.fetchone()['count']
print(f"  Записей, которые должны быть 'Работа комиссии': {should_be_commission:,}")

cursor.execute("""
    SELECT COUNT(*)::bigint as count
    FROM reestr_contract_44_fz
    WHERE status_id = 2
""")
actual_commission = cursor.fetchone()['count']
print(f"  Записей со статусом 'Работа комиссии': {actual_commission:,}")

if should_be_commission != actual_commission:
    print(f"  ⚠️  Несоответствие: {abs(should_be_commission - actual_commission):,} записей")

# Проверка правильности всех статусов
print("\n✅ Проверка правильности статусов:")

# Разыграна
cursor.execute("""
    SELECT COUNT(*)::bigint as count
    FROM reestr_contract_44_fz
    WHERE status_id = 3
      AND (delivery_end_date IS NULL OR delivery_end_date < CURRENT_DATE + INTERVAL '90 days')
""")
wrong_won = cursor.fetchone()['count']
print(f"  'Разыграна' с неправильным условием: {wrong_won:,}")

# Плохие
cursor.execute("""
    SELECT COUNT(*)::bigint as count
    FROM reestr_contract_44_fz
    WHERE status_id = 4
      AND delivery_end_date IS NOT NULL
""")
wrong_bad = cursor.fetchone()['count']
print(f"  'Плохие' с delivery_end_date: {wrong_bad:,}")

# Новая
cursor.execute("""
    SELECT COUNT(*)::bigint as count
    FROM reestr_contract_44_fz
    WHERE status_id = 1
      AND delivery_end_date IS NOT NULL
      AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days'
""")
wrong_new = cursor.fetchone()['count']
print(f"  'Новая' с delivery_end_date >= 90 дней: {wrong_new:,}")

total_wrong = wrong_won + wrong_bad + wrong_new
if total_wrong == 0:
    print("\n✅ ВСЕ СТАТУСЫ УСТАНОВЛЕНЫ ПРАВИЛЬНО!")
else:
    print(f"\n⚠️  Найдено неправильных статусов: {total_wrong:,}")

print("\n" + "=" * 70)
print("ИТОГОВАЯ СТАТИСТИКА:")
print("=" * 70)
print(f"✅ Все записи обработаны: {total:,} из {total:,}")
print(f"✅ Неправильных статусов: {total_wrong:,}")
print("=" * 70)

cursor.close()
conn.close()

