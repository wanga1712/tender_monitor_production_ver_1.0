"""
MODULE: scripts.check_commission_status
RESPONSIBILITY: Checking records for 'Commission Work' status.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv.
FORBIDDEN: None.
ERRORS: None.

Проверка записей для статуса 'Работа комиссии'
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
print("ПРОВЕРКА ЗАПИСЕЙ ДЛЯ СТАТУСА 'РАБОТА КОМИССИИ'")
print("=" * 70)

# Записи, которые должны быть "Работа комиссии"
cursor.execute("""
    SELECT 
        COALESCE(ts.name, 'Без статуса') as status_name,
        COUNT(*)::bigint as count
    FROM reestr_contract_44_fz r
    LEFT JOIN tender_statuses ts ON r.status_id = ts.id
    WHERE r.end_date IS NOT NULL
      AND r.end_date > CURRENT_DATE
      AND r.end_date <= CURRENT_DATE + INTERVAL '90 days'
      AND (r.delivery_end_date IS NULL OR r.delivery_end_date < CURRENT_DATE + INTERVAL '90 days')
    GROUP BY ts.name, ts.id
    ORDER BY ts.id NULLS FIRST
""")
statuses = cursor.fetchall()

print("\n📊 Распределение статусов для записей, которые должны быть 'Работа комиссии':")
for stat in statuses:
    print(f"  {stat['status_name']}: {stat['count']:,} записей")

# Примеры записей
cursor.execute("""
    SELECT 
        r.id,
        r.end_date,
        r.delivery_end_date,
        COALESCE(ts.name, 'Без статуса') as status_name,
        CURRENT_DATE as today,
        (r.end_date - CURRENT_DATE)::integer as days_until_end
    FROM reestr_contract_44_fz r
    LEFT JOIN tender_statuses ts ON r.status_id = ts.id
    WHERE r.end_date IS NOT NULL
      AND r.end_date > CURRENT_DATE
      AND r.end_date <= CURRENT_DATE + INTERVAL '90 days'
      AND (r.delivery_end_date IS NULL OR r.delivery_end_date < CURRENT_DATE + INTERVAL '90 days')
    ORDER BY r.end_date
    LIMIT 10
""")
examples = cursor.fetchall()

print("\n📋 Примеры записей (первые 10):")
for ex in examples:
    days = ex['days_until_end'] if ex['days_until_end'] is not None else 0
    print(f"  ID {ex['id']}: end_date={ex['end_date']}, delivery_end_date={ex['delivery_end_date']}, "
          f"статус={ex['status_name']}, дней до окончания: {days}")

cursor.close()
conn.close()

