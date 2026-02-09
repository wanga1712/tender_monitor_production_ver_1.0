"""
MODULE: scripts.check_bad_status
RESPONSIBILITY: Checking records with 'Bad' status.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv.
FORBIDDEN: None.
ERRORS: None.

Проверка статуса Плохие
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

cursor.execute("SELECT COUNT(*)::bigint as count FROM reestr_contract_44_fz WHERE status_id = 4")
bad_count = cursor.fetchone()['count']
print(f"Записей со статусом 'Плохие' (status_id=4): {bad_count:,}")

cursor.execute("SELECT COUNT(*)::bigint as count FROM reestr_contract_44_fz WHERE delivery_end_date IS NULL")
null_delivery = cursor.fetchone()['count']
print(f"Записей с delivery_end_date IS NULL: {null_delivery:,}")

cursor.close()
conn.close()

