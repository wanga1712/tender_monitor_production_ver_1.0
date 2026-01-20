#!/usr/bin/env python3
"""Простой скрипт для проверки последней даты"""
import sys
sys.path.insert(0, "/opt/tendermonitor")
from database_work.database_connection import DatabaseManager

db = DatabaseManager()
cur = db.cursor

cur.execute("SELECT MAX(start_date), MAX(end_date), COUNT(*) FROM reestr_contract_223_fz;")
result = cur.fetchone()

print(f"Последняя start_date: {result[0]}")
print(f"Последняя end_date: {result[1]}")
print(f"Всего записей: {result[2]}")

db.close()
