#!/usr/bin/env python3
"""Проверка блокировок и простых запросов"""
import sys
sys.path.insert(0, "/opt/tendermonitor")
from database_work.database_connection import DatabaseManager
import time

db = DatabaseManager()
cur = db.cursor

print("Проверка 1: Простой SELECT COUNT")
start = time.time()
try:
    cur.execute("SELECT COUNT(*) FROM reestr_contract_44_fz")
    result = cur.fetchone()
    print(f"✅ COUNT выполнился за {time.time() - start:.2f} сек: {result[0]}")
except Exception as e:
    print(f"❌ COUNT ошибка: {e}")

print("\nПроверка 2: SELECT по ID (без *)")
start = time.time()
try:
    cur.execute("SELECT id, contract_number FROM reestr_contract_44_fz WHERE id = 514724")
    result = cur.fetchone()
    print(f"✅ SELECT id выполнился за {time.time() - start:.2f} сек: {result}")
except Exception as e:
    print(f"❌ SELECT id ошибка: {e}")

print("\nПроверка 3: SELECT * (может зависнуть)")
start = time.time()
try:
    cur.execute("SELECT * FROM reestr_contract_44_fz WHERE id = 514724 LIMIT 1")
    result = cur.fetchone()
    print(f"✅ SELECT * выполнился за {time.time() - start:.2f} сек")
except Exception as e:
    print(f"❌ SELECT * ошибка: {e}")

print("\nПроверка 4: Блокировки на таблице")
try:
    cur.execute("""
        SELECT 
            locktype, mode, granted, pid, relation::regclass
        FROM pg_locks 
        WHERE relation = 'reestr_contract_44_fz'::regclass
        LIMIT 10
    """)
    locks = cur.fetchall()
    print(f"Блокировки на reestr_contract_44_fz: {len(locks)}")
    for lock in locks[:5]:
        print(f"  {lock}")
except Exception as e:
    print(f"Ошибка проверки блокировок: {e}")

print("\nПроверка 5: Блокировки на completed таблице")
try:
    cur.execute("""
        SELECT 
            locktype, mode, granted, pid, relation::regclass
        FROM pg_locks 
        WHERE relation = 'reestr_contract_44_fz_completed'::regclass
        LIMIT 10
    """)
    locks = cur.fetchall()
    print(f"Блокировки на reestr_contract_44_fz_completed: {len(locks)}")
    for lock in locks[:5]:
        print(f"  {lock}")
except Exception as e:
    print(f"Ошибка проверки блокировок: {e}")

print("\nПроверка 6: Активные запросы")
try:
    cur.execute("""
        SELECT pid, state, query, query_start
        FROM pg_stat_activity 
        WHERE state = 'active' 
        AND query NOT LIKE '%pg_stat_activity%'
        LIMIT 5
    """)
    queries = cur.fetchall()
    print(f"Активные запросы: {len(queries)}")
    for q in queries:
        print(f"  PID {q[0]}: {q[1]} - {q[2][:100]}")
except Exception as e:
    print(f"Ошибка проверки запросов: {e}")

db.close()
