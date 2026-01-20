#!/usr/bin/env python3
"""Скрипт для проверки новых закупок за ночь"""
import sys
sys.path.insert(0, "/opt/tendermonitor")
from database_work.database_connection import DatabaseManager
from datetime import datetime, timedelta

db = DatabaseManager()
cur = db.cursor

# Проверяем обработанные файлы - это основной индикатор активности
cur.execute("SELECT MAX(id), COUNT(*) FROM file_names_xml")
row = cur.fetchone()
max_file_id = row[0] or 0
total_files = row[1] or 0

# Используем processed_at, чтобы понимать, КОГДА реально обрабатывались файлы
cur.execute("SELECT MAX(processed_at) FROM file_names_xml")
last_processed_at = cur.fetchone()[0]

# Новые файлы за последние 12 часов по processed_at
cur.execute("""
    SELECT COUNT(*)
    FROM file_names_xml
    WHERE processed_at >= NOW() - INTERVAL '12 hours'
""")
recent_files_12h = cur.fetchone()[0] or 0

# Новые файлы за последние 24 часа
cur.execute("""
    SELECT COUNT(*)
    FROM file_names_xml
    WHERE processed_at >= NOW() - INTERVAL '24 hours'
""")
recent_files_24h = cur.fetchone()[0] or 0

# Получаем примерные последние файлы для примера
cur.execute("""
    SELECT file_name, processed_at
    FROM file_names_xml
    ORDER BY processed_at DESC
    LIMIT 5
""")
last_files_rows = cur.fetchall()
last_files = [(row[0], row[1]) for row in last_files_rows]

# Проверяем новые контракты (по максимальному ID)
cur.execute("SELECT MAX(id) FROM reestr_contract_44_fz")
max_contract_id = cur.fetchone()[0]

# Проверяем последнюю дату контракта
cur.execute("""
    SELECT MAX(start_date) 
    FROM reestr_contract_44_fz 
    WHERE start_date IS NOT NULL
""")
last_date = cur.fetchone()[0]

# Общее количество контрактов
cur.execute('SELECT COUNT(*) FROM reestr_contract_44_fz')
total = cur.fetchone()[0]

# Завершенные контракты
cur.execute('SELECT COUNT(*) FROM reestr_contract_44_fz_completed')
completed = cur.fetchone()[0]

# Проверяем таблицу 223-ФЗ
cur.execute('SELECT COUNT(*) FROM reestr_contract_223_fz')
total_223 = cur.fetchone()[0]

cur.execute('SELECT MAX(id) FROM reestr_contract_223_fz')
max_contract_223_id = cur.fetchone()[0] or 0

cur.execute("""
    SELECT MAX(start_date) 
    FROM reestr_contract_223_fz 
    WHERE start_date IS NOT NULL
""")
last_date_223 = cur.fetchone()[0]

print(f"\n{'='*60}")
print(f"📊 НОВЫЕ ЗАКУПКИ ЗА НОЧЬ")
print(f"{'='*60}")
print(f"🔹 ОБРАБОТАННЫЕ ФАЙЛЫ (file_names_xml):")
print(f"   Всего файлов обработано: {total_files:,}")
print(f"   Максимальный ID файла: {max_file_id:,}")
print(f"   Последнее время обработки (processed_at): {last_processed_at}")
print(f"   Новых файлов за последние 12 часов: {recent_files_12h:,}")
print(f"   Новых файлов за последние 24 часа: {recent_files_24h:,}")
print(f"   Пример последних файлов:")
for i, (file_name, processed_at) in enumerate(last_files, 1):
    print(f"      {i}. {file_name[:80]}...  ({processed_at})")
print(f"\n🔹 КОНТРАКТЫ 44-ФЗ:")
print(f"   Всего контрактов 44-ФЗ: {total:,}")
print(f"   Завершенных контрактов: {completed:,}")
print(f"   Активных контрактов: {total - completed:,}")
print(f"   Максимальный ID контракта: {max_contract_id:,}")
print(f"   Последняя дата контракта: {last_date}")
print(f"\n🔹 КОНТРАКТЫ 223-ФЗ:")
print(f"   Всего контрактов 223-ФЗ: {total_223:,}")
print(f"   Максимальный ID контракта: {max_contract_223_id:,}")
print(f"   Последняя дата контракта: {last_date_223 or 'Нет данных'}")
print(f"{'='*60}")
print(f"\n💡 ВЫВОД:")
if recent_files_24h > 0:
    print(f"   ✅ Программа работает! Обработано {recent_files_24h:,} файлов за последние 24 часа")
else:
    print(f"   ⚠️  Новых файлов не обнаружено (возможно, данные еще не загружены в ЕИС)")
if total_223 > 0:
    print(f"   ✅ 223-ФЗ: Записей появилось! Было 0, сейчас {total_223:,}")
else:
    print(f"   ⚠️  223-ФЗ: Записей все еще нет (0 записей)")
print(f"{'='*60}\n")

db.close()
