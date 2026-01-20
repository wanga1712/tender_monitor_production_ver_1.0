#!/usr/bin/env python3
"""Скрипт для очистки БД: удаление дубликатов регионов и Django-таблиц"""

import sys
sys.path.insert(0, "/opt/tendermonitor")
from database_work.database_connection import DatabaseManager

db = DatabaseManager()
cur = db.cursor

print("=" * 60)
print("ОЧИСТКА БД")
print("=" * 60)

# 1. Удаление дубликатов регионов
print("\n1. Удаление дубликатов регионов...")
cur.execute("""
    DELETE FROM region 
    WHERE id NOT IN (
        SELECT MIN(id) 
        FROM region 
        GROUP BY code
    )
""")
deleted_duplicates = cur.rowcount
db.connection.commit()
print(f"   ✅ Удалено дубликатов: {deleted_duplicates}")

# Проверяем результат
cur.execute("SELECT COUNT(*) FROM region")
count_after = cur.fetchone()[0]
print(f"   ✅ Осталось регионов: {count_after}")

# 2. Находим Django-таблицы
print("\n2. Поиск Django-таблиц...")
django_tables = [
    "auth_group",
    "auth_group_permissions",
    "auth_permission",
    "auth_user",
    "auth_user_groups",
    "auth_user_user_permissions",
    "django_admin_log",
    "django_content_type",
    "django_migrations",
    "django_session",
    "users",
    "okpd_categories",
    "okpd_from_users",
    "stop_words_names",
    "sales_deals",
    "sales_pipeline_stages",
    "contact",
    "contact_link",
    "contractor_role",
    "tender_document_matches",
    "tender_document_match_details",
]

# Проверяем какие таблицы существуют
existing_tables = []
for table in django_tables:
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        )
    """, (table,))
    if cur.fetchone()[0]:
        existing_tables.append(table)
        print(f"   ✓ Найдена таблица: {table}")

print(f"\n   ✅ Всего найдено Django-таблиц: {len(existing_tables)}")

# 3. Удаляем Django-таблицы (CASCADE удалит и зависимости)
print("\n3. Удаление Django-таблиц...")

# Отключаем проверку внешних ключей для текущей сессии
cur.execute("SET session_replication_role = 'replica';")

deleted_count = 0
for table in existing_tables:
    try:
        # Удаляем таблицу с CASCADE (удалит и зависимости)
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        print(f"   ✅ Удалена таблица: {table}")
        deleted_count += 1
    except Exception as e:
        print(f"   ❌ Ошибка удаления таблицы {table}: {e}")

# Включаем обратно проверку внешних ключей
cur.execute("SET session_replication_role = 'origin';")
db.connection.commit()

print(f"\n   ✅ Удалено Django-таблиц: {deleted_count}")

# 4. Проверяем оставшиеся таблицы
print("\n4. Оставшиеся таблицы в БД:")
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE'
    ORDER BY table_name
""")
remaining_tables = [row[0] for row in cur.fetchall()]
for table in remaining_tables:
    print(f"   • {table}")

print(f"\n   ✅ Всего оставшихся таблиц: {len(remaining_tables)}")

db.close()

print("\n" + "=" * 60)
print("✅ ОЧИСТКА ЗАВЕРШЕНА")
print("=" * 60)
