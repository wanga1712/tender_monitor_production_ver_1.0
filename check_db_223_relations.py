#!/usr/bin/env python3
"""Скрипт для проверки связей таблицы reestr_contract_223_fz"""

import sys
sys.path.insert(0, "/opt/tendermonitor")

from database_work.database_connection import DatabaseManager

def check_table_relations():
    """Проверяет все связи (foreign keys) таблицы reestr_contract_223_fz"""
    
    db = DatabaseManager()
    conn = db.connection
    cur = conn.cursor()
    
    print("=" * 60)
    print("ПРОВЕРКА СВЯЗЕЙ ТАБЛИЦЫ reestr_contract_223_fz")
    print("=" * 60)
    
    # Получаем все foreign keys для таблицы
    query = """
    SELECT
        tc.constraint_name,
        tc.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        rc.update_rule,
        rc.delete_rule
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
    JOIN information_schema.referential_constraints AS rc
        ON tc.constraint_name = rc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name = 'reestr_contract_223_fz'
        AND tc.table_schema = 'public'
    ORDER BY tc.constraint_name;
    """
    
    cur.execute(query)
    relations = cur.fetchall()
    
    if not relations:
        print("\n⚠️  Связи (Foreign Keys) не найдены!")
    else:
        print(f"\n✅ Найдено связей: {len(relations)}\n")
        for rel in relations:
            constraint_name, table_name, column_name, foreign_table, foreign_column, update_rule, delete_rule = rel
            print(f"Связь: {constraint_name}")
            print(f"  Таблица: {table_name}")
            print(f"  Колонка: {column_name}")
            print(f"  Ссылается на: {foreign_table}.{foreign_column}")
            print(f"  ON UPDATE: {update_rule}")
            print(f"  ON DELETE: {delete_rule}")
            print()
    
    # Также проверим структуру таблицы
    print("\n" + "=" * 60)
    print("СТРУКТУРА ТАБЛИЦЫ reestr_contract_223_fz")
    print("=" * 60)
    
    query = """
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_name = 'reestr_contract_223_fz'
        AND table_schema = 'public'
    ORDER BY ordinal_position;
    """
    
    cur.execute(query)
    columns = cur.fetchall()
    
    print(f"\nКолонки ({len(columns)}):\n")
    for col in columns:
        col_name, data_type, is_nullable, default = col
        nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
        default_str = f" DEFAULT {default}" if default else ""
        print(f"  • {col_name:30} {data_type:20} {nullable}{default_str}")
    
    # Проверяем индексы
    print("\n" + "=" * 60)
    print("ИНДЕКСЫ ТАБЛИЦЫ reestr_contract_223_fz")
    print("=" * 60)
    
    query = """
    SELECT indexname, indexdef
    FROM pg_indexes
    WHERE tablename = 'reestr_contract_223_fz'
        AND schemaname = 'public';
    """
    
    cur.execute(query)
    indexes = cur.fetchall()
    
    if indexes:
        print(f"\nНайдено индексов: {len(indexes)}\n")
        for idx in indexes:
            print(f"  • {idx[0]}")
            print(f"    {idx[1]}\n")
    else:
        print("\n⚠️  Индексы не найдены!")
    
    cur.close()
    db.close()
    
    print("=" * 60)
    print("ПРОВЕРКА ЗАВЕРШЕНА")
    print("=" * 60)

if __name__ == "__main__":
    try:
        check_table_relations()
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
