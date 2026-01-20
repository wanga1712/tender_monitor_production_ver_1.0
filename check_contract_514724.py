#!/usr/bin/env python3
"""Проверка контракта 514724 на проблемы с данными"""
import sys
sys.path.insert(0, "/opt/tendermonitor")
from database_work.database_connection import DatabaseManager

db = DatabaseManager()
cur = db.cursor

# Получаем данные контракта
cur.execute("""
    SELECT * FROM reestr_contract_44_fz 
    WHERE id = 514724
""")
contract = cur.fetchone()
column_names = [desc[0] for desc in cur.description]

if contract:
    print(f"Контракт найден: ID = {contract[0]}")
    print(f"\nВсего полей: {len(column_names)}")
    print("\nПоля с NULL значениями:")
    null_fields = []
    for i, (name, value) in enumerate(zip(column_names, contract)):
        if value is None:
            null_fields.append(name)
            print(f"  {name}: NULL")
    
    print(f"\nВсего NULL полей: {len(null_fields)}")
    
    # Проверяем структуру таблицы completed
    print("\n" + "="*60)
    print("СТРУКТУРА ТАБЛИЦЫ reestr_contract_44_fz_completed:")
    print("="*60)
    
    cur.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = 'reestr_contract_44_fz_completed'
        ORDER BY ordinal_position
    """)
    completed_columns = cur.fetchall()
    
    not_null_columns = []
    for col in completed_columns:
        col_name, data_type, is_nullable, default = col
        if is_nullable == 'NO' and default is None:
            not_null_columns.append(col_name)
            # Проверяем, есть ли NULL в этом поле у нашего контракта
            if col_name in null_fields:
                print(f"  ⚠️  {col_name}: {data_type} NOT NULL (в контракте NULL!)")
            else:
                print(f"  ✅ {col_name}: {data_type} NOT NULL")
        else:
            print(f"     {col_name}: {data_type} (nullable: {is_nullable})")
    
    print(f"\nNOT NULL полей без default: {len(not_null_columns)}")
    
    # Проверяем внешние ключи
    print("\n" + "="*60)
    print("ВНЕШНИЕ КЛЮЧИ reestr_contract_44_fz_completed:")
    print("="*60)
    
    cur.execute("""
        SELECT
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name = 'reestr_contract_44_fz_completed'
    """)
    fks = cur.fetchall()
    
    for fk in fks:
        constraint_name, column_name, foreign_table, foreign_column = fk
        # Проверяем значение в контракте
        col_idx = column_names.index(column_name) if column_name in column_names else -1
        if col_idx >= 0:
            fk_value = contract[col_idx]
            if fk_value is None:
                print(f"  ⚠️  {column_name} -> {foreign_table}.{foreign_column}: NULL (может быть проблемой)")
            else:
                # Проверяем, существует ли значение в связанной таблице
                cur.execute(f"SELECT COUNT(*) FROM {foreign_table} WHERE {foreign_column} = %s", (fk_value,))
                exists = cur.fetchone()[0] > 0
                if not exists:
                    print(f"  ❌ {column_name} = {fk_value} -> {foreign_table}.{foreign_column}: НЕ СУЩЕСТВУЕТ!")
                else:
                    print(f"  ✅ {column_name} = {fk_value} -> {foreign_table}.{foreign_column}: OK")
        else:
            print(f"  {column_name} -> {foreign_table}.{foreign_column}")
    
    # Пробуем вставить вручную
    print("\n" + "="*60)
    print("ПОПЫТКА РУЧНОЙ ВСТАВКИ:")
    print("="*60)
    
    try:
        columns = ', '.join(column_names)
        placeholders = ', '.join(['%s'] * len(column_names))
        insert_query = f"""
            INSERT INTO reestr_contract_44_fz_completed ({columns})
            VALUES ({placeholders})
        """
        cur.execute(insert_query, contract)
        db.connection.commit()
        print("✅ Вставка успешна!")
    except Exception as e:
        print(f"❌ Ошибка вставки: {e}")
        db.connection.rollback()
else:
    print("Контракт не найден!")

db.close()
