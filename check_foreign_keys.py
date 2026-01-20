#!/usr/bin/env python3
"""Проверка внешних ключей, ссылающихся на reestr_contract_44_fz"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from database_work.database_connection import DatabaseManager
from dotenv import load_dotenv

env_path = Path(__file__).parent / "database_work" / "db_credintials.env"
load_dotenv(env_path)

def check_foreign_keys():
    db = DatabaseManager()
    try:
        print("=" * 60)
        print("ПРОВЕРКА ВНЕШНИХ КЛЮЧЕЙ")
        print("=" * 60)
        
        # Находим все внешние ключи, которые ссылаются на reestr_contract_44_fz
        db.cursor.execute("""
            SELECT
                tc.table_name AS referencing_table,
                kcu.column_name AS referencing_column,
                ccu.table_name AS referenced_table,
                ccu.column_name AS referenced_column,
                tc.constraint_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND ccu.table_name = 'reestr_contract_44_fz'
            ORDER BY tc.table_name, kcu.column_name;
        """)
        
        fks = db.cursor.fetchall()
        
        if fks:
            print(f"\nНайдено {len(fks)} внешних ключей, ссылающихся на reestr_contract_44_fz:\n")
            for fk in fks:
                print(f"  Таблица: {fk[0]}")
                print(f"    Колонка: {fk[1]}")
                print(f"    Ссылается на: {fk[2]}.{fk[3]}")
                print(f"    Имя ограничения: {fk[4]}")
                print()
        else:
            print("\nВнешних ключей, ссылающихся на reestr_contract_44_fz, не найдено")
        
        # Проверяем, есть ли записи в таблицах, которые ссылаются на удаляемые контракты
        print("\n" + "=" * 60)
        print("ПРОВЕРКА ССЫЛАЮЩИХСЯ ЗАПИСЕЙ")
        print("=" * 60)
        
        if fks:
            for fk in fks:
                table_name = fk[0]
                column_name = fk[1]
                
                # Проверяем количество ссылок на завершенные контракты
                db.cursor.execute(f"""
                    SELECT COUNT(*) FROM {table_name} t
                    INNER JOIN reestr_contract_44_fz r ON t.{column_name} = r.id
                    INNER JOIN reestr_contract_44_fz_completed c ON r.id = c.id
                    WHERE r.delivery_end_date IS NOT NULL 
                    AND r.delivery_end_date < CURRENT_DATE;
                """)
                count = db.cursor.fetchone()[0]
                if count > 0:
                    print(f"  {table_name}.{column_name}: {count:,} ссылок на завершенные контракты")
        
        print("=" * 60)
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    check_foreign_keys()
