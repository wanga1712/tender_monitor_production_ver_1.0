#!/usr/bin/env python3
"""Проверка статуса миграции контрактов"""
import os
import sys
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent))

from database_work.database_connection import DatabaseManager
from dotenv import load_dotenv

# Загружаем переменные окружения
env_path = Path(__file__).parent / "database_work" / "db_credintials.env"
load_dotenv(env_path)

def check_migration_status():
    db = DatabaseManager()
    try:
        print("=" * 60)
        print("ПРОВЕРКА СТАТУСА МИГРАЦИИ КОНТРАКТОВ")
        print("=" * 60)
        
        # 1. Завершенные контракты в основной таблице
        db.cursor.execute("""
            SELECT COUNT(*) FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL 
            AND delivery_end_date < CURRENT_DATE;
        """)
        completed_in_main = db.cursor.fetchone()[0]
        print(f"\n1. Завершенных контрактов в основной таблице (reestr_contract_44_fz): {completed_in_main:,}")
        
        # 2. Всего контрактов в таблице completed
        db.cursor.execute("""
            SELECT COUNT(*) FROM reestr_contract_44_fz_completed;
        """)
        total_in_completed = db.cursor.fetchone()[0]
        print(f"2. Всего контрактов в таблице completed: {total_in_completed:,}")
        
        # 3. Дубликаты (контракты, которые есть в обеих таблицах)
        db.cursor.execute("""
            SELECT COUNT(*) FROM reestr_contract_44_fz r
            INNER JOIN reestr_contract_44_fz_completed c ON r.id = c.id
            WHERE r.delivery_end_date IS NOT NULL 
            AND r.delivery_end_date < CURRENT_DATE;
        """)
        duplicates = db.cursor.fetchone()[0]
        print(f"3. ДУБЛИКАТЫ (есть в обеих таблицах): {duplicates:,}")
        
        # 4. Контракты, которые должны быть мигрированы (есть в основной, но нет в completed)
        db.cursor.execute("""
            SELECT COUNT(*) FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL 
            AND delivery_end_date < CURRENT_DATE
            AND id NOT IN (SELECT id FROM reestr_contract_44_fz_completed);
        """)
        should_be_migrated = db.cursor.fetchone()[0]
        print(f"4. Контракты, которые должны быть мигрированы: {should_be_migrated:,}")
        
        # 5. Контракты в completed, которых нет в основной (уже удалены)
        db.cursor.execute("""
            SELECT COUNT(*) FROM reestr_contract_44_fz_completed c
            WHERE c.id NOT IN (
                SELECT id FROM reestr_contract_44_fz
                WHERE delivery_end_date IS NOT NULL 
                AND delivery_end_date < CURRENT_DATE
            );
        """)
        already_deleted = db.cursor.fetchone()[0]
        print(f"5. Контракты в completed, которых уже нет в основной: {already_deleted:,}")
        
        print("\n" + "=" * 60)
        print("АНАЛИЗ:")
        print("=" * 60)
        
        if duplicates > 0:
            print(f"⚠️  ПРОБЛЕМА: Найдено {duplicates:,} дубликатов!")
            print("   Контракты были вставлены в completed, но НЕ удалены из основной таблицы.")
            print(f"   Нужно удалить {duplicates:,} контрактов из основной таблицы.")
        else:
            print("✅ Дубликатов нет - миграция выполнена корректно")
        
        if should_be_migrated > 0:
            print(f"ℹ️  Есть {should_be_migrated:,} контрактов, которые еще нужно мигрировать")
        
        print("=" * 60)
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    check_migration_status()
