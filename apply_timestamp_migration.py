#!/usr/bin/env python3
"""Скрипт для применения миграции timestamp в file_names_xml"""
import sys
sys.path.insert(0, "/opt/tendermonitor")
from database_work.database_connection import DatabaseManager

db = DatabaseManager()
cur = db.cursor

try:
    # Проверяем, есть ли уже поле processed_at
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'file_names_xml' 
        AND column_name = 'processed_at'
    """)
    has_timestamp = cur.fetchone() is not None
    
    if has_timestamp:
        print("✅ Поле processed_at уже существует в таблице file_names_xml")
    else:
        print("Добавляю поле processed_at в таблицу file_names_xml...")
        
        # Добавляем поле processed_at с дефолтным значением CURRENT_TIMESTAMP
        cur.execute("""
            ALTER TABLE file_names_xml 
            ADD COLUMN processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """)
        
        # Создаем индекс для быстрого поиска по времени обработки
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_names_xml_processed_at 
            ON file_names_xml (processed_at DESC)
        """)
        
        # Обновляем существующие записи текущим временем
        cur.execute("""
            UPDATE file_names_xml 
            SET processed_at = CURRENT_TIMESTAMP 
            WHERE processed_at IS NULL
        """)
        
        db.connection.commit()
        print("✅ Поле processed_at успешно добавлено в таблицу file_names_xml")
        print("✅ Индекс создан")
        print("✅ Существующие записи обновлены")
    
    # Проверяем результат
    cur.execute("SELECT COUNT(*) FROM file_names_xml WHERE processed_at IS NOT NULL")
    count = cur.fetchone()[0]
    print(f"✅ Записей с timestamp: {count:,}")
    
except Exception as e:
    print(f"❌ Ошибка: {e}")
    db.connection.rollback()
    sys.exit(1)
finally:
    db.close()
