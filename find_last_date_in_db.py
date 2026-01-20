#!/usr/bin/env python3
"""Скрипт для поиска последней даты в таблице reestr_contract_223_fz"""

import sys
from datetime import datetime
sys.path.insert(0, "/opt/tendermonitor")

from database_work.database_connection import DatabaseManager

def find_last_date():
    """Находит последнюю дату в таблице reestr_contract_223_fz"""
    
    db = DatabaseManager()
    conn = db.connection
    cur = conn.cursor()
    
    print("=" * 60)
    print("ПОИСК ПОСЛЕДНЕЙ ДАТЫ В БД")
    print("=" * 60)
    
    # Проверяем разные возможные поля с датами
    date_fields = [
        'publish_date',
        'create_date',
        'date',
        'publication_date',
        'sign_date',
        'registration_date'
    ]
    
    # Сначала проверим, какие колонки есть в таблице
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'reestr_contract_223_fz'
            AND table_schema = 'public'
        ORDER BY ordinal_position;
    """)
    
    columns = [row[0] for row in cur.fetchall()]
    print(f"\nКолонки в таблице ({len(columns)}):")
    for col in columns:
        print(f"  • {col}")
    
    # Ищем дату в разных полях
    last_date = None
    last_date_field = None
    
    for field in date_fields:
        if field in columns:
            try:
                # Пробуем разные форматы
                queries = [
                    f"SELECT MAX({field}) FROM reestr_contract_223_fz WHERE {field} IS NOT NULL;",
                    f"SELECT MAX(CAST({field} AS DATE)) FROM reestr_contract_223_fz WHERE {field} IS NOT NULL;",
                ]
                
                for query in queries:
                    try:
                        cur.execute(query)
                        result = cur.fetchone()
                        if result and result[0]:
                            date_value = result[0]
                            if isinstance(date_value, str):
                                try:
                                    date_value = datetime.strptime(date_value.split()[0], '%Y-%m-%d').date()
                                except:
                                    pass
                            if date_value:
                                if not last_date or (isinstance(date_value, datetime) and date_value > last_date):
                                    last_date = date_value
                                    last_date_field = field
                                    break
                    except:
                        continue
            except Exception as e:
                print(f"  ⚠️  Ошибка проверки поля {field}: {e}")
                continue
    
    # Если не нашли по полям, пробуем найти по всем датам
    if not last_date:
        print("\n⚠️  Не удалось найти дату по стандартным полям, проверяем все колонки...")
        for col in columns:
            if 'date' in col.lower() or 'time' in col.lower():
                try:
                    cur.execute(f"SELECT MAX({col}) FROM reestr_contract_223_fz WHERE {col} IS NOT NULL LIMIT 1;")
                    result = cur.fetchone()
                    if result and result[0]:
                        print(f"  Проверено поле {col}: {result[0]}")
                except:
                    pass
    
    # Также проверим количество записей
    cur.execute("SELECT COUNT(*) FROM reestr_contract_223_fz;")
    count = cur.fetchone()[0]
    print(f"\n📊 Всего записей в таблице: {count}")
    
    if last_date:
        print(f"\n✅ Последняя дата найдена!")
        print(f"   Поле: {last_date_field}")
        print(f"   Дата: {last_date}")
        if isinstance(last_date, datetime):
            date_str = last_date.date().strftime('%Y-%m-%d')
        else:
            date_str = str(last_date)
        print(f"\n📝 Дата для config.ini: {date_str}")
    else:
        print("\n⚠️  Не удалось определить последнюю дату автоматически")
        print("   Нужно проверить вручную")
    
    cur.close()
    db.close()
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    try:
        find_last_date()
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
