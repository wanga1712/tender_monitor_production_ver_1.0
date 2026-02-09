"""
MODULE: scripts.analyze_tender_db
RESPONSIBILITY: Analyzing tender database structure and content.
ALLOWED: psycopg2, os, dotenv, typing, json, datetime.
FORBIDDEN: None.
ERRORS: None.

Скрипт для анализа структуры базы данных tender_monitor

Анализирует:
- Список всех таблиц
- Структуру каждой таблицы (колонки, типы данных)
- Примеры данных из каждой таблицы
- Связи между таблицами (foreign keys)
- Индексы
- Возможности поиска
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from typing import List, Dict, Any
import json
from datetime import datetime

# Загружаем переменные окружения
load_dotenv()


def get_tender_db_connection():
    """Получение подключения к базе данных tender_monitor"""
    # Получаем параметры подключения из переменных окружения (без значений по умолчанию)
    host = os.getenv("TENDER_MONITOR_DB_HOST")
    database = os.getenv("TENDER_MONITOR_DB_DATABASE")
    user = os.getenv("TENDER_MONITOR_DB_USER")
    password = os.getenv("TENDER_MONITOR_DB_PASSWORD")
    port = os.getenv("TENDER_MONITOR_DB_PORT")
    
    # Проверяем, что все обязательные параметры заданы
    if not all([host, database, user, password, port]):
        missing = []
        if not host:
            missing.append("TENDER_MONITOR_DB_HOST")
        if not database:
            missing.append("TENDER_MONITOR_DB_DATABASE")
        if not user:
            missing.append("TENDER_MONITOR_DB_USER")
        if not password:
            missing.append("TENDER_MONITOR_DB_PASSWORD")
        if not port:
            missing.append("TENDER_MONITOR_DB_PORT")
        
        print("ОШИБКА: Не заданы обязательные переменные окружения в .env файле:")
        for var in missing:
            print(f"  - {var}")
        print("\nУбедитесь, что файл .env существует и содержит все необходимые переменные.")
        return None
    
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=int(port)
        )
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None


def get_all_tables(conn) -> List[str]:
    """Получение списка всех таблиц в базе данных"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        return [row[0] for row in cur.fetchall()]


def get_table_structure(conn, table_name: str) -> List[Dict[str, Any]]:
    """Получение структуры таблицы (колонки, типы, ограничения)"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        return [dict(row) for row in cur.fetchall()]


def get_table_primary_keys(conn, table_name: str) -> List[str]:
    """Получение первичных ключей таблицы"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = 'public'
            AND tc.table_name = %s
            AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position;
        """, (table_name,))
        return [row[0] for row in cur.fetchall()]


def get_table_foreign_keys(conn, table_name: str) -> List[Dict[str, Any]]:
    """Получение внешних ключей таблицы"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                tc.constraint_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
            AND tc.table_name = %s;
        """, (table_name,))
        return [dict(row) for row in cur.fetchall()]


def get_table_indexes(conn, table_name: str) -> List[Dict[str, Any]]:
    """Получение индексов таблицы"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
            AND tablename = %s;
        """, (table_name,))
        return [dict(row) for row in cur.fetchall()]


def get_table_sample_data(conn, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Получение примеров данных из таблицы"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(f"SELECT * FROM {table_name} LIMIT %s;", (limit,))
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            return [{"error": str(e)}]


def get_table_row_count(conn, table_name: str) -> int:
    """Получение количества строк в таблице"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        return cur.fetchone()[0]


def analyze_database():
    """Основная функция анализа базы данных"""
    print("=" * 80)
    print("АНАЛИЗ БАЗЫ ДАННЫХ TENDER_MONITOR")
    print("=" * 80)
    print(f"Время анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    conn = get_tender_db_connection()
    if not conn:
        print("Не удалось подключиться к базе данных!")
        return
    
    try:
        # Получаем список всех таблиц
        tables = get_all_tables(conn)
        print(f"Найдено таблиц: {len(tables)}")
        print()
        
        # Анализируем каждую таблицу
        analysis_result = {
            "database": "tender_monitor",
            "analysis_date": datetime.now().isoformat(),
            "tables": []
        }
        
        for table_name in tables:
            print("-" * 80)
            print(f"ТАБЛИЦА: {table_name}")
            print("-" * 80)
            
            # Структура таблицы
            structure = get_table_structure(conn, table_name)
            print(f"\nКолонки ({len(structure)}):")
            for col in structure:
                col_type = col['data_type']
                if col['character_maximum_length']:
                    col_type += f"({col['character_maximum_length']})"
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                print(f"  - {col['column_name']}: {col_type} {nullable}{default}")
            
            # Первичные ключи
            primary_keys = get_table_primary_keys(conn, table_name)
            if primary_keys:
                print(f"\nПервичные ключи: {', '.join(primary_keys)}")
            
            # Внешние ключи
            foreign_keys = get_table_foreign_keys(conn, table_name)
            if foreign_keys:
                print(f"\nВнешние ключи ({len(foreign_keys)}):")
                for fk in foreign_keys:
                    print(f"  - {fk['column_name']} -> {fk['foreign_table_name']}.{fk['foreign_column_name']}")
            
            # Индексы
            indexes = get_table_indexes(conn, table_name)
            if indexes:
                print(f"\nИндексы ({len(indexes)}):")
                for idx in indexes:
                    print(f"  - {idx['indexname']}")
            
            # Количество строк
            row_count = get_table_row_count(conn, table_name)
            print(f"\nКоличество строк: {row_count}")
            
            # Примеры данных
            if row_count > 0:
                sample_data = get_table_sample_data(conn, table_name, limit=3)
                print(f"\nПримеры данных (первые 3 записи):")
                for i, row in enumerate(sample_data, 1):
                    if 'error' not in row:
                        print(f"  Запись {i}:")
                        for key, value in list(row.items())[:5]:  # Показываем первые 5 полей
                            value_str = str(value)
                            if len(value_str) > 50:
                                value_str = value_str[:47] + "..."
                            print(f"    {key}: {value_str}")
                        if len(row) > 5:
                            print(f"    ... и еще {len(row) - 5} полей")
                    else:
                        print(f"  Ошибка получения данных: {row['error']}")
            
            # Сохраняем информацию о таблице
            table_info = {
                "name": table_name,
                "columns": structure,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys,
                "indexes": [idx['indexname'] for idx in indexes],
                "row_count": row_count,
                "sample_data_count": len(sample_data) if row_count > 0 else 0
            }
            analysis_result["tables"].append(table_info)
            
            print()
        
        # Сохраняем результат в JSON файл
        output_file = f"tender_db_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_result, f, ensure_ascii=False, indent=2, default=str)
        
        print("=" * 80)
        print(f"Анализ завершен. Результаты сохранены в файл: {output_file}")
        print("=" * 80)
        
    except Exception as e:
        print(f"Ошибка при анализе: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == "__main__":
    analyze_database()

