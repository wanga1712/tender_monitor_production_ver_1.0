#!/usr/bin/env python3
"""
Скрипт для восстановления БД из дампа и проверки последней даты.
"""

import subprocess
import sys
import os
sys.path.insert(0, "/opt/tendermonitor")

from database_work.database_connection import DatabaseManager
from datetime import datetime, timedelta
from dotenv import load_dotenv

DUMP_FILE = "/opt/tendermonitor/tender_monitor.dump"

# Загружаем данные подключения
env_file = "/opt/tendermonitor/database_work/db_credintials.env"
load_dotenv(dotenv_path=env_file)

DB_NAME = os.getenv("DB_DATABASE", "tender_monitor")
DB_USER = os.getenv("DB_USER", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

def check_dump():
    """Проверяет дамп перед восстановлением"""
    print("=" * 60)
    print("ПРОВЕРКА ДАМПА")
    print("=" * 60)
    
    if not os.path.exists(DUMP_FILE):
        print(f"❌ Файл дампа не найден: {DUMP_FILE}")
        return False
    
    size = os.path.getsize(DUMP_FILE) / (1024 * 1024)  # MB
    print(f"✅ Файл найден: {DUMP_FILE}")
    print(f"   Размер: {size:.2f} MB")
    
    # Проверяем структуру дампа
    try:
        env = os.environ.copy()
        env["PGPASSWORD"] = DB_PASSWORD
        
        result = subprocess.run(
            ["pg_restore", "-l", DUMP_FILE],
            capture_output=True,
            text=True,
            timeout=30,
            env=env
        )
        if result.returncode == 0:
            lines = result.stdout.split('\n')
            table_lines = [l for l in lines if 'TABLE DATA' in l and 'reestr_contract_223_fz' in l]
            if table_lines:
                print(f"✅ Таблица reestr_contract_223_fz найдена в дампе")
                return True
            else:
                print(f"⚠️  Таблица reestr_contract_223_fz не найдена в дампе")
                return False
        else:
            print(f"⚠️  Не удалось прочитать структуру дампа")
            return True  # Все равно пробуем восстановить
    except Exception as e:
        print(f"⚠️  Ошибка проверки дампа: {e}")
        return True

def restore_dump():
    """Восстанавливает дамп БД"""
    print("\n" + "=" * 60)
    print("ВОССТАНОВЛЕНИЕ ДАМПА")
    print("=" * 60)
    
    print(f"\n📋 Подключение к БД:")
    print(f"   Пользователь: {DB_USER}")
    print(f"   Хост: {DB_HOST}:{DB_PORT}")
    print(f"   База данных: {DB_NAME}")
    
    env = os.environ.copy()
    env["PGPASSWORD"] = DB_PASSWORD
    
    try:
        # Создаем резервную копию текущей БД
        backup_file = f"/tmp/backup_before_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.dump"
        print(f"\n📦 Создание резервной копии текущей БД...")
        
        dump_cmd = ["pg_dump", "-Fc", "-h", DB_HOST, "-p", DB_PORT, "-U", DB_USER, "-d", DB_NAME, "-f", backup_file]
        
        result = subprocess.run(
            dump_cmd,
            capture_output=True,
            text=True,
            timeout=300,
            env=env
        )
        
        if result.returncode == 0:
            print(f"✅ Резервная копия создана: {backup_file}")
        else:
            print(f"⚠️  Не удалось создать резервную копию: {result.stderr[:200]}")
        
        # Восстанавливаем дамп
        print(f"\n🔄 Восстановление дампа...")
        print(f"   Это может занять несколько минут...")
        
        restore_cmd = ["pg_restore", "-h", DB_HOST, "-p", DB_PORT, "-U", DB_USER, "-d", DB_NAME, "-v", "-c", DUMP_FILE]
        
        result = subprocess.run(
            restore_cmd,
            capture_output=True,
            text=True,
            timeout=600,
            env=env
        )
        
        if result.returncode == 0:
            print(f"✅ Дамп восстановлен успешно")
            return True
        else:
            print(f"❌ Ошибка восстановления дампа:")
            print(result.stderr[:500])
            # Иногда вывод идет в stdout
            if "ERROR" in result.stdout or "error" in result.stdout.lower():
                print("\nДополнительная информация из stdout:")
                print(result.stdout[:500])
            return False
            
    except Exception as e:
        print(f"❌ Ошибка при восстановлении: {e}")
        import traceback
        traceback.print_exc()
        return False

def restore_relations():
    """Восстанавливает связи таблицы"""
    print("\n" + "=" * 60)
    print("ВОССТАНОВЛЕНИЕ СВЯЗЕЙ")
    print("=" * 60)
    
    relations_file = "/opt/tendermonitor/DB_RELATIONS_223_FZ.sql"
    
    if not os.path.exists(relations_file):
        print(f"⚠️  Файл связей не найден: {relations_file}")
        return False
    
    env = os.environ.copy()
    env["PGPASSWORD"] = DB_PASSWORD
    
    try:
        psql_cmd = ["psql", "-h", DB_HOST, "-p", DB_PORT, "-U", DB_USER, "-d", DB_NAME, "-f", relations_file]
        
        result = subprocess.run(
            psql_cmd,
            capture_output=True,
            text=True,
            timeout=60,
            env=env
        )
        
        # Игнорируем ошибки "already exists"
        errors = [l for l in result.stderr.split('\n') if l.strip() and 'already exists' not in l.lower()]
        
        if result.returncode == 0 or not errors:
            print(f"✅ Связи проверены/восстановлены")
            if errors:
                for err in errors[:5]:  # Показываем только первые 5 ошибок
                    print(f"   ⚠️  {err}")
            return True
        else:
            print(f"❌ Ошибки при восстановлении связей:")
            for err in errors[:5]:
                print(f"   {err}")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка при восстановлении связей: {e}")
        return False

def check_last_date():
    """Проверяет последнюю дату в восстановленной БД"""
    print("\n" + "=" * 60)
    print("ПРОВЕРКА ПОСЛЕДНЕЙ ДАТЫ")
    print("=" * 60)
    
    try:
        db = DatabaseManager()
        cur = db.cursor
        
        # Проверяем количество записей
        cur.execute("SELECT COUNT(*) FROM reestr_contract_223_fz;")
        count = cur.fetchone()[0]
        print(f"\n📊 Всего записей: {count}")
        
        # Проверяем последнюю дату
        cur.execute("SELECT MAX(start_date), MAX(end_date) FROM reestr_contract_223_fz;")
        result = cur.fetchone()
        
        max_start = result[0]
        max_end = result[1]
        
        if max_start:
            print(f"✅ Последняя start_date: {max_start}")
            print(f"✅ Последняя end_date: {max_end}")
            
            # Вычисляем дату для config.ini (день после последней start_date)
            if isinstance(max_start, datetime):
                next_date = max_start.date() + timedelta(days=1)
            else:
                next_date = max_start + timedelta(days=1)
            
            date_str = next_date.strftime('%Y-%m-%d')
            print(f"\n📝 Дата для config.ini: {date_str}")
            
            # Обновляем config.ini
            update_config_date(date_str)
            
            db.close()
            return date_str
        else:
            print(f"⚠️  Не найдено записей с датами")
            db.close()
            return None
            
    except Exception as e:
        print(f"❌ Ошибка при проверке даты: {e}")
        import traceback
        traceback.print_exc()
        return None

def update_config_date(date_str):
    """Обновляет дату в config.ini"""
    config_path = "/opt/tendermonitor/config.ini"
    
    try:
        import configparser
        
        config = configparser.ConfigParser()
        with open(config_path, "r", encoding="utf-8") as f:
            config.read_file(f)
        
        config.set("eis", "date", date_str)
        
        with open(config_path, "w", encoding="utf-8") as f:
            config.write(f)
        
        print(f"✅ config.ini обновлен: date = {date_str}")
        
    except Exception as e:
        print(f"⚠️  Ошибка обновления config.ini: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("ВОССТАНОВЛЕНИЕ БД ИЗ ДАМПА")
    print("=" * 60)
    
    # 1. Проверка дампа
    if not check_dump():
        print("\n❌ Проверка дампа не пройдена. Выход.")
        sys.exit(1)
    
    # 2. Восстановление дампа
    if not restore_dump():
        print("\n❌ Восстановление дампа не удалось. Выход.")
        sys.exit(1)
    
    # 3. Восстановление связей
    restore_relations()
    
    # 4. Проверка последней даты
    last_date = check_last_date()
    
    print("\n" + "=" * 60)
    print("✅ ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)
    
    if last_date:
        print(f"\n📋 Следующие шаги:")
        print(f"   1. Проверьте данные в БД")
        print(f"   2. Дата в config.ini установлена на: {last_date}")
        print(f"   3. Перезапустите приложение")
