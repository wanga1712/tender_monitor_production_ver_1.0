"""
MODULE: scripts.apply_migration_simple
RESPONSIBILITY: Simple migration application (structure only).
ALLOWED: psycopg2, psycopg2.extras, os, dotenv, sys.
FORBIDDEN: None.
ERRORS: None.

ПРОСТАЯ миграция - только создание структуры, БЕЗ обновления данных
Используйте этот скрипт, если основной зависает
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import sys

load_dotenv()

def apply_migration_simple():
    """Простая миграция - только структура"""
    try:
        print("=" * 70)
        print("ПРОСТАЯ МИГРАЦИЯ: Только структура (без обновления данных)")
        print("=" * 70)
        
        # Подключение с таймаутом
        conn = psycopg2.connect(
            host=os.getenv("TENDER_MONITOR_DB_HOST"),
            database=os.getenv("TENDER_MONITOR_DB_DATABASE"),
            user=os.getenv("TENDER_MONITOR_DB_USER"),
            password=os.getenv("TENDER_MONITOR_DB_PASSWORD"),
            port=os.getenv("TENDER_MONITOR_DB_PORT", "5432"),
            connect_timeout=10
        )
        conn.set_session(autocommit=True)  # Автокоммит для избежания блокировок
        cursor = conn.cursor()
        
        print("\nШАГ 1: Создание таблицы статусов...")
        sys.stdout.flush()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tender_statuses (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL UNIQUE,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("✅ Таблица tender_statuses создана")
        sys.stdout.flush()
        
        print("\nШАГ 2: Вставка статусов...")
        sys.stdout.flush()
        
        cursor.execute("""
            INSERT INTO tender_statuses (id, name, description) VALUES
                (1, 'Новая', 'Закупка с end_date NOT NULL и end_date <= CURRENT_DATE'),
                (2, 'Работа комиссии', 'Закупка с end_date > CURRENT_DATE и end_date <= CURRENT_DATE + 90 дней'),
                (3, 'Разыграна', 'Закупка с delivery_end_date NOT NULL и delivery_end_date >= CURRENT_DATE + 90 дней'),
                (4, 'Плохие', 'Закупка с delivery_end_date IS NULL (44ФЗ) или end_date > CURRENT_DATE + 180 дней (223ФЗ)')
            ON CONFLICT (id) DO NOTHING;
        """)
        print("✅ Статусы вставлены")
        sys.stdout.flush()
        
        cursor.execute("SELECT setval('tender_statuses_id_seq', (SELECT MAX(id) FROM tender_statuses), true);")
        
        print("\nШАГ 3: Добавление столбца status_id в reestr_contract_44_fz...")
        sys.stdout.flush()
        
        cursor.execute("ALTER TABLE reestr_contract_44_fz ADD COLUMN IF NOT EXISTS status_id INTEGER;")
        print("✅ Столбец добавлен")
        sys.stdout.flush()
        
        print("\nШАГ 4: Создание внешнего ключа для 44ФЗ...")
        sys.stdout.flush()
        
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conname = 'fk_reestr_contract_44_fz_status_id'
                ) THEN
                    ALTER TABLE reestr_contract_44_fz
                    ADD CONSTRAINT fk_reestr_contract_44_fz_status_id
                    FOREIGN KEY (status_id) REFERENCES tender_statuses(id);
                END IF;
            END $$;
        """)
        print("✅ Внешний ключ создан")
        sys.stdout.flush()
        
        print("\nШАГ 5: Добавление столбца status_id в reestr_contract_223_fz...")
        sys.stdout.flush()
        
        cursor.execute("ALTER TABLE reestr_contract_223_fz ADD COLUMN IF NOT EXISTS status_id INTEGER;")
        print("✅ Столбец добавлен")
        sys.stdout.flush()
        
        print("\nШАГ 6: Создание внешнего ключа для 223ФЗ...")
        sys.stdout.flush()
        
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conname = 'fk_reestr_contract_223_fz_status_id'
                ) THEN
                    ALTER TABLE reestr_contract_223_fz
                    ADD CONSTRAINT fk_reestr_contract_223_fz_status_id
                    FOREIGN KEY (status_id) REFERENCES tender_statuses(id);
                END IF;
            END $$;
        """)
        print("✅ Внешний ключ создан")
        sys.stdout.flush()
        
        print("\n" + "=" * 70)
        print("✅ СТРУКТУРА СОЗДАНА УСПЕШНО!")
        print("=" * 70)
        print("\nСтолбцы status_id созданы, но пока пустые (NULL).")
        print("Для заполнения статусов запустите отдельный скрипт обновления.")
        
        cursor.close()
        conn.close()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    apply_migration_simple()

