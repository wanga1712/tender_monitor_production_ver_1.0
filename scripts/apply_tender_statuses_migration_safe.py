"""
MODULE: scripts.apply_tender_statuses_migration_safe
RESPONSIBILITY: Safe application of tender statuses migration with detailed logging.
ALLOWED: psycopg2, psycopg2.extras, os, dotenv, pathlib, loguru, time.
FORBIDDEN: None.
ERRORS: None.

Безопасная миграция статусов закупок с батчами и подробным логированием

Выполняет миграцию по шагам:
1. Создание таблицы статусов
2. Создание столбцов status_id
3. Обновление статусов батчами с прогрессом
4. Создание индексов
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from pathlib import Path
from loguru import logger
import time

# Настройка логирования
logger.add("logs/migration.log", rotation="10 MB", level="INFO")

# Загружаем переменные окружения
load_dotenv()

# Размер батча для обновления
BATCH_SIZE = 10000  # Обновляем по 10k записей за раз (меньше для больших таблиц)


def get_tender_db_connection():
    """Получение подключения к базе данных tender_monitor"""
    host = os.getenv("TENDER_MONITOR_DB_HOST")
    database = os.getenv("TENDER_MONITOR_DB_DATABASE")
    user = os.getenv("TENDER_MONITOR_DB_USER")
    password = os.getenv("TENDER_MONITOR_DB_PASSWORD")
    port = os.getenv("TENDER_MONITOR_DB_PORT", "5432")
    
    if not all([host, database, user, password]):
        raise ValueError(
            "Не все параметры подключения к БД tender_monitor заданы в .env файле. "
            "Требуются: TENDER_MONITOR_DB_HOST, TENDER_MONITOR_DB_DATABASE, "
            "TENDER_MONITOR_DB_USER, TENDER_MONITOR_DB_PASSWORD"
        )
    
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        logger.info(f"Успешное подключение к БД {database}")
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        raise


def count_records_to_update(cursor, table_name: str, condition: str):
    """Подсчет записей, которые нужно обновить (приблизительно)"""
    try:
        # Используем TABLESAMPLE для быстрой оценки
        cursor.execute(f"""
            SELECT COUNT(*)::bigint as count
            FROM {table_name}
            TABLESAMPLE SYSTEM (1)
            WHERE {condition}
        """)
        sample_count = cursor.fetchone()[0]
        # Умножаем на 100 для приблизительной оценки
        estimated = sample_count * 100
        return estimated
    except:
        # Если не получилось, возвращаем None
        return None


def update_status_batched(cursor, table_name: str, status_id: int, condition: str, status_name: str):
    """
    Обновление статусов батчами с логированием прогресса
    
    Args:
        cursor: Курсор БД
        table_name: Имя таблицы
        status_id: ID статуса для присвоения
        condition: SQL условие для WHERE (без WHERE)
        status_name: Название статуса для логирования
    """
    total_updated = 0
    batch_num = 0
    
    print(f"\n🔄 Обновление статуса '{status_name}' (status_id={status_id}) для {table_name}...")
    logger.info(f"Начало обновления статуса '{status_name}' для {table_name}")
    
    # Показываем приблизительное количество записей
    estimated = count_records_to_update(cursor, table_name, condition)
    if estimated:
        print(f"  Приблизительно записей для обновления: ~{estimated:,}")
    
    start_time_total = time.time()
    
    while True:
        batch_num += 1
        start_time = time.time()
        
        # Используем более эффективный запрос с использованием CTE и UPDATE
        query = f"""
            UPDATE {table_name} r
            SET status_id = %s
            WHERE r.id IN (
                SELECT id FROM {table_name}
                WHERE {condition}
                LIMIT {BATCH_SIZE}
            )
        """
        
        cursor.execute(query, (status_id,))
        updated = cursor.rowcount
        
        if updated == 0:
            break
        
        total_updated += updated
        elapsed = time.time() - start_time
        elapsed_total = time.time() - start_time_total
        rate = updated / elapsed if elapsed > 0 else 0
        
        # Вычисляем процент, если есть оценка
        percent = ""
        if estimated and estimated > 0:
            percent_val = min(100, (total_updated / estimated) * 100)
            percent = f", ~{percent_val:.1f}%"
        
        # Оценка оставшегося времени
        eta = ""
        if rate > 0 and estimated and total_updated < estimated:
            remaining = estimated - total_updated
            eta_seconds = remaining / rate
            if eta_seconds < 60:
                eta = f", осталось ~{eta_seconds:.0f} сек"
            elif eta_seconds < 3600:
                eta = f", осталось ~{eta_seconds/60:.1f} мин"
            else:
                eta = f", осталось ~{eta_seconds/3600:.1f} час"
        
        print(
            f"  Батч #{batch_num}: обновлено {updated:,} записей "
            f"(всего: {total_updated:,}{percent}, время: {elapsed:.2f} сек, "
            f"скорость: {rate:,.0f} записей/сек{eta})"
        )
        logger.info(
            f"Батч #{batch_num}: обновлено {updated:,} записей "
            f"(всего: {total_updated:,}, скорость: {rate:,.0f} записей/сек)"
        )
        
        # Коммитим после каждого батча
        cursor.connection.commit()
        
        # Небольшая пауза для снижения нагрузки каждые 5 батчей
        if batch_num % 5 == 0:
            time.sleep(0.05)
    
    elapsed_total = time.time() - start_time_total
    print(f"✅ Статус '{status_name}' присвоен {total_updated:,} записям за {elapsed_total/60:.1f} минут")
    logger.info(f"Завершено обновление статуса '{status_name}': {total_updated:,} записей за {elapsed_total/60:.1f} минут")
    return total_updated


def apply_migration_safe(conn):
    """Безопасное применение миграции по шагам"""
    cursor = conn.cursor()
    conn.autocommit = False
    
    try:
        print("\n" + "=" * 70)
        print("ШАГ 1: Создание таблицы статусов")
        print("=" * 70)
        
        print("Выполнение CREATE TABLE...")
        logger.info("Начало создания таблицы tender_statuses")
        
        # Создаем таблицу статусов
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tender_statuses (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) NOT NULL UNIQUE,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("  ✅ CREATE TABLE выполнен")
            logger.info("CREATE TABLE выполнен успешно")
            
            conn.commit()
            print("  ✅ COMMIT выполнен")
            logger.info("COMMIT выполнен успешно")
            
            print("✅ Таблица tender_statuses создана")
            logger.info("Таблица tender_statuses создана")
        except Exception as e:
            print(f"  ❌ Ошибка при создании таблицы: {e}")
            logger.error(f"Ошибка при создании таблицы: {e}", exc_info=True)
            raise
        
        # Вставляем статусы
        print("\nВставка статусов в таблицу...")
        logger.info("Начало вставки статусов")
        try:
            cursor.execute("""
                INSERT INTO tender_statuses (id, name, description) VALUES
                    (1, 'Новая', 'Закупка с end_date NOT NULL и end_date <= CURRENT_DATE (завершилась до текущей даты)'),
                    (2, 'Работа комиссии', 'Закупка с end_date > CURRENT_DATE и end_date <= CURRENT_DATE + 90 дней (завершится в ближайшие 90 дней)'),
                    (3, 'Разыграна', 'Закупка с delivery_end_date NOT NULL и delivery_end_date >= CURRENT_DATE + 90 дней (конец поставки не ранее чем через 90 дней)'),
                    (4, 'Плохие', 'Закупка с delivery_end_date IS NULL (44ФЗ) или end_date > CURRENT_DATE + 180 дней (223ФЗ)')
                ON CONFLICT (id) DO NOTHING;
            """)
            print("  ✅ INSERT выполнен")
            logger.info("INSERT статусов выполнен")
            
            cursor.execute("SELECT setval('tender_statuses_id_seq', (SELECT MAX(id) FROM tender_statuses), true);")
            print("  ✅ setval выполнен")
            logger.info("setval выполнен")
            
            conn.commit()
            print("  ✅ COMMIT выполнен")
            logger.info("COMMIT выполнен")
        except Exception as e:
            print(f"  ❌ Ошибка при вставке статусов: {e}")
            logger.error(f"Ошибка при вставке статусов: {e}", exc_info=True)
            raise
        
        # Проверяем, что статусы созданы
        cursor.execute("SELECT id, name FROM tender_statuses ORDER BY id")
        statuses = cursor.fetchall()
        print(f"✅ Создано статусов: {len(statuses)}")
        for s in statuses:
            print(f"   - {s[0]}: {s[1]}")
        logger.info(f"Статусы созданы: {len(statuses)}")
        
        print("\n" + "=" * 70)
        print("ШАГ 2: Добавление столбца status_id в reestr_contract_44_fz")
        print("=" * 70)
        
        # Добавляем столбец
        cursor.execute("ALTER TABLE reestr_contract_44_fz ADD COLUMN IF NOT EXISTS status_id INTEGER;")
        conn.commit()
        print("✅ Столбец status_id добавлен в reestr_contract_44_fz")
        logger.info("Столбец status_id добавлен в reestr_contract_44_fz")
        
        # Проверяем, что столбец создан
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'reestr_contract_44_fz' 
              AND column_name = 'status_id'
        """)
        col = cursor.fetchone()
        if col:
            print(f"   Проверка: столбец существует (тип: {col[1]})")
        else:
            raise Exception("Столбец status_id не был создан!")
        
        # Добавляем внешний ключ
        print("\nСоздание внешнего ключа...")
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
        conn.commit()
        print("✅ Внешний ключ создан")
        logger.info("Внешний ключ для reestr_contract_44_fz создан")
        
        print("\n" + "=" * 70)
        print("ШАГ 3: Добавление столбца status_id в reestr_contract_223_fz")
        print("=" * 70)
        
        # Добавляем столбец
        cursor.execute("ALTER TABLE reestr_contract_223_fz ADD COLUMN IF NOT EXISTS status_id INTEGER;")
        conn.commit()
        print("✅ Столбец status_id добавлен в reestr_contract_223_fz")
        logger.info("Столбец status_id добавлен в reestr_contract_223_fz")
        
        # Проверяем
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'reestr_contract_223_fz' 
              AND column_name = 'status_id'
        """)
        col = cursor.fetchone()
        if col:
            print(f"   Проверка: столбец существует (тип: {col[1]})")
        else:
            raise Exception("Столбец status_id не был создан!")
        
        # Добавляем внешний ключ
        print("\nСоздание внешнего ключа...")
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
        conn.commit()
        print("✅ Внешний ключ создан")
        logger.info("Внешний ключ для reestr_contract_223_fz создан")
        
        print("\n" + "=" * 70)
        print("ШАГ 4: Присвоение статусов для reestr_contract_44_fz (БАТЧАМИ)")
        print("=" * 70)
        print(f"Размер батча: {BATCH_SIZE:,} записей")
        print("Это может занять 10-30 минут для больших таблиц...\n")
        
        total_44fz = 0
        start_time_44fz = time.time()
        
        # Новая (status_id = 1)
        total_44fz += update_status_batched(
            cursor, "reestr_contract_44_fz", 1,
            "end_date IS NOT NULL AND end_date <= CURRENT_DATE AND status_id IS NULL",
            "Новая"
        )
        
        # Работа комиссии (status_id = 2)
        total_44fz += update_status_batched(
            cursor, "reestr_contract_44_fz", 2,
            "end_date IS NOT NULL AND end_date > CURRENT_DATE AND end_date <= CURRENT_DATE + INTERVAL '90 days' AND status_id IS NULL",
            "Работа комиссии"
        )
        
        # Разыграна (status_id = 3)
        total_44fz += update_status_batched(
            cursor, "reestr_contract_44_fz", 3,
            "delivery_end_date IS NOT NULL AND delivery_end_date >= CURRENT_DATE + INTERVAL '90 days' AND status_id IS NULL",
            "Разыграна"
        )
        
        # Плохие (status_id = 4)
        total_44fz += update_status_batched(
            cursor, "reestr_contract_44_fz", 4,
            "delivery_end_date IS NULL AND status_id IS NULL",
            "Плохие"
        )
        
        elapsed_44fz = time.time() - start_time_44fz
        print(f"\n✅ Всего обновлено в reestr_contract_44_fz: {total_44fz:,} записей за {elapsed_44fz/60:.1f} минут")
        logger.info(f"Обновление reestr_contract_44_fz завершено: {total_44fz:,} записей за {elapsed_44fz/60:.1f} минут")
        
        print("\n" + "=" * 70)
        print("ШАГ 5: Присвоение статусов для reestr_contract_223_fz (БАТЧАМИ)")
        print("=" * 70)
        
        start_time_223fz = time.time()
        
        # Плохие для 223ФЗ
        total_223fz = update_status_batched(
            cursor, "reestr_contract_223_fz", 4,
            "end_date IS NOT NULL AND end_date > CURRENT_DATE + INTERVAL '180 days' AND status_id IS NULL",
            "Плохие"
        )
        
        elapsed_223fz = time.time() - start_time_223fz
        print(f"\n✅ Всего обновлено в reestr_contract_223_fz: {total_223fz:,} записей за {elapsed_223fz/60:.1f} минут")
        logger.info(f"Обновление reestr_contract_223_fz завершено: {total_223fz:,} записей за {elapsed_223fz/60:.1f} минут")
        
        print("\n" + "=" * 70)
        print("ШАГ 6: Создание индексов")
        print("=" * 70)
        
        # Создаем индексы
        indexes = [
            ("idx_reestr_contract_44_fz_status_id", 
             "CREATE INDEX IF NOT EXISTS idx_reestr_contract_44_fz_status_id ON reestr_contract_44_fz(status_id) WHERE status_id IS NOT NULL",
             "Индекс по status_id для 44ФЗ"),
            ("idx_reestr_contract_223_fz_status_id",
             "CREATE INDEX IF NOT EXISTS idx_reestr_contract_223_fz_status_id ON reestr_contract_223_fz(status_id) WHERE status_id IS NOT NULL",
             "Индекс по status_id для 223ФЗ"),
            ("idx_reestr_contract_44_fz_status_end_date",
             "CREATE INDEX IF NOT EXISTS idx_reestr_contract_44_fz_status_end_date ON reestr_contract_44_fz(status_id, end_date) WHERE status_id IN (1, 2)",
             "Композитный индекс для новых закупок 44ФЗ"),
            ("idx_reestr_contract_44_fz_status_delivery_end_date",
             "CREATE INDEX IF NOT EXISTS idx_reestr_contract_44_fz_status_delivery_end_date ON reestr_contract_44_fz(status_id, delivery_end_date) WHERE status_id = 3",
             "Композитный индекс для разыгранных закупок 44ФЗ"),
            ("idx_reestr_contract_223_fz_status_end_date",
             "CREATE INDEX IF NOT EXISTS idx_reestr_contract_223_fz_status_end_date ON reestr_contract_223_fz(status_id, end_date) WHERE status_id IS NULL OR status_id != 4",
             "Индекс для исключения плохих записей 223ФЗ"),
        ]
        
        for idx_name, idx_sql, description in indexes:
            print(f"\nСоздание индекса: {idx_name}")
            print(f"  Описание: {description}")
            start_idx = time.time()
            cursor.execute(idx_sql)
            conn.commit()
            elapsed_idx = time.time() - start_idx
            print(f"  ✅ Создан за {elapsed_idx:.2f} секунд")
            logger.info(f"Индекс {idx_name} создан за {elapsed_idx:.2f} секунд")
        
        # Статистика
        print("\n" + "=" * 70)
        print("ШАГ 7: Статистика по статусам")
        print("=" * 70)
        
        # Быстрая статистика через выборку
        print("\nСтатистика для reestr_contract_44_fz (приблизительная, через выборку):")
        cursor.execute("""
            WITH sample AS (
                SELECT status_id 
                FROM reestr_contract_44_fz 
                TABLESAMPLE SYSTEM (0.1)
                LIMIT 10000
            )
            SELECT 
                ts.name as status_name,
                COUNT(*)::bigint as count
            FROM sample s
            LEFT JOIN tender_statuses ts ON s.status_id = ts.id
            GROUP BY ts.name, ts.id
            ORDER BY ts.id
        """)
        
        stats_44fz = cursor.fetchall()
        for stat in stats_44fz:
            status_name = stat[0] or "Без статуса"
            count = stat[1]
            print(f"  {status_name}: ~{count * 1000:,} записей (приблизительно)")
        
        print("\nСтатистика для reestr_contract_223_fz (приблизительная, через выборку):")
        cursor.execute("""
            WITH sample AS (
                SELECT status_id 
                FROM reestr_contract_223_fz 
                TABLESAMPLE SYSTEM (0.1)
                LIMIT 10000
            )
            SELECT 
                CASE 
                    WHEN s.status_id IS NULL THEN 'Без статуса (используются в поиске)'
                    ELSE ts.name 
                END as status_name,
                COUNT(*)::bigint as count
            FROM sample s
            LEFT JOIN tender_statuses ts ON s.status_id = ts.id
            GROUP BY s.status_id, ts.name
            ORDER BY s.status_id NULLS FIRST
        """)
        
        stats_223fz = cursor.fetchall()
        for stat in stats_223fz:
            status_name = stat[0]
            count = stat[1]
            print(f"  {status_name}: ~{count * 1000:,} записей (приблизительно)")
        
        conn.commit()
        print("\n✅ Миграция успешно применена!")
        logger.info("Миграция успешно завершена")
        
    except Exception as e:
        conn.rollback()
        error_msg = f"Ошибка при применении миграции: {e}"
        print(f"\n❌ {error_msg}")
        logger.error(error_msg, exc_info=True)
        raise
    finally:
        cursor.close()


def main():
    """Главная функция"""
    print("=" * 70)
    print("БЕЗОПАСНАЯ МИГРАЦИЯ: Добавление статусов закупок (БАТЧАМИ)")
    print("=" * 70)
    print(f"Размер батча: {BATCH_SIZE:,} записей")
    print("Это может занять 10-30 минут для таблиц с 22 млн записей")
    print("Прогресс будет отображаться в реальном времени\n")
    
    logger.info("=" * 70)
    logger.info("Начало безопасной миграции статусов закупок")
    logger.info("=" * 70)
    
    try:
        # Подключаемся к БД
        conn = get_tender_db_connection()
        
        # Применяем миграцию батчами
        start_time = time.time()
        apply_migration_safe(conn)
        elapsed = time.time() - start_time
        
        print("\n" + "=" * 70)
        print(f"✅ МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО за {elapsed/60:.1f} минут!")
        print("=" * 70)
        print("\nСледующие шаги:")
        print("1. Запросы в сервисах уже обновлены для использования статусов")
        print("2. Записи с status_id = 4 (Плохие) автоматически исключаются из поиска")
        print("3. Для 44ФЗ используются статусы 1, 2, 3")
        print("4. Для 223ФЗ используются только записи без статуса (status_id IS NULL)")
        print("5. При следующем запуске приложения статусы будут обновляться автоматически в фоне")
        
        logger.info(f"Миграция завершена за {elapsed/60:.1f} минут")
        
    except Exception as e:
        error_msg = f"Критическая ошибка: {e}"
        print(f"\n❌ {error_msg}")
        logger.error(error_msg, exc_info=True)
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("\nСоединение с БД закрыто")
            logger.info("Соединение с БД закрыто")


if __name__ == "__main__":
    main()

