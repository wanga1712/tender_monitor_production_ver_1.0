#!/usr/bin/env python3
"""Тестирование миграции - создание тестового контракта и проверка миграции"""
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent))

from database_work.database_connection import DatabaseManager
from database_work.contracts_migration import migrate_completed_contracts
from dotenv import load_dotenv
from loguru import logger

# Загружаем переменные окружения
env_path = Path(__file__).parent / "database_work" / "db_credintials.env"
load_dotenv(env_path)

def create_test_contract():
    """Создает тестовый контракт для проверки миграции"""
    db = DatabaseManager()
    try:
        logger.info("=" * 60)
        logger.info("СОЗДАНИЕ ТЕСТОВОГО КОНТРАКТА")
        logger.info("=" * 60)
        
        # Получаем минимальные необходимые данные для создания контракта
        # Нужны: customer_id, contractor_id, okpd_id, platform_id
        
        # Получаем первый доступный customer
        db.cursor.execute("SELECT id FROM customer LIMIT 1;")
        customer_row = db.cursor.fetchone()
        if not customer_row:
            logger.error("Нет доступных customer в БД")
            return None
        customer_id = customer_row[0]
        
        # Получаем первый доступный contractor
        db.cursor.execute("SELECT id FROM contractor LIMIT 1;")
        contractor_row = db.cursor.fetchone()
        if not contractor_row:
            logger.error("Нет доступных contractor в БД")
            return None
        contractor_id = contractor_row[0]
        
        # Получаем первый доступный okpd
        db.cursor.execute("SELECT id FROM collection_codes_okpd LIMIT 1;")
        okpd_row = db.cursor.fetchone()
        if not okpd_row:
            logger.error("Нет доступных okpd в БД")
            return None
        okpd_id = okpd_row[0]
        
        # Получаем первый доступный platform
        db.cursor.execute("SELECT id FROM trading_platform LIMIT 1;")
        platform_row = db.cursor.fetchone()
        if not platform_row:
            logger.error("Нет доступных platform в БД")
            return None
        platform_id = platform_row[0]
        
        # Получаем максимальный ID для создания уникального
        db.cursor.execute("SELECT COALESCE(MAX(id), 0) FROM reestr_contract_44_fz;")
        max_id = db.cursor.fetchone()[0]
        test_id = max_id + 1
        
        # Создаем тестовый контракт с завершенной датой (вчера)
        yesterday = datetime.now() - timedelta(days=1)
        test_contract_number = f"TEST-MIGRATION-{test_id}"
        
        insert_query = """
            INSERT INTO reestr_contract_44_fz (
                id, contract_number, tender_link, auction_name, start_date,
                delivery_end_date, customer_id, contractor_id, okpd_id, platform_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING id;
        """
        
        db.cursor.execute(insert_query, (
            test_id,
            test_contract_number,
            f"https://test.example.com/{test_id}",
            "Тестовый контракт для миграции",
            yesterday.date(),
            yesterday.date(),  # delivery_end_date = вчера (завершен)
            customer_id,
            contractor_id,
            okpd_id,
            platform_id
        ))
        
        inserted_id = db.cursor.fetchone()[0]
        db.connection.commit()
        
        logger.info(f"✅ Тестовый контракт создан:")
        logger.info(f"   ID: {inserted_id}")
        logger.info(f"   Номер: {test_contract_number}")
        logger.info(f"   delivery_end_date: {yesterday.date()}")
        logger.info("=" * 60)
        
        return inserted_id
        
    except Exception as e:
        logger.error(f"Ошибка создания тестового контракта: {e}")
        import traceback
        traceback.print_exc()
        db.connection.rollback()
        return None
    finally:
        db.close()

def check_migration_result(test_id):
    """Проверяет результат миграции тестового контракта"""
    db = DatabaseManager()
    try:
        logger.info("=" * 60)
        logger.info("ПРОВЕРКА РЕЗУЛЬТАТА МИГРАЦИИ")
        logger.info("=" * 60)
        
        # Проверяем наличие в основной таблице
        db.cursor.execute("SELECT COUNT(*) FROM reestr_contract_44_fz WHERE id = %s;", (test_id,))
        in_main = db.cursor.fetchone()[0]
        
        # Проверяем наличие в completed
        db.cursor.execute("SELECT COUNT(*) FROM reestr_contract_44_fz_completed WHERE id = %s;", (test_id,))
        in_completed = db.cursor.fetchone()[0]
        
        logger.info(f"Контракт ID {test_id}:")
        logger.info(f"   В основной таблице: {'✅ ЕСТЬ' if in_main > 0 else '❌ НЕТ'}")
        logger.info(f"   В completed: {'✅ ЕСТЬ' if in_completed > 0 else '❌ НЕТ'}")
        
        if in_main == 0 and in_completed > 0:
            logger.info("✅ МИГРАЦИЯ УСПЕШНА: контракт перенесен и удален из основной таблицы")
        elif in_main > 0 and in_completed > 0:
            logger.warning("⚠️  ДУБЛИКАТ: контракт есть в обеих таблицах")
        elif in_main > 0 and in_completed == 0:
            logger.error("❌ ОШИБКА: контракт не был мигрирован")
        else:
            logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: контракт отсутствует в обеих таблицах")
        
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Ошибка проверки: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    # 1. Создаем тестовый контракт
    test_id = create_test_contract()
    
    if test_id:
        # 2. Запускаем миграцию
        logger.info("\n" + "=" * 60)
        logger.info("ЗАПУСК МИГРАЦИИ")
        logger.info("=" * 60)
        result = migrate_completed_contracts()
        logger.info(f"Результат миграции: {result}")
        
        # 3. Проверяем результат
        check_migration_result(test_id)
    else:
        logger.error("Не удалось создать тестовый контракт")
