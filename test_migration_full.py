#!/usr/bin/env python3
"""Полное тестирование миграции - создание тестового контракта со всеми связями"""
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

def create_test_entities(db):
    """Создает тестовые сущности (customer, contractor, okpd, platform)"""
    test_id_suffix = int(datetime.now().timestamp())
    
    # 1. Создаем тестового customer
    db.cursor.execute("""
        INSERT INTO customer (
            customer_short_name, customer_full_name, customer_inn, customer_kpp,
            customer_legal_address, customer_actual_address
        ) VALUES (
            %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (customer_inn) DO UPDATE SET customer_short_name = EXCLUDED.customer_short_name
        RETURNING id;
    """, (
        f"ТЕСТ-Заказчик-{test_id_suffix}",
        f"ТЕСТОВЫЙ Заказчик для миграции {test_id_suffix}",
        f"123456789{test_id_suffix % 1000:03d}",
        f"123456789",
        "г. Москва, ул. Тестовая, д. 1",
        "г. Москва, ул. Тестовая, д. 1"
    ))
    customer_id = db.cursor.fetchone()[0]
    logger.info(f"✅ Создан тестовый customer: ID={customer_id}")
    
    # 2. Создаем тестового contractor
    db.cursor.execute("""
        INSERT INTO contractor (
            short_name, full_name, inn, kpp,
            legal_address
        ) VALUES (
            %s, %s, %s, %s, %s
        ) ON CONFLICT (inn) DO UPDATE SET short_name = EXCLUDED.short_name
        RETURNING id;
    """, (
        f"ТЕСТ-Подрядчик-{test_id_suffix}",
        f"ТЕСТОВЫЙ Подрядчик для миграции {test_id_suffix}",
        f"987654321{test_id_suffix % 1000:03d}",
        f"987654321",
        "г. Москва, ул. Подрядная, д. 2"
    ))
    contractor_id = db.cursor.fetchone()[0]
    logger.info(f"✅ Создан тестовый contractor: ID={contractor_id}")
    
    # 3. Получаем первый доступный okpd
    db.cursor.execute("SELECT id FROM collection_codes_okpd LIMIT 1;")
    okpd_row = db.cursor.fetchone()
    if okpd_row:
        okpd_id = okpd_row[0]
        logger.info(f"✅ Используется существующий okpd: ID={okpd_id}")
    else:
        logger.error("Нет доступных okpd в БД")
        raise Exception("Нет доступных okpd в БД")
    
    # 4. Получаем или создаем тестовую platform
    db.cursor.execute("""
        SELECT id FROM trading_platform 
        WHERE trading_platform_name = 'ТЕСТ-Площадка-Миграция'
        LIMIT 1;
    """)
    platform_row = db.cursor.fetchone()
    if platform_row:
        platform_id = platform_row[0]
        logger.info(f"✅ Используется существующая platform: ID={platform_id}")
    else:
        db.cursor.execute("""
            INSERT INTO trading_platform (trading_platform_name, trading_platform_url)
            VALUES (%s, %s)
            RETURNING id;
        """, (
            "ТЕСТ-Площадка-Миграция",
            "https://test-platform.example.com"
        ))
        platform_id = db.cursor.fetchone()[0]
        logger.info(f"✅ Создана тестовая platform: ID={platform_id}")
    
    return customer_id, contractor_id, okpd_id, platform_id

def create_test_contract_full():
    """Создает полный тестовый контракт со всеми связями"""
    db = DatabaseManager()
    try:
        logger.info("=" * 60)
        logger.info("СОЗДАНИЕ ПОЛНОГО ТЕСТОВОГО КОНТРАКТА")
        logger.info("=" * 60)
        
        # Создаем тестовые сущности
        customer_id, contractor_id, okpd_id, platform_id = create_test_entities(db)
        db.connection.commit()
        
        # Получаем максимальный ID для создания уникального контракта
        db.cursor.execute("SELECT COALESCE(MAX(id), 0) FROM reestr_contract_44_fz;")
        max_id = db.cursor.fetchone()[0]
        test_id = max_id + 1
        
        # Создаем тестовый контракт с завершенной датой (вчера)
        yesterday = datetime.now() - timedelta(days=1)
        test_contract_number = f"TEST-MIGRATION-{test_id}"
        
        insert_query = """
            INSERT INTO reestr_contract_44_fz (
                id, contract_number, tender_link, auction_name, start_date,
                delivery_end_date, customer_id, contractor_id, okpd_id, trading_platform_id,
                initial_price, final_price
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING id;
        """
        
        db.cursor.execute(insert_query, (
            test_id,
            test_contract_number,
            f"https://test.example.com/tender/{test_id}",
            "ТЕСТОВЫЙ контракт для проверки миграции",
            yesterday.date(),
            yesterday.date(),  # delivery_end_date = вчера (завершен)
            customer_id,
            contractor_id,
            okpd_id,
            platform_id,
            1000000.00,  # initial_price
            950000.00    # final_price
        ))
        
        inserted_id = db.cursor.fetchone()[0]
        db.connection.commit()
        logger.info(f"✅ Тестовый контракт создан: ID={inserted_id}, номер={test_contract_number}")
        
        # Создаем тестовые ссылки документации (links_documentation_44_fz)
        test_links = [
            {
                "contract_id": inserted_id,
                "file_name": "Извещение о проведении электронного аукциона",
                "document_links": f"https://test.example.com/docs/{inserted_id}/notice.pdf"
            },
            {
                "contract_id": inserted_id,
                "file_name": "Проект контракта",
                "document_links": f"https://test.example.com/docs/{inserted_id}/contract.pdf"
            },
            {
                "contract_id": inserted_id,
                "file_name": "Дополнительные материалы",
                "document_links": f"https://test.example.com/docs/{inserted_id}/additional.pdf"
            }
        ]
        
        links_inserted = 0
        for link_data in test_links:
            db.cursor.execute("""
                INSERT INTO links_documentation_44_fz (
                    contract_id, file_name, document_links
                ) VALUES (
                    %s, %s, %s
                ) ON CONFLICT DO NOTHING
                RETURNING id;
            """, (
                link_data["contract_id"],
                link_data["file_name"],
                link_data["document_links"]
            ))
            if db.cursor.fetchone():
                links_inserted += 1
        
        db.connection.commit()
        logger.info(f"✅ Создано тестовых ссылок документации: {links_inserted}")
        
        logger.info("=" * 60)
        logger.info(f"✅ ПОЛНЫЙ ТЕСТОВЫЙ КОНТРАКТ СОЗДАН:")
        logger.info(f"   ID контракта: {inserted_id}")
        logger.info(f"   Номер: {test_contract_number}")
        logger.info(f"   delivery_end_date: {yesterday.date()} (завершен)")
        logger.info(f"   Customer ID: {customer_id}")
        logger.info(f"   Contractor ID: {contractor_id}")
        logger.info(f"   OKPD ID: {okpd_id}")
        logger.info(f"   Platform ID: {platform_id}")
        logger.info(f"   Ссылок документации: {links_inserted}")
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
        
        # Проверяем ссылки документации
        db.cursor.execute("SELECT COUNT(*) FROM links_documentation_44_fz WHERE contract_id = %s;", (test_id,))
        links_count = db.cursor.fetchone()[0]
        
        logger.info(f"Контракт ID {test_id}:")
        logger.info(f"   В основной таблице: {'✅ ЕСТЬ' if in_main > 0 else '❌ НЕТ'}")
        logger.info(f"   В completed: {'✅ ЕСТЬ' if in_completed > 0 else '❌ НЕТ'}")
        logger.info(f"   Ссылок документации: {links_count}")
        
        if in_main == 0 and in_completed > 0:
            logger.info("✅ МИГРАЦИЯ УСПЕШНА: контракт перенесен и удален из основной таблицы")
            logger.info("✅ Ссылки документации остались валидными (ссылаются на ID, который есть в completed)")
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
    # 1. Создаем полный тестовый контракт
    test_id = create_test_contract_full()
    
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
