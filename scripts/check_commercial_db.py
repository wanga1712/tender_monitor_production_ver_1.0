"""
MODULE: scripts.check_commercial_db
RESPONSIBILITY: Checking commercial database schema and content.
ALLOWED: sys, os, config.settings, core.database, loguru.
FORBIDDEN: None.
ERRORS: None.

Скрипт для проверки схемы БД commercial_db (таблицы products)
"""
import sys
import os
sys.path.insert(0, os.getcwd())

from config.settings import config
from core.database import DatabaseManager
from loguru import logger

def main():
    logger.info("=" * 80)
    logger.info("ПРОВЕРКА БД commercial_db (каталог товаров)")
    logger.info("=" * 80)
    
    # Подключаемся к коммерческой БД
    db_manager = DatabaseManager(config.database)
    db_manager.connect()
    
    logger.info(f"БД: {config.database.database}")
    logger.info(f"Host: {config.database.host}")
    logger.info("-" * 80)
    
    # Проверяем таблицу products
    check_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'products'
        )
    """
    
    result = db_manager.execute_query(check_query)
    table_exists = result[0].get("exists", False) if result else False
    
    if table_exists:
        logger.success("✅ Таблица 'products' найдена!")
        
        # Получаем структуру
        columns_query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'products' AND table_schema = 'public'
            ORDER BY ordinal_position
        """
        columns = db_manager.execute_query(columns_query)
        logger.info(f"\nСтруктура таблицы 'products' ({len(columns)} колонок):")
        for col in columns:
            logger.info(f"  - {col.get('column_name')}: {col.get('data_type')}")
            
        # Считаем количество записей
        count_query = "SELECT COUNT(*) as cnt FROM products WHERE name IS NOT NULL"
        count_result = db_manager.execute_query(count_query)
        count = count_result[0].get("cnt", 0) if count_result else 0
        logger.info(f"\nКоличество записей с name: {count}")
        
        # Показываем примеры названий
        if count > 0:
            sample_query = "SELECT name FROM products WHERE name IS NOT NULL LIMIT 5"
            samples = db_manager.execute_query(sample_query)
            logger.info("\nПримеры названий товаров:")
            for row in samples:
                logger.info(f"  - {row.get('name')}")
    else:
        logger.error("❌ Таблица 'products' НЕ найдена в БД commercial_db!")

if __name__ == "__main__":
    main()
