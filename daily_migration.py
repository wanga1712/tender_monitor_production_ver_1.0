#!/usr/bin/env python3
"""Ежедневная миграция завершенных контрактов - запускается в 22:00"""
import os
import sys
from pathlib import Path
from datetime import datetime

# Добавляем путь к модулям
sys.path.insert(0, str(Path(__file__).parent))

from database_work.contracts_migration import migrate_completed_contracts
from dotenv import load_dotenv
from loguru import logger

# Загружаем переменные окружения
env_path = Path(__file__).parent / "database_work" / "db_credintials.env"
load_dotenv(env_path)

def main():
    """Основная функция для ежедневного запуска миграции"""
    logger.info("=" * 60)
    logger.info(f"НАЧАЛО ЕЖЕДНЕВНОЙ МИГРАЦИИ - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    try:
        result = migrate_completed_contracts()
        
        logger.info("=" * 60)
        logger.info("РЕЗУЛЬТАТЫ МИГРАЦИИ:")
        logger.info(f"  44-ФЗ: перенесено {result.get('44_fz_migrated', 0)}, удалено {result.get('44_fz_deleted', 0)}")
        logger.info(f"  223-ФЗ: перенесено {result.get('223_fz_migrated', 0)}, удалено {result.get('223_fz_deleted', 0)}")
        logger.info(f"  44-ФЗ неизвестные: перенесено {result.get('44_fz_unknown_migrated', 0)}, удалено {result.get('44_fz_unknown_deleted', 0)}")
        logger.info(f"  44-ФЗ плохие: перенесено {result.get('44_fz_bad_migrated', 0)}, удалено {result.get('44_fz_bad_deleted', 0)}")
        logger.info(f"  Успех: {result.get('success', False)}")
        if result.get('error'):
            logger.error(f"  Ошибка: {result.get('error')}")
        logger.info("=" * 60)
        logger.info(f"МИГРАЦИЯ ЗАВЕРШЕНА - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        return 0 if result.get('success') else 1
        
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА при миграции: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
