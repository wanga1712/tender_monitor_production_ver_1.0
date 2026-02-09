"""
MODULE: scripts.apply_okpd_optimization
RESPONSIBILITY: Applying OKPD optimization indexes.
ALLOWED: sys, pathlib, config.settings, core.tender_database, loguru.
FORBIDDEN: None.
ERRORS: None.

Скрипт для применения оптимизации индексов для ОКПД и тендеров.
"""

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import config
from core.tender_database import TenderDatabaseManager
from loguru import logger


def apply_optimization():
    """Применение SQL скрипта оптимизации"""
    try:
        # Читаем SQL скрипт
        sql_file = project_root / "scripts" / "optimize_okpd_queries.sql"
        if not sql_file.exists():
            logger.error(f"Файл {sql_file} не найден")
            return False
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Подключаемся к БД tender_monitor
        if not config.tender_database:
            logger.error("Конфигурация БД tender_monitor не задана")
            return False
        
        db_manager = TenderDatabaseManager()
        db_manager.connect()
        
        logger.info("Применение оптимизации индексов для ОКПД и тендеров...")
        
        # Выполняем SQL команды по одной (psycopg2 не поддерживает множественные команды в одном execute)
        commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip() and not cmd.strip().startswith('--')]
        
        for i, command in enumerate(commands, 1):
            if not command:
                continue
            try:
                logger.debug(f"Выполнение команды {i}/{len(commands)}: {command[:100]}...")
                db_manager.execute_query(command, None, None)
                logger.debug(f"Команда {i} выполнена успешно")
            except Exception as e:
                logger.warning(f"Ошибка при выполнении команды {i}: {e}")
                # Продолжаем выполнение остальных команд
        
        logger.info("Оптимизация индексов завершена")
        db_manager.close()
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при применении оптимизации: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    logger.info("Запуск оптимизации индексов для ОКПД и тендеров")
    success = apply_optimization()
    if success:
        logger.info("Оптимизация успешно применена")
        sys.exit(0)
    else:
        logger.error("Ошибка при применении оптимизации")
        sys.exit(1)

