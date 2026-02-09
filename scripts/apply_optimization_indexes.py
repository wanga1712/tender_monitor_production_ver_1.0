"""
MODULE: scripts.apply_optimization_indexes
RESPONSIBILITY: Applying general optimization indexes to database.
ALLOWED: os, sys, pathlib, dotenv, psycopg2, psycopg2.extras, loguru.
FORBIDDEN: None.
ERRORS: None.

Скрипт для применения индексов оптимизации к базе данных tender_monitor.
Запуск: python -m scripts.apply_optimization_indexes
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from loguru import logger

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Загружаем переменные окружения
load_dotenv()

def apply_optimization_indexes():
    """Применение индексов оптимизации к базе данных"""
    try:
        # Получаем параметры подключения
        host = os.getenv("TENDER_MONITOR_DB_HOST")
        database = os.getenv("TENDER_MONITOR_DB_DATABASE")
        user = os.getenv("TENDER_MONITOR_DB_USER")
        password = os.getenv("TENDER_MONITOR_DB_PASSWORD")
        port = os.getenv("TENDER_MONITOR_DB_PORT", "5432")
        
        if not all([host, database, user, password]):
            logger.error("Не все параметры подключения к БД заданы в .env файле")
            return False
        
        # Подключаемся к БД
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        conn.autocommit = True
        
        logger.info("Подключение к базе данных установлено")
        
        # Читаем SQL файл с индексами
        sql_file = project_root / "scripts" / "optimize_tender_queries.sql"
        if not sql_file.exists():
            logger.error(f"Файл {sql_file} не найден")
            return False
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Разбиваем на отдельные команды
        commands = [cmd.strip() for cmd in sql_content.split(';') if cmd.strip() and not cmd.strip().startswith('--')]
        
        with conn.cursor() as cur:
            for i, command in enumerate(commands, 1):
                try:
                    logger.info(f"Выполнение команды {i}/{len(commands)}...")
                    cur.execute(command)
                    logger.success(f"Команда {i} выполнена успешно")
                except Exception as e:
                    logger.warning(f"Ошибка при выполнении команды {i}: {e}")
                    # Продолжаем выполнение остальных команд
        
        conn.close()
        logger.success("Индексы оптимизации применены успешно")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при применении индексов: {e}")
        return False

if __name__ == "__main__":
    apply_optimization_indexes()

