#!/usr/bin/env python3
"""
MODULE: scripts.check_idle_transactions
RESPONSIBILITY: Monitoring and analyzing idle transactions in the database.
ALLOWED: psycopg2, logging, config.settings, sys.
FORBIDDEN: None.
ERRORS: None.

Скрипт для проверки и исправления idle in transaction состояний в БД
"""

import psycopg2
import logging
from config.settings import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_idle_transactions():
    """Проверяет активные соединения в состоянии idle in transaction"""
    try:
        conn = psycopg2.connect(
            host=config.database.host,
            port=config.database.port,
            database=config.database.database,
            user=config.database.user,
            password=config.database.password
        )

        with conn.cursor() as cursor:
            # Проверяем активные соединения
            cursor.execute("""
                SELECT
                    pid,
                    usename,
                    client_addr,
                    client_port,
                    state,
                    state_change,
                    query_start,
                    EXTRACT(epoch FROM (now() - state_change)) as idle_time_seconds,
                    query
                FROM pg_stat_activity
                WHERE state = 'idle in transaction'
                AND datname = %s
                ORDER BY state_change ASC
            """, (config.database.database,))

            idle_transactions = cursor.fetchall()

            if idle_transactions:
                logger.warning(f"Найдено {len(idle_transactions)} соединений в состоянии 'idle in transaction':")
                for row in idle_transactions:
                    pid, usename, client_addr, client_port, state, state_change, query_start, idle_time, query = row
                    logger.warning(f"PID: {pid}, User: {usename}, Idle: {idle_time:.1f}s, Query: {query[:100]}...")

                    # Если соединение висит больше 300 секунд (5 минут), завершаем его
                    if idle_time > 300:
                        logger.info(f"Завершаем процесс PID {pid} (висит {idle_time:.1f}s)")
                        cursor.execute("SELECT pg_terminate_backend(%s)", (pid,))
                        logger.info(f"Процесс PID {pid} завершен")
            else:
                logger.info("Нет соединений в состоянии 'idle in transaction'")

        conn.close()

    except Exception as e:
        logger.error(f"Ошибка при проверке транзакций: {e}")

def kill_all_idle_transactions():
    """Завершает все idle in transaction соединения"""
    try:
        conn = psycopg2.connect(
            host=config.database.host,
            port=config.database.port,
            database=config.database.database,
            user=config.database.user,
            password=config.database.password
        )

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE state = 'idle in transaction'
                AND datname = %s
            """, (config.database.database,))

            terminated_count = cursor.rowcount
            logger.info(f"Завершено {terminated_count} соединений в состоянии 'idle in transaction'")

        conn.close()

    except Exception as e:
        logger.error(f"Ошибка при завершении транзакций: {e}")

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--kill":
        logger.info("Завершаем все idle in transaction соединения...")
        kill_all_idle_transactions()
    else:
        logger.info("Проверяем idle in transaction соединения...")
        check_idle_transactions()
