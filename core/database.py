"""
MODULE: core.database
RESPONSIBILITY: Low-level PostgreSQL connection management (Singleton).
ALLOWED: psycopg2, logging, connection pooling logic.
FORBIDDEN: Business logic, specific tender operations (use repositories).
ERRORS: DatabaseConnectionError, DatabaseQueryError.

Менеджер базы данных с connection pooling и обработкой ошибок

Модуль предоставляет:
- DatabaseManager: Singleton для управления подключением к PostgreSQL
- DatabaseRepository: Базовый репозиторий для работы с таблицами
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
import logging
from loguru import logger

from config.settings import DatabaseConfig
from core.exceptions import DatabaseConnectionError, DatabaseQueryError


class DatabaseManager:
    """
    Менеджер для работы с PostgreSQL базой данных
    
    Реализует паттерн Singleton для единого подключения к базе данных
    во всем приложении. Предоставляет методы для выполнения SQL запросов
    с автоматической обработкой ошибок и логированием.
    
    Attributes:
        db_config: Конфигурация подключения к БД
        connection: Активное подключение к PostgreSQL
    """

    _instance = None

    def __new__(cls, db_config: DatabaseConfig):
        """
        Создание единственного экземпляра (Singleton)
        
        Args:
            db_config: Конфигурация подключения к БД
        
        Returns:
            Единственный экземпляр DatabaseManager
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, db_config: DatabaseConfig):
        """
        Инициализация менеджера базы данных
        
        Args:
            db_config: Конфигурация подключения к БД
        """
        if not hasattr(self, '_initialized'):
            self.db_config = db_config
            self.connection: Optional[psycopg2.extensions.connection] = None
            self._initialized = True

    def connect(self) -> None:
        """
        Установка соединения с базой данных
        
        Создает подключение к PostgreSQL используя параметры из конфигурации.
        Использует RealDictCursor для возврата результатов в виде словарей.
        
        Raises:
            DatabaseConnectionError: При ошибке подключения
        """
        try:
            self.connection = psycopg2.connect(
                host=self.db_config.host,
                database=self.db_config.database,
                user=self.db_config.user,
                password=self.db_config.password,
                port=self.db_config.port,
                cursor_factory=RealDictCursor
            )
            logger.info(f"Успешное подключение к БД: {self.db_config.database}")

        except psycopg2.OperationalError as e:
            error_msg = f"Ошибка подключения к БД {self.db_config.database}: {e}"
            logger.error(error_msg)
            raise DatabaseConnectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Неожиданная ошибка при подключении к БД: {e}"
            logger.error(error_msg)
            raise DatabaseConnectionError(error_msg) from e

    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Выполнение SQL запроса

        Args:
            query: SQL запрос
            params: Параметры для запроса

        Returns:
            Список словарей с результатами

        Raises:
            DatabaseQueryError: При ошибке выполнения запроса
        """
        if self.connection is None:
            raise DatabaseConnectionError("Нет активного подключения к БД")

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())

                if query.strip().upper().startswith('SELECT'):
                    result = cursor.fetchall()
                    logger.debug(f"Выполнен SELECT запрос, возвращено {len(result)} строк")
                    return result
                else:
                    self.connection.commit()
                    logger.debug("Выполнен DML запрос")
                    return []

        except psycopg2.Error as e:
            self.connection.rollback()
            error_msg = f"Ошибка выполнения запроса: {e}\nЗапрос: {query}"
            logger.error(error_msg)
            raise DatabaseQueryError(error_msg) from e
        except Exception as e:
            self.connection.rollback()
            error_msg = f"Неожиданная ошибка при выполнении запроса: {e}"
            logger.error(error_msg)
            raise DatabaseQueryError(error_msg) from e

    def execute_single_value(self, query: str, params: tuple = None) -> Any:
        """
        Выполнение запроса, возвращающего одно значение

        Returns:
            Единичное значение из первой строки первого столбца
        """
        results = self.execute_query(query, params)
        if results and len(results) > 0:
            first_row = results[0]
            if first_row and len(first_row) > 0:
                return list(first_row.values())[0]
        return None

    def check_connection(self) -> bool:
        """
        Проверка активности соединения

        Returns:
            True если соединение активно
        """
        try:
            if self.connection and not self.connection.closed:
                self.execute_query("SELECT 1")
                return True
            return False
        except Exception:
            return False

    def close(self) -> None:
        """Закрытие соединения с БД с защитой от Access Violation"""
        try:
            if self.connection and not self.connection.closed:
                try:
                    self.connection.close()
                    logger.info("Соединение с БД закрыто")
                except Exception as close_error:
                    logger.warning(f"Ошибка при закрытии соединения с БД: {close_error}")
        except Exception as e:
            logger.warning(f"Ошибка при проверке состояния соединения: {e}")
        finally:
            self.connection = None

    def __enter__(self):
        """Поддержка контекстного менеджера"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Автоматическое закрытие соединения"""
        self.close()


class DatabaseRepository:
    """
    Базовый репозиторий для работы с конкретными таблицами
    """

    def __init__(self, db_manager: DatabaseManager, table_name: str):
        self.db = db_manager
        self.table_name = table_name

    def get_all(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Получение всех записей с лимитом"""
        query = f"SELECT * FROM {self.table_name} ORDER BY id LIMIT %s"
        return self.db.execute_query(query, (limit,))

    def get_by_id(self, record_id: int) -> Optional[Dict[str, Any]]:
        """Получение записи по ID"""
        query = f"SELECT * FROM {self.table_name} WHERE id = %s"
        results = self.db.execute_query(query, (record_id,))
        return results[0] if results else None

    def count(self) -> int:
        """Получение количества записей"""
        query = f"SELECT COUNT(*) as count FROM {self.table_name}"
        result = self.db.execute_single_value(query)
        return result or 0