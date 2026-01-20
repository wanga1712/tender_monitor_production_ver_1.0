import os
import psycopg2
from dotenv import load_dotenv
from contextlib import contextmanager
from typing import Optional

from utils.logger_config import get_logger
from utils.exceptions import DatabaseError

# Получаем logger (только ошибки в файл)
logger = get_logger()

class DatabaseManager:
    """
    Класс для управления подключением и взаимодействием с базой данных.

    Атрибуты:
        connection (psycopg2.extensions.connection): Объект соединения с базой данных.
        cursor (psycopg2.extensions.cursor): Курсор для выполнения SQL-запросов к базе данных.
        db_host (str): Хост базы данных.
        db_name (str): Название базы данных.
        db_user (str): Имя пользователя базы данных.
        db_password (str): Пароль пользователя базы данных.
        db_port (str): Порт подключения к базе данных.
        zip_directory (str): Директория для хранения ZIP-архивов.
    """

    def __init__(self):
        """
        Инициализация объекта DatabaseManager.

        Загружает настройки подключения из файла .env, устанавливает соединение с базой данных и инициализирует курсор.

        Исключения:
            Exception: В случае ошибки подключения к базе данных.
        """

        # Загружаем переменные окружения из файла .env
        # Определяем путь к файлу с учетными данными относительно текущего модуля
        env_file_path = os.path.join(os.path.dirname(__file__), 'db_credintials.env')
        load_dotenv(dotenv_path=env_file_path)

        # Получаем данные для подключения к базе данных
        self.db_host = os.getenv("DB_HOST")
        self.db_name = os.getenv("DB_DATABASE")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.db_port = os.getenv("DB_PORT")

        try:
            # Устанавливаем соединение с базой данных
            self.connection = psycopg2.connect(
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port,
                connect_timeout=10
            )
            # Устанавливаем autocommit в False для управления транзакциями
            self.connection.autocommit = False

            # Инициализируем курсор для выполнения операций с базой данных
            self.cursor = self.connection.cursor()
        except psycopg2.Error as e:
            # Логируем и выбрасываем исключение в случае ошибки подключения
            error_msg = f'Ошибка подключения к базе данных: {e}'
            logger.error(error_msg, exc_info=True)
            raise DatabaseError(error_msg, original_error=e) from e
        except Exception as e:
            error_msg = f'Неожиданная ошибка при подключении к базе данных: {e}'
            logger.error(error_msg, exc_info=True)
            raise DatabaseError(error_msg, original_error=e) from e

    def execute_query(self, query, params=None, fetch=False):
        """
        Выполняет SQL-запрос.

        :param query: SQL-запрос для выполнения.
        :param params: Параметры для запроса.
        :param fetch: Если True, возвращает результат выполнения запроса.

        :return: Результат запроса, если `fetch=True`, иначе None.
        :raises DatabaseError: При ошибке выполнения запроса.
        """
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            if fetch:  # Только если нужен результат
                return self.cursor.fetchall()
        except psycopg2.Error as e:
            self.connection.rollback()
            error_msg = f'Ошибка при выполнении запроса: {e}'
            logger.error(error_msg, exc_info=True)
            raise DatabaseError(error_msg, original_error=e) from e

    def fetch_one(self, query, params=None):
        """
        Выполняет SQL-запрос и возвращает одну строку результата.

        :param query: SQL-запрос для выполнения.
        :param params: Параметры для запроса.
        :return: Одна строка результата запроса или None.
        :raises DatabaseError: При ошибке выполнения запроса.
        """
        try:
            self.cursor.execute(query, params)
            result = self.cursor.fetchone()
            return result[0] if result else None
        except psycopg2.Error as e:
            error_msg = f'Ошибка при выполнении запроса: {e}'
            logger.error(error_msg, exc_info=True)
            raise DatabaseError(error_msg, original_error=e) from e
    
    @contextmanager
    def get_cursor(self):
        """
        Контекстный менеджер для получения курсора.
        Автоматически коммитит изменения или откатывает при ошибке.
        
        :yields: Курсор базы данных
        """
        cursor = None
        try:
            cursor = self.connection.cursor()
            yield cursor
            self.connection.commit()
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    def close(self):
        """
        Закрывает соединение и курсор с базой данных.

        :return: None
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except Exception as e:
            logger.error(f"Ошибка при закрытии соединения или курсора: {e}", exc_info=True)