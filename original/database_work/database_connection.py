from loguru import logger
import os
import psycopg2
from dotenv import load_dotenv

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
        load_dotenv(dotenv_path=r'C:\Users\wangr\PycharmProjects\TenderMonitor\database_work\db_credintials.env')

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
                port=self.db_port
            )

            # Инициализируем курсор для выполнения операций с базой данных
            self.cursor = self.connection.cursor()
            logger.debug('Подключился к базе данных.')
        except Exception as e:
            # Логируем и выбрасываем исключение в случае ошибки подключения
            logger.exception(f'Ошибка подключения к базе данных: {e}')

    def execute_query(self, query, params=None, fetch=False):
        """
        Выполняет SQL-запрос.

        :param query: SQL-запрос для выполнения.
        :param params: Параметры для запроса.
        :param fetch: Если True, возвращает результат выполнения запроса.

        :return: Результат запроса, если `fetch=True`, иначе None.
        """
        self.cursor.execute(query, params)
        self.connection.commit()
        if fetch:  # Только если нужен результат
            return self.cursor.fetchall()

    def fetch_one(self, query, params=None):
        """
        Выполняет SQL-запрос и возвращает одну строку результата.

        :param query: SQL-запрос для выполнения.
        :param params: Параметры для запроса.
        :return: Одна строка результата запроса или None.
        """
        self.cursor.execute(query, params)
        result = self.cursor.fetchone()
        return result[0] if result else False  # Вернёт False, если данных нет

    def close(self):
        """
        Закрывает соединение и курсор с базой данных.

        :return: None
        """
        try:
            if self.cursor:
                self.cursor.close()
                logger.debug("Курсор закрыт.")
            if self.connection:
                self.connection.close()
                logger.debug("Соединение с базой данных закрыто.")
        except Exception as e:
            logger.exception(f"Ошибка при закрытии соединения или курсора: {e}")