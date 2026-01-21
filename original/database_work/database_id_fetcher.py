from loguru import logger
from database_work.database_connection import DatabaseManager

class DatabaseIDFetcher:
    """
    Класс для извлечения id записей из различных таблиц базы данных по заданным значениям.
    Атрибуты:
        db_manager (DatabaseManager): Объект для работы с подключением к базе данных.
        cursor (cursor): Общий курсор для выполнения запросов.
    """

    def __init__(self):
        """
        Инициализация объекта DatabaseIDFetcher.

        Создает экземпляр DatabaseManager для выполнения запросов к базе данных.
        """

        self.db_manager = DatabaseManager()
        self.cursor = None  # Инициализируем курсор как None

    def get_cursor(self):
        """
        Метод для получения или создания курсора.
        Если курсор уже существует, возвращает его.
        Если курсор не существует, создаёт новый.
        """
        if self.cursor is None:
            self.cursor = self.db_manager.connection.cursor()
        return self.cursor

    def fetch_id(self, table_name, column_name, value):
        """
        Универсальный метод для получения id записи по заданному значению в указанной таблице.
        """
        query = f"SELECT id FROM {table_name} WHERE {column_name} = %s"
        params = (value,)

        # Получаем курсор и выполняем запрос
        try:
            cursor = self.get_cursor()  # Получаем курсор (создаём, если не существует)
            cursor.execute(query, params)
            result = cursor.fetchone()
            if result:
                return result[0]  # Возвращаем id
            else:
                logger.warning(f"Запись с {column_name}={value} не найдена в {table_name}.")
                return None
        except Exception as e:
            logger.error(f"Ошибка при получении id из {table_name}: {e}")
            return None

    def get_collection_codes_okpd_id(self, code):
        """
        Получает id записи из таблицы collection_codes_okpd по коду.
        :param code: Код для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("collection_codes_okpd", "code", code)

    def get_customer_id(self, inn):
        """
        Получает id записи из таблицы customer по имени (ИНН).
        :param inn: ИНН для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("customer", "customer_inn", inn)

    def get_contractor_id(self, inn):
        """
        Получает id записи из таблицы contractor по имени (ИНН).

        :param inn: ИНН для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("contractor", "inn", inn)

    def get_dates_id(self, date_value):
        """
        Получает id записи из таблицы dates по значению даты.

        :param date_value: Дата для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("dates", "date", date_value)

    def get_file_names_xml_id(self, file_name):
        """
        Получает id записи из таблицы file_names_xml по имени файла.

        :param file_name: Имя файла для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("file_names_xml", "file_name", file_name)

    def get_key_words_names_id(self, keyword):
        """
        Получает id записи из таблицы key_words_names по ключевому слову.

        :param keyword: Ключевое слово для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("key_words_names", "keyword", keyword)

    def get_key_words_names_documentations_id(self, keyword):
        """
        Получает id записи из таблицы key_words_names_documentations по ключевому слову.

        :param keyword: Ключевое слово для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("key_words_names_documentations", "keyword", keyword)

    def get_links_documentation_223_fz_id(self, link):
        """
        Получает id записи из таблицы links_documentation_223_fz по ссылке.

        :param link: Ссылка для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("links_documentation_223_fz", "link", link)

    def get_links_documentation_44_fz_id(self, link):
        """
        Получает id записи из таблицы links_documentation_44_fz по ссылке.

        :param link: Ссылка для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("links_documentation_44_fz", "link", link)

    def get_okpd_from_users_id(self, code):
        """
        Получает id записи из таблицы okpd_from_users по коду.

        :param code: Код для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("okpd_from_users", "code", code)

    def get_reestr_contract_223_fz_id(self, contract_number):
        """
        Получает id записи из таблицы reestr_contract_223_fz по номеру контракта.

        :param contract_number: Номер контракта для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("reestr_contract_223_fz", "contract_number", contract_number)

    def get_reestr_contract_44_fz_id(self, contract_number):
        """
        Получает id записи из таблицы reestr_contract_44_fz по номеру контракта.

        :param contract_number: Номер контракта для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("reestr_contract_44_fz", "contract_number", contract_number)

    def get_region_id(self, region_code):
        """
        Получает id записи из таблицы region по коду региона.

        :param region_code: Код региона для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("region", "code", region_code)

    def get_stop_words_names_id(self, word):
        """
        Получает id записи из таблицы stop_words_names по слову.

        :param word: Слово для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("stop_words_names", "word", word)

    def get_trading_platform_id(self, name):
        """
        Получает id записи из таблицы trading_platform по имени.

        :param name: Имя торговой платформы для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("trading_platform", "trading_platform_name", name)

    def get_users_id(self, username):
        """
        Получает id записи из таблицы users по имени пользователя.

        :param username: Имя пользователя для поиска.
        :return: id записи или None, если не найдено.
        """
        return self.fetch_id("users", "username", username)

    def get_okpd_id(self, okpd_code):
        """
                Получает id записи из таблицы users по имени пользователя.

                :param username: Имя пользователя для поиска.
                :return: id записи или None, если не найдено.
                """
        return self.fetch_id("collection_codes_okpd", "sub_code", okpd_code)

    def contract_number_44_fz_id(self, contract_number_44_fz):
        """
                Получает id записи из таблицы users по имени пользователя.

                :param username: Имя пользователя для поиска.
                :return: id записи или None, если не найдено.
                """

        return self.fetch_id("reestr_contract_44_fz", "contract_number", contract_number_44_fz)

    def contract_number_223_fz_id(self, contract_number_223_fz):
        """
                Получает id записи из таблицы users по имени пользователя.

                :param username: Имя пользователя для поиска.
                :return: id записи или None, если не найдено.
                """
        return self.fetch_id("reestr_contract_223_fz", "contract_number", contract_number_223_fz)


    def close(self):
        """
        Закрывает соединение с базой данных.
        """
        self.db_manager.close()
