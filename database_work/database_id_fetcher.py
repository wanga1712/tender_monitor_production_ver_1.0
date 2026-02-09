from database_work.database_connection import DatabaseManager

from utils.logger_config import get_logger
from utils.cache import get_cache

# Получаем logger (только ошибки в файл)
logger = get_logger()

# Получаем кэш для часто используемых запросов
cache = get_cache()

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
    
    def __del__(self):
        """Деструктор для закрытия соединения при удалении объекта."""
        if hasattr(self, 'cursor') and self.cursor:
            try:
                self.cursor.close()
            except:
                pass
        if hasattr(self, 'db_manager') and self.db_manager:
            try:
                self.db_manager.close()
            except:
                pass

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
        Использует кэширование для часто используемых запросов.
        
        Возвращает:
        - int: id записи, если найдена
        - None: если запись не найдена (нормальная ситуация)
        - Выбрасывает исключение: если ошибка доступа к БД (критическая ситуация)
        """
        # Создаем ключ для кэша
        cache_key = f"{table_name}:{column_name}:{value}"
        
        # Пытаемся получить из кэша
        if cache.has(cache_key):
            cached_value = cache.get(cache_key)
            return cached_value
        
        query = f"SELECT id FROM {table_name} WHERE {column_name} = %s"
        params = (value,)

        # Получаем курсор и выполняем запрос
        try:
            cursor = self.get_cursor()  # Получаем курсор (создаём, если не существует)
            cursor.execute(query, params)
            result = cursor.fetchone()
            if result:
                result_id = result[0]  # Возвращаем id
                # Сохраняем в кэш
                cache.set(cache_key, result_id)
                return result_id
            else:
                # Сохраняем None в кэш (будет сохранен как NOT_FOUND маркер)
                cache.set(cache_key, None)
                return None
        except Exception as e:
            # При ошибке БД пробрасываем исключение дальше - это критическая ошибка
            error_msg = f"КРИТИЧЕСКАЯ ОШИБКА БД при получении id из {table_name} (колонка {column_name}, значение {value}): {e}"
            logger.error(error_msg, exc_info=True)
            raise  # Пробрасываем исключение, чтобы вызывающий код знал об ошибке

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

    def get_reestr_contract_223_fz_id(self, contract_number, return_table=False, table_name=None):
        """
        Получает id записи из таблицы reestr_contract_223_fz по номеру контракта.
        
        :param contract_number: Номер контракта для поиска.
        :param return_table: Если True, возвращает tuple (id, table_name), иначе только id.
        :param table_name: Если указана, ищет только в этой таблице. Иначе проверяет все статусные таблицы.
        :return: id записи или tuple (id, table_name), или None/(None, None), если не найдено.
        """
        # Если указана конкретная таблица, ищем только в ней
        if table_name:
            contract_id = self.fetch_id(table_name, "contract_number", contract_number)
            if contract_id:
                if return_table:
                    return (contract_id, table_name)
                return contract_id
            if return_table:
                return (None, None)
            return None
        
        # Иначе проверяем все таблицы (в порядке приоритета)
        tables = [
            "reestr_contract_223_fz",
            "reestr_contract_223_fz_commission_work",
            "reestr_contract_223_fz_unclear",
            "reestr_contract_223_fz_awarded",
            "reestr_contract_223_fz_completed"
        ]
        
        for table in tables:
            contract_id = self.fetch_id(table, "contract_number", contract_number)
            if contract_id:
                if return_table:
                    return (contract_id, table)
                return contract_id
        
        if return_table:
            return (None, None)
        return None

    def get_reestr_contract_44_fz_id(self, contract_number, return_table=False, table_name=None):
        """
        Получает id записи из таблицы reestr_contract_44_fz по номеру контракта.
        
        :param contract_number: Номер контракта для поиска.
        :param return_table: Если True, возвращает tuple (id, table_name), иначе только id.
        :param table_name: Если указана, ищет только в этой таблице. Иначе проверяет все статусные таблицы.
        :return: id записи или tuple (id, table_name), или None/(None, None), если не найдено.
        """
        # Если указана конкретная таблица, ищем только в ней
        if table_name:
            contract_id = self.fetch_id(table_name, "contract_number", contract_number)
            if contract_id:
                if return_table:
                    return (contract_id, table_name)
                return contract_id
            if return_table:
                return (None, None)
            return None
        
        # Иначе проверяем все таблицы (в порядке приоритета)
        tables = [
            "reestr_contract_44_fz",
            "reestr_contract_44_fz_commission_work",
            "reestr_contract_44_fz_unclear",
            "reestr_contract_44_fz_awarded",
            "reestr_contract_44_fz_completed"
        ]
        
        for table in tables:
            contract_id = self.fetch_id(table, "contract_number", contract_number)
            if contract_id:
                if return_table:
                    return (contract_id, table)
                return contract_id
        
        if return_table:
            return (None, None)
        return None

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
        Получает id записи из таблицы reestr_contract_44_fz по номеру контракта.
        Проверяет все статусные таблицы.
        
        :param contract_number_44_fz: Номер контракта для поиска.
        :return: id записи или None, если не найдено.
        """
        contract_id, _ = self.get_reestr_contract_44_fz_id(contract_number_44_fz)
        return contract_id

    def contract_number_223_fz_id(self, contract_number_223_fz):
        """
        Получает id записи из таблицы reestr_contract_223_fz по номеру контракта.
        Проверяет все статусные таблицы.
        
        :param contract_number_223_fz: Номер контракта для поиска.
        :return: id записи или None, если не найдено.
        """
        contract_id, _ = self.get_reestr_contract_223_fz_id(contract_number_223_fz)
        return contract_id


    def close(self):
        """
        Закрывает соединение с базой данных.
        """
        self.db_manager.close()