from database_work.database_connection import DatabaseManager
from psycopg2 import IntegrityError
from secondary_functions import load_config

from utils.logger_config import get_logger
from utils import stats as stats_collector

# Получаем logger (только ошибки в файл)
logger = get_logger()


class DatabaseOperations:
    def __init__(self, config_path="config.ini"):

        self.db_manager = DatabaseManager()

        self.config = load_config(config_path)
        if not self.config:
            raise ValueError("Ошибка загрузки конфигурации!")

        self.tags_paths = self.config['tags']

    def _prepare_contact(self, customer_data, tags_file):
        """Подготовка поля contact (ФИО) для записи."""
        if tags_file == self.tags_paths['get_tags_44_new']:
            contact_parts = [
                (customer_data.get("contact_last_name") or "").strip(),
                (customer_data.get("contact_first_name") or "").strip(),
                (customer_data.get("contact_middle_name") or "").strip()
            ]
        elif tags_file == self.tags_paths['get_tags_223_new']:
            contact_parts = [
                customer_data.get("contact_last_name") or None,
                customer_data.get("contact_first_name") or None,
                customer_data.get("contact_middle_name") or None
            ]
        else:
            contact_parts = []

        # Убираем пустые строки, заменяем на None
        contact = " ".join([part for part in contact_parts if part]).strip() or None
        return contact

    def _update_field(self, existing_value, new_value):
        """Если новое значение отличается от существующего, добавляем его через ;"""
        if not new_value:  # Если новое значение пустое или None, оставляем старое
            return existing_value
        if existing_value and existing_value != new_value:
            return f"{existing_value}; {new_value}"
        return new_value

    def _is_contact_exists(self, contact, cursor):
        """Проверяем, существует ли уже контакт в базе данных."""
        if contact is None:
            return False  # Если контакт равен None, значит, его не существует в базе
        try:
            cursor.execute("""SELECT COUNT(1) FROM customer WHERE contact = %s""", (contact,))
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logger.error(f"Ошибка при проверке контакта: {e}")
            return False

    def _insert_data(self, table_name, data, cursor=None):
        """Универсальная функция для вставки данных в любую таблицу."""
        try:
            use_local_cursor = False
            if cursor is None:
                cursor = self.db_manager.connection.cursor()
                use_local_cursor = True  # Если курсор не передан, значит, коммитить должны сами

            # Проверяем и заменяем пустые значения
            for column, value in data.items():
                if value is None or value == '':
                    data[column] = None  # Заменяем пустую строку на None

            # Проверка на уникальность контакта
            contact = data.get('contact')
            if contact and self._is_contact_exists(contact, cursor):
                # Считаем пропуск из-за существующего контакта
                try:
                    stats_collector.increment(f"{table_name}_skipped_contact", 1)
                except Exception:
                    pass
                return None

            columns = ', '.join(data.keys())
            values = tuple(data.values())
            placeholders = ', '.join(['%s'] * len(data))

            insert_query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({placeholders}) RETURNING id
            """
            cursor.execute(insert_query, values)
            inserted_id = cursor.fetchone()[0]

            # Если курсор локальный, коммитим
            if use_local_cursor:
                self.db_manager.connection.commit()

            # Инкрементируем статистику успешных вставок по имени таблицы
            try:
                stats_collector.increment(table_name, 1)
            except Exception:
                # Никогда не падаем из-за подсчёта статистики
                pass

            # DEBUG: Логируем успешную вставку данных
            logger.debug(f"✅ Успешно записано в БД: таблица '{table_name}', id={inserted_id}")
            # Для важных таблиц логируем дополнительную информацию
            if table_name in ['reestr_contract_44_fz', 'reestr_contract_223_fz']:
                contract_number = data.get('contract_number', 'неизвестен')
                logger.debug(f"   → Контракт {contract_number} (id={inserted_id}) записан в {table_name}")

            return inserted_id

        except IntegrityError as e:
            # Это нормально для дублирующихся записей - считаем пропуск
            # Но нужно откатить транзакцию, чтобы она не осталась в состоянии ошибки
            try:
                self.db_manager.connection.rollback()
            except Exception:
                pass
            try:
                stats_collector.increment(f"{table_name}_skipped_duplicate", 1)
            except Exception:
                pass
            # Логируем детали для подрядчиков и заказчиков, чтобы понять причину
            if table_name in ['contractor', 'customer']:
                logger.error(f"IntegrityError при вставке {table_name}: {e} | Данные: {data}")
            return None
        except Exception as e:
            # Детальное логирование для подрядчиков и заказчиков
            if table_name in ['contractor', 'customer']:
                entity_name = "подрядчика" if table_name == 'contractor' else "заказчика"
                inn_field = 'inn' if table_name == 'contractor' else 'customer_inn'
                inn = data.get(inn_field, 'неизвестен')
                logger.error(f"Ошибка при вставке {entity_name} в БД (ИНН {inn}): {e}", exc_info=True)
                logger.error(f"Данные {entity_name}: {data}")
                # Проверяем длину полей
                for key, value in data.items():
                    if value and isinstance(value, str) and len(value) > 500:
                        logger.error(f"Поле {key} слишком длинное ({len(value)} символов): {value[:100]}...")
            else:
                logger.error(f"Ошибка при вставке данных в {table_name}: {e}", exc_info=True)
            try:
                self.db_manager.connection.rollback()
            except Exception:
                pass
            return None
        finally:
            if use_local_cursor:
                cursor.close()

    def insert_customer(self, customer_data, tags_file):
        """Вставка нового заказчика в таблицу с валидацией."""
        try:
            # Проверяем наличие обязательного поля ИНН
            inn = customer_data.get('customer_inn')
            if not inn or (isinstance(inn, str) and not inn.strip()):
                logger.error(f"Попытка вставить заказчика без ИНН. Данные: {customer_data}")
                return None
            
            contact = self._prepare_contact(customer_data, tags_file)
            customer_data['contact'] = contact

            # Удаляем ненужные ключи, которых нет в БД
            customer_data.pop("contact_last_name", None)
            customer_data.pop("contact_first_name", None)
            customer_data.pop("contact_middle_name", None)

            result = self._insert_data('customer', customer_data)
            
            if not result:
                # Детальное логирование при неудачной вставке
                logger.error(f"Не удалось добавить заказчика с ИНН {inn}. Данные: {customer_data}")
            
            return result

        except Exception as e:
            logger.error(f"Ошибка при вставке заказчика с ИНН {customer_data.get('customer_inn', 'неизвестен')}: {e}", exc_info=True)
            logger.error(f"Данные заказчика: {customer_data}")
            return None

    def update_customer(self, customer_data, customer_id, tags_file):
        """Обновление данных заказчика."""
        try:
            with self.db_manager.connection.cursor() as cursor:
                contact = self._prepare_contact(customer_data, tags_file)
                legal_address = customer_data.get("customer_legal_address")
                actual_address = customer_data.get("customer_actual_address")
                phone = customer_data.get("contact_phone")
                email = customer_data.get("contact_email")

                cursor.execute(""" 
                    SELECT contact, contact_phone, contact_email, customer_legal_address, customer_actual_address
                    FROM customer WHERE id = %s
                """, (customer_id,))
                existing_customer = cursor.fetchone()

                if existing_customer:
                    existing_contact, existing_phone, existing_email, existing_legal_address, existing_actual_address = existing_customer

                    update_fields = []
                    update_values = []

                    if legal_address and legal_address != existing_legal_address:
                        update_fields.append("customer_legal_address = %s")
                        update_values.append(legal_address)

                    if actual_address and actual_address != existing_actual_address:
                        update_fields.append("customer_actual_address = %s")
                        update_values.append(actual_address)

                    new_contact = self._update_field(existing_contact, contact)
                    if new_contact != existing_contact:
                        update_fields.append("contact = %s")
                        update_values.append(new_contact)

                    new_phone = self._update_field(existing_phone, phone)
                    if new_phone != existing_phone:
                        update_fields.append("contact_phone = %s")
                        update_values.append(new_phone)

                    new_email = self._update_field(existing_email, email)
                    if new_email != existing_email:
                        update_fields.append("contact_email = %s")
                        update_values.append(new_email)

                    if update_fields:
                        update_query = f"""
                            UPDATE customer
                            SET {', '.join(update_fields)}
                            WHERE id = %s
                        """
                        update_values.append(customer_id)
                        cursor.execute(update_query, tuple(update_values))
                        self.db_manager.connection.commit()  # <-- ДОБАВИЛ КОМИТ
                else:
                    logger.error(f"Запись с id {customer_id} не найдена для обновления в таблице customer")
                    return None

                return customer_id

        except Exception as e:
            logger.error(f"Ошибка при обновлении данных в customer: {e}")
            self.db_manager.connection.rollback()
            return None

    def insert_file_name(self, file_name):
        """Вставляет имя обработанного XML-файла в таблицу file_names_xml с timestamp."""
        try:
            with self.db_manager.connection.cursor() as cursor:  # Используем контекстный менеджер
                # Проверяем, есть ли поле processed_at в таблице
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'file_names_xml' 
                    AND column_name = 'processed_at'
                """)
                has_timestamp = cursor.fetchone() is not None
                
                if has_timestamp:
                    insert_query = """
                        INSERT INTO file_names_xml (file_name, processed_at)
                        VALUES (%s, CURRENT_TIMESTAMP) RETURNING id;
                    """
                else:
                    # Если поля нет, используем старый запрос (для обратной совместимости)
                    insert_query = """
                        INSERT INTO file_names_xml (file_name)
                        VALUES (%s) RETURNING id;
                    """
                
                cursor.execute(insert_query, (file_name,))
                inserted_id = cursor.fetchone()[0]
                self.db_manager.connection.commit()

                # Успешно добавили имя файла — фиксируем статистику
                try:
                    stats_collector.increment('file_names_xml', 1)
                except Exception:
                    pass

                # DEBUG: Логируем успешную вставку файла
                logger.debug(f"✅ Файл обработан и записан в file_names_xml: {file_name[:80]}... (id={inserted_id})")

                return inserted_id

        except IntegrityError as e:
            # Это нормально, если файл уже существует
            return None
        except Exception as e:
            logger.error(f"Ошибка при вставке имени файла {file_name}: {e}")
            self.db_manager.connection.rollback()
            return None

    def _update_existing_contract(self, contract_id, contract_data, table_name=None):
        """
        Обновление данных существующего контракта.
        Если table_name не указана, проверяет все статусные таблицы.
        
        :param contract_id: ID контракта для обновления
        :param contract_data: Словарь с данными для обновления
        :param table_name: Имя таблицы для обновления (опционально)
        :return: contract_id или None при ошибке
        """
        try:
            # Проверяем состояние транзакции - если она в состоянии ошибки, делаем rollback
            if self.db_manager.connection.status == 1:  # 1 = STATUS_IN_ERROR
                try:
                    self.db_manager.connection.rollback()
                except Exception:
                    pass
            
            # Если table_name не указана, ищем контракт во всех статусных таблицах
            if not table_name:
                from database_work.database_id_fetcher import DatabaseIDFetcher
                db_id_fetcher = DatabaseIDFetcher()
                
                # Пробуем найти контракт по ID в основных таблицах
                # Сначала проверяем основную таблицу 44-ФЗ
                cursor = self.db_manager.connection.cursor()
                cursor.execute("SELECT contract_number FROM reestr_contract_44_fz WHERE id = %s", (contract_id,))
                result = cursor.fetchone()
                
                if result:
                    contract_number = result[0]
                    # Проверяем, что найденный контракт имеет тот же ID
                    found_id, found_table = db_id_fetcher.get_reestr_contract_44_fz_id(contract_number, return_table=True)
                    if found_id == contract_id:
                        table_name = found_table
                    else:
                        # Пробуем 223-ФЗ
                        cursor.execute("SELECT contract_number FROM reestr_contract_223_fz WHERE id = %s", (contract_id,))
                        result = cursor.fetchone()
                        if result:
                            contract_number = result[0]
                            found_id, found_table = db_id_fetcher.get_reestr_contract_223_fz_id(contract_number, return_table=True)
                            if found_id == contract_id:
                                table_name = found_table
                            else:
                                table_name = None
                        else:
                            table_name = None
                else:
                    # Пробуем 223-ФЗ
                    cursor.execute("SELECT contract_number FROM reestr_contract_223_fz WHERE id = %s", (contract_id,))
                    result = cursor.fetchone()
                    if result:
                        contract_number = result[0]
                        found_id, found_table = db_id_fetcher.get_reestr_contract_223_fz_id(contract_number, return_table=True)
                        if found_id == contract_id:
                            table_name = found_table
                        else:
                            table_name = None
                    else:
                        table_name = None
                
                cursor.close()
                db_id_fetcher.close()
                
                # Если не нашли, используем основную таблицу 44-ФЗ по умолчанию
                if not table_name:
                    table_name = "reestr_contract_44_fz"
            
            with self.db_manager.connection.cursor() as cursor:
                update_columns = []
                update_values = []

                for column, value in contract_data.items():
                    if value is not None:
                        update_columns.append(f"{column} = %s")
                        update_values.append(value)

                if update_columns:
                    update_query = f"""
                        UPDATE {table_name}
                        SET {', '.join(update_columns)}
                        WHERE id = %s
                    """
                    update_values.append(contract_id)
                    cursor.execute(update_query, tuple(update_values))
                    self.db_manager.connection.commit()

                    return contract_id
                else:
                    return contract_id
        except Exception as e:
            logger.error(f"Ошибка при обновлении контракта {contract_id} в таблице {table_name}: {e}")
            try:
                self.db_manager.connection.rollback()
            except Exception:
                pass
            return None

    # Пример вставки в другие таблицы, аналогично insert_customer
    def insert_trading_platform(self, trading_platform_data, cursor=None):
        return self._insert_data('trading_platform', trading_platform_data, cursor)

    def insert_reestr_contract_44_fz(self, contract_data, cursor=None):
        return self._insert_data('reestr_contract_44_fz', contract_data, cursor)

    def insert_link_documentation_44_fz(self, links_44_fz_data, cursor=None):
        return self._insert_data('links_documentation_44_fz', links_44_fz_data, cursor)

    def insert_reestr_contract_223_fz(self, contract_data, cursor=None):
        return self._insert_data('reestr_contract_223_fz', contract_data, cursor)

    def insert_link_documentation_223_fz(self, links_44_fz_data, cursor=None):
        return self._insert_data('links_documentation_223_fz', links_44_fz_data, cursor)

    def insert_contractor(self, contractor_data, cursor=None):
        """
        Вставка подрядчика с валидацией и нормализацией данных.
        """
        # Проверяем наличие обязательного поля ИНН
        inn = contractor_data.get('inn')
        if not inn or (isinstance(inn, str) and not inn.strip()):
            logger.error(f"Попытка вставить подрядчика без ИНН. Данные: {contractor_data}")
            return None
        
        # Создаем копию данных для нормализации
        normalized_data = {}
        
        # Максимальные длины полей (примерные, нужно проверить схему БД)
        max_lengths = {
            'short_name': 500,
            'full_name': 1000,
            'inn': 20,
            'kpp': 20,
            'legal_address': 1000,
            'email': 255,
            'phone': 100,
        }
        
        for key, value in contractor_data.items():
            if value is None:
                normalized_data[key] = None
            elif isinstance(value, str):
                # Убираем лишние пробелы
                value = value.strip()
                # Если пустая строка после trim - делаем None (кроме ИНН)
                if not value:
                    if key == 'inn':
                        # ИНН обязателен, не может быть пустым
                        logger.error(f"ИНН подрядчика пустой после нормализации. Исходные данные: {contractor_data}")
                        return None
                    normalized_data[key] = None
                else:
                    # Обрезаем слишком длинные строки
                    max_len = max_lengths.get(key, 1000)
                    if len(value) > max_len:
                        logger.warning(f"Поле {key} обрезано с {len(value)} до {max_len} символов для подрядчика ИНН {inn}")
                        value = value[:max_len]
                    normalized_data[key] = value
            else:
                # Для не-строковых значений оставляем как есть
                normalized_data[key] = value
        
        return self._insert_data('contractor', normalized_data, cursor)