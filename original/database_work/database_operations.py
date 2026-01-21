from loguru import logger
from database_work.database_connection import DatabaseManager
from psycopg2 import IntegrityError
from secondary_functions import load_config


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
                logger.warning(f"Контакт {contact} уже существует в базе данных.")
                return None

            logger.debug(f"Вставляем в {table_name} данные: {data}")

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

            logger.info(f"Добавлена новая запись в таблицу {table_name} с id: {inserted_id}")
            return inserted_id

        except IntegrityError as e:
            logger.warning(f"Ошибка при вставке данных в {table_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при вставке данных в {table_name}: {e}")
            self.db_manager.connection.rollback()
            return None
        finally:
            if use_local_cursor:
                cursor.close()

    def insert_customer(self, customer_data, tags_file):
        """Вставка нового заказчика в таблицу."""
        try:
            contact = self._prepare_contact(customer_data, tags_file)
            customer_data['contact'] = contact

            # Удаляем ненужные ключи, которых нет в БД
            customer_data.pop("contact_last_name", None)
            customer_data.pop("contact_first_name", None)
            customer_data.pop("contact_middle_name", None)

            logger.info(f"Попытка вставки заказчика: {customer_data}")

            result = self._insert_data('customer', customer_data)
            logger.info(f"Заказчик {customer_data.get('inn')} успешно добавлен.")
            return result

        except Exception as e:
            logger.error(f"Ошибка при вставке заказчика с ИНН {customer_data.get('inn')}: {e}", exc_info=True)
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

                        logger.info(f"Обновлена запись в customer с id: {customer_id}")
                else:
                    logger.warning(f"Запись с id {customer_id} не найдена для обновления.")
                    return None

                return customer_id

        except Exception as e:
            logger.error(f"Ошибка при обновлении данных в customer: {e}")
            self.db_manager.connection.rollback()
            return None

    def insert_file_name(self, file_name):
        """Вставляет имя обработанного XML-файла в таблицу file_names_xml."""
        try:
            with self.db_manager.connection.cursor() as cursor:  # Используем контекстный менеджер
                insert_query = """
                    INSERT INTO file_names_xml (file_name)
                    VALUES (%s) RETURNING id;
                """
                cursor.execute(insert_query, (file_name,))
                inserted_id = cursor.fetchone()[0]
                self.db_manager.connection.commit()

                logger.info(f"Добавлено имя файла в file_names_xml с id: {inserted_id}")
                return inserted_id

        except IntegrityError as e:
            logger.warning(f"Ошибка при вставке имени файла {file_name} в file_names_xml: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при вставке имени файла {file_name}: {e}")
            self.db_manager.connection.rollback()
            return None

    def _update_existing_contract(self, contract_id, contract_data):
        """Обновление данных существующего контракта."""
        try:
            with self.db_manager.connection.cursor() as cursor:
                update_columns = []
                update_values = []

                for column, value in contract_data.items():
                    if value is not None:
                        update_columns.append(f"{column} = %s")
                        update_values.append(value)

                if update_columns:
                    update_query = f"""
                        UPDATE reestr_contract_44_fz
                        SET {', '.join(update_columns)}
                        WHERE id = %s
                    """
                    update_values.append(contract_id)
                    cursor.execute(update_query, tuple(update_values))
                    self.db_manager.connection.commit()  # <-- ДОБАВИЛ КОМИТ

                    logger.info(f"Контракт с номером {contract_id} успешно обновлен.")
                    return contract_id
                else:
                    logger.info(f"Нет данных для обновления для контракта {contract_id}.")
                    return contract_id
        except Exception as e:
            logger.error(f"Ошибка при обновлении контракта {contract_id}: {e}")
            self.db_manager.connection.rollback()
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
        return self._insert_data('contractor', contractor_data, cursor)
