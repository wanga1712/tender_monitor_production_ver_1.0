import os
import re
import xml.etree.ElementTree as ET
from loguru import logger

from database_work.check_database import DatabaseCheckManager
from database_work.database_operations import DatabaseOperations
from database_work.database_id_fetcher import DatabaseIDFetcher
from parsing_xml.xml_parser import XMLParser  # Импортируем родительский класс
from file_delete.file_deleter import FileDeleter


class AdvancedXMLParser(XMLParser):
    """
    Дочерний класс, который наследует XMLParser и расширяет его функциональность.
    """

    def __init__(self, config_path="config.ini"):

        super().__init__(config_path)  # Инициализируем родительский класс XMLParser
        self.database_check_manager = DatabaseCheckManager()  # Менеджер для проверки БД

    def parse_reestr_contract_44_fz_recouped(self, root, tags, id_contract_number, contractor_id, tags_file):
        """
        Парсит данные для таблицы реестра контрактов 44-ФЗ и обновляет БД.
        """
        found_tags = {}

        # Парсинг общих данных
        for tag, xpath in tags.items():
            tag_without_namespace = xpath.split(":")[-1]
            elements = root.findall(f".//{tag_without_namespace}")  # Ищем элементы по пути

            if elements:
                values = [elem.text.strip() for elem in elements if elem.text and elem.text.strip()]
                found_tags[tag] = values[0] if values else None  # Сохраняем первое значение
            else:
                found_tags[tag] = None  # Если элементы не найдены, сохраняем None

        # Добавляем contractor_id
        found_tags['contractor_id'] = contractor_id

        logger.debug(f"Теги для контракта: {found_tags}")

        # Поиск всех тегов <endDate> в документе
        end_dates = root.findall(".//executionPeriod/endDate")

        if end_dates:
            last_end_date = end_dates[-1].text.strip() if end_dates[-1].text else None
            found_tags["delivery_end_date"] = last_end_date
            logger.info(f"Последний delivery_end_date найден: {last_end_date}")
        else:
            found_tags["delivery_end_date"] = None
            logger.warning("Тег executionPeriod/endDate не найден!")

        # Обновление данных в базе данных
        try:
            self.database_operations._update_existing_contract(id_contract_number, found_tags)

        except Exception as e:
            logger.error(f"Ошибка при обновлении контракта в базе данных: {e}")
            raise  # Прекращаем выполнение программы

    def parse_contractor(self, root, tags, tags_file):
        """
        Парсит данные для таблицы contractor, проверяя наличие ИНН в базе данных.
        Если ИНН существует, получаем его ID, если нет — добавляем нового поставщика и получаем его ID.
        """
        found_tags = {}

        # Проходим по всем тегам
        for tag, xpath in tags.items():
            element = root.find(f".//{xpath}")
            if element is None:
                logger.warning(f"Не найден тег '{tag}' по пути: .//{xpath}")
                found_tags[tag] = None
                continue

            try:
                if tags_file == self.tags_paths['get_tags_44_recouped']:
                    found_tags[tag] = element.text.strip() if element.text else None
                else:
                    logger.error(f"Неизвестный файл тегов: {tags_file}")
                    return None
            except AttributeError:
                logger.error(f"Ошибка при обработке тега '{tag}': element.text = {element.text}")
                found_tags[tag] = None

        # Проверка наличия ИНН
        inn = found_tags.get('inn')
        if inn:
            # Получаем ID контрагента по ИНН
            contractor_id = self.db_id_fetcher.get_contractor_id(inn)

            if contractor_id:
                # Поставщик найден, просто получаем ID
                logger.info(f"Поставщик с ИНН {inn} найден в базе данных. ID: {contractor_id}")
            else:
                # Поставщик не найден, создаем нового и получаем его ID
                logger.info(f"Поставщик с ИНН {inn} не найден, создаем нового.")
                contractor_id = self.database_operations.insert_contractor(found_tags)
                logger.info(f"Новый поставщик добавлен с ID {contractor_id}.")
        else:
            logger.warning("ИНН не найден в данных.")

        # Возвращаем ID контрагента (если нужно передать в другие функции)
        return contractor_id

    def parse_links_documentation_recouped(self, root, id_contract_number, links_documentation_tags, tags_file):
        """
        Универсальный метод: загружает теги из JSON-файла и парсит XML.
        """
        found_tags = []

        for tag_name, tag_data in links_documentation_tags.items():
            xpath = tag_data.get("xpath")
            if not xpath:
                logger.warning(f"Отсутствует xpath в секции {tag_name}")
                continue

            for elem in root.findall(xpath):
                file_name_tag = tag_data.get("file_name", tag_data.get("default_file_name", tag_name))
                file_name_elem = elem.find(file_name_tag)
                url_elem = elem.find(tag_data.get("document_links"))

                file_name = file_name_elem.text.strip() if file_name_elem is not None and file_name_elem.text else file_name_tag
                url = url_elem.text.strip() if url_elem is not None and url_elem.text else None

                if url:
                    found_tags.append({
                        "file_name": file_name,
                        "document_links": url,
                        "contract_id": id_contract_number
                    })
                    logger.info(f"Найдена ссылка для контракта {id_contract_number}: {url} ({file_name})")

        for entry in found_tags:
            logger.debug(f"Попытка вставки в базу: {entry}")
            try:
                inserted_id = self.database_operations.insert_link_documentation_44_fz(entry)
                if inserted_id:
                    logger.debug(
                        f"Успешная вставка: контракт {entry['contract_id']}, файл {entry['file_name']}, ссылка {entry['document_links']}")
                else:
                    logger.warning(f"Не удалось вставить запись для контракта {entry['contract_id']}: {entry}")
            except Exception as e:
                logger.error(f"Ошибка при вставке в базу (контракт {entry['contract_id']}): {e}")
                raise

        return found_tags

    def parse_xml_tags_recouped_contract(self, file_path, contract_number, xml_folder_path):
        """
        Функция для извлечения тегов для одной записи XML.
        """
        logger.info(f"Обрабатываем файл: {file_path}")

        # Определяем, какой JSON файл использовать в зависимости от папки
        if xml_folder_path == self.xml_paths['recouped_contract_archive_44_fz_xml']:
            tags_file = self.tags_paths['get_tags_44_recouped']
        elif xml_folder_path == self.xml_paths['recouped_contract_archive_223_fz_xml']:
            tags_file = self.tags_paths['get_tags_223_recouped']
        else:
            logger.error(f"Неизвестная папка: {xml_folder_path}")
            raise ValueError(f"Неизвестная папка: {xml_folder_path}")  # Прекращаем выполнение программы

        # Загружаем теги из соответствующего JSON файла
        tags = self.load_json_tags(tags_file)

        if not tags:
            logger.error("Не удалось загрузить теги из JSON.")
            raise ValueError("Не удалось загрузить теги из JSON.")  # Прекращаем выполнение программы

        # Загружаем и парсим XML
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                xml_content = f.read()

            # Удаляем пространства имен перед парсингом
            cleaned_xml_content = self.remove_namespaces(xml_content)

            tree = ET.ElementTree(ET.fromstring(cleaned_xml_content))
            root = tree.getroot()

        except ET.ParseError as e:
            logger.error(f"Ошибка при парсинге XML-файла {file_path}: {e}")
            raise  # Прекращаем выполнение программы

        # Получаем данные о поставщике
        contractor_id = self.parse_contractor(root, tags.get('contractor', {}), tags_file)

        id_contract_number = self.db_id_fetcher.get_reestr_contract_44_fz_id(str(contract_number))

        # Выбираем правильную функцию для контракта
        if tags_file == self.tags_paths['get_tags_44_recouped']:
            contract_id = self.parse_reestr_contract_44_fz_recouped(
                root,
                tags.get('reestr_contract', {}),
                id_contract_number,
                contractor_id,
                tags_file
            )
        elif tags_file == self.tags_paths['get_tags_223_new']:
            # Удаляем файл через FileDeleter
            file_deleter = FileDeleter(xml_folder_path)
            file_deleter.delete_single_file(file_path)

        # Парсим ссылки и документацию
        logger.debug(f"Начинаем парсить ссылки и документацию для контракта {id_contract_number}")
        links_documentation = self.parse_links_documentation_recouped(
            root,
            id_contract_number,
            tags.get("links_documentation", {}),
            tags_file
        )
        logger.debug(f"Парсинг ссылок завершен для контракта {contract_id}")

        logger.info(f"Успешно обработан файл {file_path}")
