import json
import xml.etree.ElementTree as ET
from loguru import logger
import re
from datetime import datetime

from secondary_functions import load_config
from database_work.database_operations import DatabaseOperations
from database_work.database_id_fetcher import DatabaseIDFetcher
from file_delete.file_deleter import FileDeleter

class XMLParser:
    """
    Класс для обработки XML-файлов в указанной директории.
    """

    def __init__(self, config_path="config.ini"):
        """
        Загружает конфигурацию и путь к XML-файлам из config.ini.
        """

        # Инициализируем методы для работы с базой данных внутри XMLParser
        self.database_operations = DatabaseOperations()
        self.db_id_fetcher = DatabaseIDFetcher()

        self.config = load_config(config_path)
        if not self.config:
            raise ValueError("Ошибка загрузки конфигурации!")

        # Пути к папкам с XML и теги для каждой папки из конфигурации
        self.xml_paths = self.config['path']
        self.tags_paths = self.config['tags']

    @staticmethod
    def remove_namespaces(xml_string):
        """
        Полностью удаляет все пространства имен из XML-строки.
        Убирает как префиксы, так и их определения.
        """
        logger.warning('запуск функции: remove_namespaces')
        # Удаление всех атрибутов xmlns:... и xmlns="..."
        no_namespaces = re.sub(r'\sxmlns(:\w+)?="[^"]+"', '', xml_string)

        # Удаление всех префиксов вида <ns3:tag> и </ns3:tag>
        no_namespaces = re.sub(r'<(/?)(\w+):', r'<\1', no_namespaces)

        # Также важно удалить префикс внутри атрибутов, если он есть (например, ns5:href)
        no_namespaces = re.sub(r'(\s)(\w+):', r'\1', no_namespaces)

        return no_namespaces

    def load_json_tags(self, tags_path):
        """
        Загружает теги из указанного JSON файла.
        """
        try:
            with open(tags_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка при загрузке JSON файла с тегами {tags_path}: {e}")
            return None

    def parse_reestr_contract_44_fz(self, root, tags, region_code, okpd_code, customer_id, platform_id, tags_file,
                                    file_path, xml_folder_path):
        """
        Парсит данные для таблицы реестра контрактов 44-ФЗ и вставляет в БД.
        Если поле 'auction_name' пустое, прекращает обработку и удаляет файл через FileDeleter.
        """
        found_tags = self._parse_common_contract_data(root, tags, region_code, okpd_code, customer_id, platform_id,
                                                      tags_file)

        # Проверяем, что поле auction_name не пустое
        if not found_tags.get('auction_name'):
            logger.warning(f"Поле 'auction_name' пустое для файла: {tags_file}. Прекращаем обработку и удаляем файл.")

            # Удаляем файл через FileDeleter
            file_deleter = FileDeleter(xml_folder_path)
            file_deleter.delete_single_file(file_path)
            logger.info(f"Файл {tags_file} удален.")

            # Прекращаем дальнейшую обработку
            return None

        # Если значение поля 'auction_name' присутствует, продолжаем вставку данных
        contract_id = self.database_operations.insert_reestr_contract_44_fz(found_tags)
        logger.info(f"Вставленная запись для 44-ФЗ имеет id: {contract_id}")

        return contract_id

    def parse_reestr_contract_223_fz(self, root, tags, region_code, okpd_code, customer_id, platform_id, tags_file,
                                     file_path, xml_folder_path):
        """
        Парсит данные для таблицы реестра контрактов 223-ФЗ и вставляет в БД.
        """
        # Парсим общие данные контракта
        found_tags = self._parse_common_contract_data(root, tags, region_code, okpd_code, customer_id, platform_id,
                                                      tags_file)

        # Проверяем, если нет значения для contract_number, пропускаем обработку и удаляем файл
        if not found_tags.get('contract_number'):
            logger.warning(f"Отсутствует contract_number в файле {tags_file}. Пропускаем файл и удаляем его.")

            # Удаляем файл через FileDeleter
            file_deleter = FileDeleter(xml_folder_path)
            file_deleter.delete_single_file(file_path)
            logger.info(f"Файл {tags_file} удален.")

            # Прекращаем дальнейшую обработку
            return None

        # Вставляем данные в таблицу reestr_contract_223_fz
        contract_id = self.database_operations.insert_reestr_contract_223_fz(found_tags)
        logger.info(f"Вставленная запись для 223-ФЗ имеет id: {contract_id}")

        return contract_id

    def _parse_common_contract_data(self, root, tags, region_code, okpd_code, customer_id, platform_id, tags_file):
        """
        Общая логика парсинга данных для контрактов, используемая для 44-ФЗ и 223-ФЗ.
        """
        found_tags = {}

        # Парсинг общих данных
        for tag, xpath in tags.items():
            tag_without_namespace = xpath.split(":")[-1]
            elements = root.findall(f".//{tag_without_namespace}")

            if elements:
                values = [elem.text.strip() for elem in elements if elem.text and elem.text.strip()]
                found_tags[tag] = values[0] if values else None
            else:
                found_tags[tag] = None

            # Обрабатываем start_date, end_date и initial_price
            if tag == "start_date" and not found_tags[tag]:
                found_tags[tag] = datetime.now().strftime('%Y-%m-%d')

            if tag == "end_date" and not found_tags[tag]:
                found_tags[tag] = datetime.now().strftime('%Y-%m-%d')

            if tag == "initial_price" and not found_tags[tag]:
                found_tags[tag] = 0

        # Добавляем дополнительные параметры
        found_tags['region_id'] = self.db_id_fetcher.get_region_id(region_code)
        found_tags['okpd_id'] = self.db_id_fetcher.get_okpd_id(okpd_code)
        found_tags['customer_id'] = customer_id
        found_tags['trading_platform_id'] = platform_id

        return found_tags

    def parse_trading_platform(self, root, tags):
        """
        Парсит данные для таблицы trading_platform, проверяя наличие записи.
        Если запись уже есть, просто возвращает ее ID, иначе создает новую запись.
        """
        found_tags = {}

        # Парсим данные из XML
        for tag, xpath in tags.items():
            element = root.find(f".//{xpath}")  # Добавляем ".//" для поиска на любом уровне
            found_tags[tag] = element.text.strip() if element is not None and element.text else None

        # Получаем имя торговой площадки
        trading_platform_name = found_tags.get('trading_platform_name')

        # Если имя торговой площадки не найдено, ставим дефолтное значение
        if not trading_platform_name:
            trading_platform_name = "Торговая площадка не найдена"  # Присваиваем дефолтное значение

        # Проверяем, есть ли в базе запись с этим именем
        platform_id = self.db_id_fetcher.get_trading_platform_id(trading_platform_name)

        # Если площадка уже существует, возвращаем её ID
        if platform_id:
            logger.info(f"Торговая площадка '{trading_platform_name}' уже существует, ID: {platform_id}")
            return platform_id

        # Если площадки нет в БД, создаем новую запись
        found_tags['trading_platform_name'] = trading_platform_name

        # Проверяем наличие URL, если его нет, ставим дефолтный
        if not found_tags.get('trading_platform_url'):
            found_tags['trading_platform_url'] = "https://нет.ссылки"  # Устанавливаем дефолтный URL

        # Вставляем данные в таблицу
        platform_id = self.database_operations.insert_trading_platform(found_tags)

        if platform_id:
            logger.info(f"Добавлена торговая площадка '{trading_platform_name}' с ID: {platform_id}")
        else:
            logger.error(f"Не удалось добавить торговую площадку '{trading_platform_name}' в БД.")

        return platform_id  # Возвращаем ID, который был найден или создан

    def parse_links_documentation(self, root, links_documentation_tags, contract_id, tags_file):
        """
        Парсит данные для таблицы links_documentation_44_fz (или 223_fz)
        и вызывает парсинг для таблицы printFormInfo.
        """
        found_tags = []

        for tag_name, tag_data in links_documentation_tags.items():
            xpath = tag_data.get("xpath")
            if not xpath:
                logger.warning(f"Отсутствует xpath в секции {tag_name}")
                continue

            # Ищем элементы по заданному XPath
            for elem in root.findall(xpath):
                # Если нет file_name, используем default_file_name, если он есть, или пропускаем
                file_name_tag = tag_data.get("file_name")
                if not file_name_tag:
                    # Если file_name отсутствует, используем default_file_name, если он есть
                    file_name_tag = tag_data.get("default_file_name", tag_name)

                file_name_elem = elem.find(file_name_tag)
                url_elem = elem.find(tag_data.get("document_links"))

                file_name = file_name_elem.text.strip() if file_name_elem is not None and file_name_elem.text else file_name_tag
                url = url_elem.text.strip() if url_elem is not None and url_elem.text else None

                # Если URL найден, добавляем информацию в список
                if url:
                    found_tags.append({
                        "file_name": file_name,
                        "document_links": url,
                        "contract_id": contract_id
                    })

        # Вставляем все собранные данные для соответствующей таблицы в базу
        for entry in found_tags:
            if entry:  # Если данные не пустые
                if tags_file == self.tags_paths['get_tags_44_new']:
                    inserted_id = self.database_operations.insert_link_documentation_44_fz(entry)
                elif tags_file == self.tags_paths['get_tags_223_new']:
                    inserted_id = self.database_operations.insert_link_documentation_223_fz(entry)
                else:
                    logger.error(f"Неизвестный файл тегов: {tags_file}")
                    continue
                logger.info(f"Вставленная запись в {tags_file} имеет id: {inserted_id}")

        # Возвращаем все найденные данные
        return found_tags

    def parse_customer(self, root, tags, tags_file):
        """
        Парсит данные для таблицы customer, проверяя наличие ИНН в базе данных.
        Если ИНН существует, обновляет данные, если нет — добавляет нового заказчика.
        """
        found_tags = {}

        for tag, xpath in tags.items():
            element = root.find(f".//{xpath}")

            if element is None or element.text is None:
                found_tags[tag] = None
                logger.warning(f"Не найден тег '{tag}' в XML.")
                continue

            try:
                if tags_file == self.tags_paths['get_tags_44_new']:
                    found_tags[tag] = element.text.strip() if element.text else None
                elif tags_file == self.tags_paths['get_tags_223_new']:
                    found_tags[tag] = element.text
                else:
                    logger.error(f"Неизвестный файл тегов: {tags_file}")
                    return None

            except AttributeError:
                logger.error(f"Ошибка при обработке тега '{tag}': element.text = {element.text}")
                found_tags[tag] = None

        # Проверяем наличие ИНН
        inn = found_tags.get('customer_inn')
        if inn:
            customer_id = self.db_id_fetcher.get_customer_id(inn)

            if customer_id:
                # Закомментировать следующую часть, чтобы отключить обновление данных
                # logger.info(f"Обновляем данные заказчика с ID {customer_id}")
                # customer_data = found_tags
                # self.database_operations.update_customer(customer_data, customer_id, tags_file)

                # Логирование, что обновление отключено
                logger.info(f"Обновление данных заказчика с ID {customer_id} временно отключено.")
            else:
                # Создаем нового заказчика, если не найден
                logger.info(f"Заказчик с ИНН {inn} не найден, создаем нового.")
                customer_data = found_tags
                customer_id = self.database_operations.insert_customer(customer_data, tags_file)
                if customer_id:
                    logger.info(f"Новый заказчик добавлен с ID {customer_id}")
                else:
                    logger.error(f"Не удалось добавить нового заказчика с ИНН {inn}")
        else:
            logger.warning("ИНН не найден в данных.")

        return customer_id

    def parse_xml_tags(self, file_path, region_code, okpd_code, xml_folder_path):
        """
        Функция для извлечения тегов для одной записи XML.
        :param file_path: Путь к конкретному XML файлу для обработки
        :param region_code: Код региона
        :param okpd_code: Код ОКПД для обработки
        """
        logger.info(f"Обрабатываем файл: {file_path}")

        # Определяем, какой JSON файл использовать в зависимости от папки
        if xml_folder_path == self.xml_paths['reest_new_contract_archive_44_fz_xml']:
            tags_file = self.tags_paths['get_tags_44_new']
        elif xml_folder_path == self.xml_paths['reest_new_contract_archive_223_fz_xml']:
            tags_file = self.tags_paths['get_tags_223_new']
        else:
            logger.error(f"Неизвестная папка: {xml_folder_path}")
            return None

        # Загружаем теги из соответствующего JSON файла
        tags = self.load_json_tags(tags_file)  # Определение файла тегов по пути

        if not tags_file:
            logger.error(f"Не удалось найти файл тегов для файла {file_path}")
            return None

        tags = self.load_json_tags(tags_file)
        if not tags:
            logger.error("Не удалось загрузить теги из JSON.")
            return None

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
            return

        # Получаем данные о заказчике
        customer_id = self.parse_customer(
            root,
            tags.get('customer', {}),
            tags_file  # Передаем сюда tags_file
        )

        # Получаем данные о торговой площадке
        platform_id = self.parse_trading_platform(root, tags.get('trading_platform', {}))

        # Выбираем правильную функцию для контракта
        if tags_file == self.tags_paths['get_tags_44_new']:
            contract_id = self.parse_reestr_contract_44_fz(
                root,
                tags.get('reestr_contract', {}),
                region_code,
                okpd_code,
                customer_id,  # Передаем customer_id
                platform_id,
                tags_file,
                file_path,
                xml_folder_path
            )
        elif tags_file == self.tags_paths['get_tags_223_new']:
            contract_id = self.parse_reestr_contract_223_fz(
                root,
                tags.get('reestr_contract', {}),
                region_code,
                okpd_code,
                customer_id,  # Передаем customer_id
                platform_id,
                tags_file,
                file_path,
                xml_folder_path
            )

        if not contract_id:
            logger.info(f"Пропускаем файл {file_path} из-за отсутствия contract_number")
            return

        # Парсим ссылки и документацию
        links_documentation = self.parse_links_documentation(
            root,
            tags.get('links_documentation', {}),
            contract_id,
            tags_file
        )

        logger.info(f"Успешно обработан файл {file_path}")
