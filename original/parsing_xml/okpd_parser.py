import os
from loguru import logger
import xml.etree.ElementTree as ET
import time

from secondary_functions import load_config
from database_work.check_database import DatabaseCheckManager
from database_work.database_id_fetcher import DatabaseIDFetcher
from file_delete.file_deleter import FileDeleter
from parsing_xml.xml_parser import XMLParser  # Импортируем функцию process_file из xml_parser.py
from parsing_xml.xml_parser_recouped_contract import AdvancedXMLParser
from database_work.database_operations import DatabaseOperations

def process_okpd_files(folder_path, region_code):
    """
    Общая функция для запуска всех этапов обработки.
    :param folder_path: Путь к папке с распакованными файлами
    :param region_code: Код региона из SOAP-запроса
    """
    db_id_fetcher = DatabaseIDFetcher()
    region_id = db_id_fetcher.get_region_id(region_code)

    if not region_id:
        logger.error(f"Не удалось получить ID региона для кода {region_code}")
        return

    # Загружаем конфигурацию и получаем пути папок
    config = load_config()
    recouped_contract_archive_44_fz_xml = config.get('path', 'recouped_contract_archive_44_fz_xml', fallback=None)
    recouped_contract_archive_223_fz_xml = config.get('path', 'recouped_contract_archive_223_fz_xml', fallback=None)

    # Обработка файлов контрактов или удаление файлов из папки
    if folder_path == recouped_contract_archive_44_fz_xml:
        process_contract_files(folder_path, db_id_fetcher)
    elif folder_path == recouped_contract_archive_223_fz_xml:
        # Удаляем файл из папки
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
                logger.info(f"Файл {file_name} удален из папки {folder_path}")
    else:
        process_okpd_files_normal(folder_path, db_id_fetcher, region_code)


def process_contract_files(folder_path, db_id_fetcher):
    """
    Обрабатывает файлы контрактов в папках с архивами контрактов (44-ФЗ, 223-ФЗ).
    :param folder_path: Путь к папке с контрактами
    :param db_id_fetcher: Объект для получения данных из базы
    :param region_code: Код региона из SOAP-запроса
    """
    file_deleter = FileDeleter(folder_path)
    logger.info(f"Обрабатываем папку с контрактами: {folder_path}")

    for file_name in os.listdir(folder_path):
        if not file_name.endswith(".xml"):
            continue

        file_path = os.path.join(folder_path, file_name)
        logger.info(f"Обрабатываем файл: {file_name}")

        process_contract_file(file_path, file_name, db_id_fetcher, file_deleter, folder_path)


def process_contract_file(file_path, file_name, db_id_fetcher, file_deleter, folder_path):
    """
    Обрабатывает конкретный файл контракта, извлекая номер контракта и проверяя его в базе данных.
    :param file_path: Путь к XML файлу
    :param file_name: Имя XML файла
    :param db_id_fetcher: Объект для получения данных из базы
    :param region_code: Код региона из SOAP-запроса
    :param file_deleter: Объект для удаления файлов
    """
    try:
        # Проверка наличия файла в базе данных перед его открытием
        file_id = db_id_fetcher.get_file_names_xml_id(file_name)
        if file_id:
            logger.info(f"Файл {file_name} уже был записан в БД. Завершаем обработку.")
            file_deleter.delete_single_file(file_path)
            return

        # Если файла нет в базе данных, добавляем имя файла в базу
        logger.info(f"Файл {file_name} не найден в базе данных, записываем в БД.")
        db_operations = DatabaseOperations()
        db_operations.insert_file_name(file_name)

        # Открываем файл и начинаем его обработку
        with open(file_path, "r", encoding="utf-8") as file:
            xml_content = file.read()

        xml_content = XMLParser.remove_namespaces(xml_content)
        root = ET.fromstring(xml_content)

        contract_number = extract_contract_number(root)
        if contract_number:
            logger.debug(f"Найден номер контракта: {contract_number}")

            # Проверка контракта в базе данных
            contract_id = db_id_fetcher.contract_number_44_fz_id(contract_number)
            if contract_id:
                logger.debug(f"Номер контракта {contract_number} найден в базе данных.")
                process_contract_with_number(file_path, contract_number, folder_path)
            else:
                logger.info(f"Номер контракта {contract_number} не найден в базе данных. Удаляем файл.")
                file_deleter.delete_single_file(file_path)
        else:
            logger.warning(f"Не найден номер контракта в файле {file_name}")
            file_deleter.delete_single_file(file_path)

    except Exception as e:
        logger.error(f"Ошибка при обработке файла {file_name}: {e}")
        file_deleter.delete_single_file(file_path)


def extract_contract_number(root):
    """
    Извлекает номер контракта из XML.
    :param root: Корень XML-дерева
    :return: Номер контракта или None, если не найден
    """

    xpath = "order/notificationNumber"  # Путь без namespace
    tag_without_namespace = xpath.split(":")[-1]  # Если вдруг останется namespac
    # Ищем все элементы с названием notificationNumber, игнорируя namespace
    contract_number_element = root.find(f".//{tag_without_namespace}")  # Ищем по пути

    return str(contract_number_element.text) if contract_number_element is not None else None


def process_contract_with_number(file_path, contract_number, folder_path):
    """
    Обрабатывает контракт с номером.
    :param file_path: Путь к файлу
    :param contract_number: Номер контракта
    """
    xml_parser_recouped = AdvancedXMLParser(config_path="config.ini")
    xml_parser_recouped.parse_xml_tags_recouped_contract(file_path, contract_number, folder_path)


def process_okpd_files_normal(folder_path, db_id_fetcher, region_code):
    """
    Обрабатывает обычные XML-файлы с кодами ОКПД.
    :param folder_path: Путь к папке с файлами
    :param db_id_fetcher: Объект для получения данных из базы
    :param region_code: Код региона из SOAP-запроса
    """
    file_deleter = FileDeleter(folder_path)
    logger.info(f"Начинаем парсинг XML-файлов новых контрактов в папке: {folder_path}")

    for file_name in os.listdir(folder_path):
        if not file_name.endswith(".xml"):
            continue

        file_path = os.path.join(folder_path, file_name)
        logger.info(f"Обрабатываем файл нового контракта: {file_name}")

        process_okpd_file(file_path, file_name, db_id_fetcher, region_code, file_deleter, folder_path)


def process_okpd_file(file_path, file_name, db_id_fetcher, region_code, file_deleter, folder_path):
    """
    Обрабатывает файл с кодами ОКПД.
    :param file_path: Путь к файлу
    :param file_name: Имя файла
    :param db_id_fetcher: Объект для получения данных из базы
    :param region_code: Код региона из SOAP-запроса
    :param file_deleter: Объект для удаления файлов
    """
    try:
        # Проверка наличия файла в базе данных перед его открытием
        file_id = db_id_fetcher.get_file_names_xml_id(file_name)
        if file_id:
            logger.info(f"Файл нового контракта {file_name} уже был записан в БД. Завершаем обработку.")
            file_deleter.delete_single_file(file_path)
            return

        # Если файла нет в базе данных, добавляем имя файла в базу
        logger.info(f"Файл нового контракта: {file_name} не найден в базе данных, записываем в БД.")
        db_operations = DatabaseOperations()
        db_operations.insert_file_name(file_name)

        # Открываем файл и начинаем его обработку
        with open(file_path, "r", encoding="utf-8") as file:
            xml_content = file.read()

        xml_content = XMLParser.remove_namespaces(xml_content)
        root = ET.fromstring(xml_content)

        okpd_code = extract_okpd_code(root)
        if okpd_code:
            logger.debug(f"Обработанный код ОКПД для файла {file_name}: {okpd_code}")
            process_okpd_code(okpd_code, file_path, region_code, folder_path)
        else:
            logger.warning(f"Не найден код ОКПД в файле {file_name}")
            file_deleter.delete_single_file(file_path)

    except Exception as e:
        logger.error(f"Ошибка при обработке файла {file_name}: {e}")
        file_deleter.delete_single_file(file_path)


def extract_okpd_code(root):
    """
    Извлекает код ОКПД из XML.
    :param root: Корень XML-дерева
    :return: Код ОКПД или None, если не найден
    """
    okpd_code_element = root.find(".//OKPDCode")
    if okpd_code_element is not None:
        return okpd_code_element.text

    okpd2_code_element = root.find(".//okpd2/code")
    if okpd2_code_element is not None:
        return okpd2_code_element.text

    return None


def process_okpd_code(okpd_code, file_path, region_code, folder_path):
    """
    Обрабатывает код ОКПД.
    :param okpd_code: Код ОКПД
    :param file_path: Путь к файлу
    :param region_code: Код региона из SOAP-запроса
    """
    # Если код состоит из 2-х частей и заканчивается на '0', убираем последний '0'
    if len(okpd_code.split('.')) == 2 and okpd_code.endswith('0'):
        okpd_code = okpd_code[:-1]

    # Проверяем код в базе данных
    db_id_fetcher = DatabaseIDFetcher()
    exists_in_db = db_id_fetcher.get_okpd_id(okpd_code)
    if exists_in_db:
        logger.debug(f"Код ОКПД {okpd_code} найден в базе данных.")
        xml_parser = XMLParser(config_path="config.ini")
        xml_parser.parse_xml_tags(file_path, region_code, okpd_code, folder_path)

        # Удаляем файл после обработки
        file_deleter = FileDeleter(file_path)
        file_deleter.delete_single_file(file_path)
    else:
        logger.info(f"Код ОКПД {okpd_code} не найден в базе данных, файл будет удален.")
        file_deleter = FileDeleter(file_path)
        file_deleter.delete_single_file(file_path)
