import os
import uuid
import requests
from urllib.parse import urlparse
from loguru import logger
from secondary_functions import load_config, load_token
import time

from archive_extractor import ArchiveExtractor
from parsing_xml.okpd_parser import process_okpd_files  # Импортируем функцию для проверки ОКПД
from file_delete.file_deleter import FileDeleter  # Импортируем класс FileDeleter


class FileDownloader:
    def __init__(self, config_path="config.ini"):
        """
        Инициализирует объект для скачивания файлов, загружает конфигурацию и токен.

        :param config_path: Путь к конфигурационному файлу (по умолчанию "config.ini").
        :raises ValueError: Если конфигурация или токен не могут быть загружены.
        """

        # Загружаем настройки из конфигурации
        self.config = load_config(config_path)
        if not self.config:
            raise ValueError("Ошибка загрузки конфигурации!")

        # Загружаем токен
        self.token = load_token(self.config)
        if not self.token:
            raise ValueError("Токен не найден! Проверьте .env файл.")

        # Создаем объект для разархивации
        self.archive_extractor = ArchiveExtractor(config_path)

        # Логируем успешную загрузку конфигурации и токена
        logger.info("Конфигурация и токен загружены успешно.")

    def download_files(self, urls, subsystem, region_code):
        """
        Скачивает файлы по переданному списку URL и сохраняет их в нужную папку в зависимости от типа документа.
        :param urls: Список URL для скачивания файлов.
        :param subsystem: Тип документа, который используется для определения пути сохранения файлов.
        :param region_code: Код региона из SOAP-запроса.
        :return: Путь, куда были сохранены архивы.
        :raises: Записывает ошибки в лог при проблемах с скачиванием.
        """
        path_mapping = {
            "PRIZ": "reest_new_contract_archive_44_fz_xml",
            "RGK": "recouped_contract_archive_44_fz_xml",
            "RI223": "reest_new_contract_archive_223_fz_xml",
            "RD223": "recouped_contract_archive_223_fz_xml",
        }

        # Проверяем, есть ли subsystem в словаре
        path_key = path_mapping.get(subsystem)
        if not path_key:
            logger.error(f"Не найден путь для типа документа: {subsystem}")
            return None

        # Получаем путь из config.ini
        save_path = self.config.get("path", path_key, fallback=None)
        if not save_path:
            logger.error(f"Путь не найден в конфигурации для {path_key}")
            return None

        logger.info(f"Файлы будут сохранены в: {save_path}")

        # Создаём FileDeleter с уже известным save_path
        file_deleter = FileDeleter(save_path)

        # Перебираем все URL в списке
        for url in urls:
            try:
                # Разбираем URL для получения имени файла
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path) or f"file_{uuid.uuid4().hex[:8]}.zip"
                file_path = os.path.join(save_path, filename)

                logger.info(f"Скачивание {url} в {file_path}...")

                # Устанавливаем заголовки для запроса
                headers = {'individualPerson_token': self.token}

                # Отправляем GET-запрос для скачивания файла
                response = requests.get(url, stream=True, headers=headers, timeout=120)
                response.raise_for_status()  # Проверка на успешность запроса

                # Записываем скачанный файл на диск
                with open(file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)

                logger.info(f"Файл сохранен: {file_path}")

                # После скачивания сразу разархивируем файл
                self.archive_extractor.unzip_files(save_path)

                time.sleep(5)  # 1 секунда задержки (можно настроить по необходимости)

                # Удаляем файл после обработки
                file_deleter.delete_single_file(file_path)
                # logger.info(f'Файл {file_path} удален')

                # Путь к разархивированным файлам
                extracted_folder_path = save_path  # Папка с разархивированными файлами

                # Проверяем файлы на ОКПД и удаляем, если они не в базе
                okpd_results = process_okpd_files(extracted_folder_path, region_code)
                logger.info(f"Обработка файлов в папке {extracted_folder_path} завершена.")

            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка при скачивании {url}: {e}")

        # Возвращаем путь, в который были сохранены архивы
        return save_path