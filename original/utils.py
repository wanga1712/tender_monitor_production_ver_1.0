from loguru import logger
from lxml import etree
from xml.etree import ElementTree as ET


class XMLParser:
    @staticmethod
    def extract_archive_urls(xml_content):
        """
        Извлекает все URL-адреса архивов из XML-строки.

        :param xml_content: Строка, содержащая XML-документ.
        :return: Список URL-адресов архивов, найденных в XML. Если ошибка при парсинге — пустой список.
        :raises Exception: Если произошла ошибка при парсинге XML.
        """
        try:
            # Преобразуем строку XML в объект ElementTree, используя кодировку UTF-8
            root = ET.fromstring(xml_content.encode("utf-8"))

            # Используем lxml для поиска всех элементов <archiveUrl> в XML
            tree = etree.fromstring(xml_content.encode("utf-8"))
            # Извлекаем все URL-адреса архивов с помощью xpath
            urls = [url.text for url in tree.xpath("//archiveUrl")]

            return urls
        except etree.XMLSyntaxError as e:
            # Логируем ошибку, если произошла ошибка при парсинге XML
            logger.error(f"Ошибка при парсинге XML: {e}")
            return []  # Возвращаем пустой список в случае ошибки
        except Exception as e:
            # Логируем любую другую ошибку
            logger.error(f"Неизвестная ошибка: {e}")
            return []  # Возвращаем пустой список в случае ошибки

