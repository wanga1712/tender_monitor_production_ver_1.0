import json
import configparser
import os
from dotenv import load_dotenv
import inspect
from loguru import logger


def load_config(config_path="config.ini"):
    """
    Загружает конфигурационный файл и возвращает объект ConfigParser.

    :param config_path: Путь к конфигурационному файлу (по умолчанию "config.ini").
    :return: Объект ConfigParser с загруженной конфигурацией.
    :raises: Ошибка, если не удалось загрузить конфигурацию.
    """
    config = configparser.ConfigParser()

    # Получаем путь к файлу конфигурации относительно местоположения самого модуля secondary_functions
    current_function_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

    # Формируем абсолютный путь к конфигурации относительно модуля secondary_functions
    config_path = os.path.join(current_function_path, config_path)

    try:
        # Читаем конфигурационный файл с указанным путём
        logger.info(f"Используем конфигурацию из: {config_path}")
        config.read(config_path, encoding="utf-8")
        return config
    except configparser.Error as e:
        logger.error(f"Ошибка загрузки {config_path}: {e}")
        raise


def check_file_exists(file_path, description):
    """
    Проверяет существование файла и логирует ошибку, если его нет.

    :param file_path: Путь к проверяемому файлу.
    :param description: Описание файла для вывода в лог.
    :return: True, если файл существует, иначе False.
    """
    if not os.path.exists(file_path):
        logger.error(f"Файл {description} не найден: {file_path}")
        return False
    return True


def load_regions(regions_file):
    """
    Загружает словарь регионов из JSON-файла.

    :param regions_file: Путь к файлу с регионами.
    :return: Словарь регионов или пустой словарь в случае ошибки.
    """
    # Проверяем, существует ли файл с регионами
    if not check_file_exists(regions_file, "регионов"):
        return {}

    try:
        # Загружаем регионы из JSON-файла
        with open(regions_file, "r", encoding="utf-8") as file:
            regions = json.load(file)
            logger.info(f"Загружено {len(regions)} регионов из {regions_file}")
            return regions
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка в JSON-файле {regions_file}: {e}")
    except Exception as e:
        logger.error(f"Неизвестная ошибка при загрузке регионов: {e}")

    return {}


def get_region_codes(regions_file):
    """
    Возвращает список кодов регионов.

    :param regions_file: Путь к файлу с регионами.
    :return: Список кодов регионов.
    """
    regions = load_regions(regions_file)
    # Если регионы загружены, возвращаем список кодов, иначе пустой список
    return [int(code) for code in regions.keys()] if regions else []


def load_token(config):
    """
    Загружает токен из .env файла, путь к которому хранится в config.ini.

    :param config: Объект конфигурации, в котором хранится путь к .env файлу.
    :return: Токен из .env файла или None, если токен не найден.
    """
    # Получаем путь к .env файлу из конфигурации
    env_path = config.get("path", "env_file", fallback=None)

    if not env_path:
        logger.error("Путь к .env файлу не найден в config.ini")
        return None

    env_path = os.path.normpath(env_path)  # Приводим путь к корректному формату

    # Проверяем существование .env файла
    if not check_file_exists(env_path, ".env"):
        return None

    # Загружаем .env файл
    load_dotenv(env_path)
    token = os.getenv("TOKEN")

    if token:
        logger.info("Токен загружен успешно.")
    else:
        logger.error("Токен не найден в .env файле.")

    return token


# --- Основной блок кода ---
if __name__ == "__main__":
    config = load_config()

    # Загружаем путь к файлу регионов
    regions_file = config.get("path", "reest_new_contract_archive_44_fz_xml", fallback=None)
    if not regions_file:
        logger.error("Путь к файлу регионов не указан в config.ini")
    else:
        logger.info(f"Файл регионов: {regions_file}")

    token = load_token(config)

    logger.debug("Программа завершила работу.")
