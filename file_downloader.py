import os
import uuid
import json
import time
from pathlib import Path

import requests
from urllib.parse import urlparse, urlunparse
from typing import Optional

from utils.logger_config import get_logger
from utils.progress import ProgressManager
from secondary_functions import load_config, load_token
from archive_extractor import ArchiveExtractor
from parsing_xml.okpd_parser import process_okpd_files  # Импортируем функцию для проверки ОКПД
from file_delete.file_deleter import FileDeleter  # Импортируем класс FileDeleter

# Получаем logger (только ошибки в файл)
logger = get_logger()

# Путь для отладочных логов (NDJSON) – используется в debug mode
DEBUG_LOG_PATH = Path(__file__).resolve().parent / ".cursor" / "debug.log"


def debug_log(hypothesis_id: str, location: str, message: str, data: Optional[dict] = None) -> None:
    """
    Пишет отладочное сообщение в NDJSON-файл.
    Используется только для диагностики (не влияет на основную логику).
    """
    try:
        DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "sessionId": "debug-session",
            "runId": "network-debug",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        with DEBUG_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        # Никогда не ломаем основную логику из-за проблем с отладочными логами
        pass


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

        # Базовый URL прокси (stunnel/nginx), через который должны идти ВСЕ запросы к ЕИС
        # Для SOAP это уже http://localhost:8080/eis-integration/..., здесь используем тот же хост/порт.
        self.proxy_scheme = "http"
        self.proxy_netloc = "localhost:8080"

    def _build_proxy_url(self, original_url: str) -> str:
        """
        Преобразует исходный URL ЕИС (https://int.zakupki.gov.ru/... или https://int44.zakupki.gov.ru/...)
        в URL, проходящий через локальный прокси (http://localhost:8080/...).

        Путь и query сохраняются как есть, меняются только схема и host:port.
        НЕ меняем host (int.zakupki.gov.ru vs int44.zakupki.gov.ru) - используем тот, что пришёл от ЕИС.
        """
        parsed = urlparse(original_url)
        # Оставляем path и query нетронутыми, чтобы не поломать ticket/docRequestUid
        # Меняем только scheme и netloc на прокси
        proxied = parsed._replace(
            scheme=self.proxy_scheme,
            netloc=self.proxy_netloc,
        )
        return urlunparse(proxied)

    def download_files(self, urls, subsystem, region_code, progress_manager: Optional[ProgressManager] = None):
        """
        Скачивает файлы по переданному списку URL и сохраняет их в нужную папку в зависимости от типа документа.
        :param urls: Список URL для скачивания файлов.
        :param subsystem: Тип документа, который используется для определения пути сохранения файлов.
        :param region_code: Код региона из SOAP-запроса.
        :param progress_manager: Менеджер прогресс-баров для обновления прогресса.
        :return: Путь, куда были сохранены архивы.
        :raises: Записывает ошибки в лог при проблемах с скачиванием.
        """
        path_mapping = {
            "PRIZ": "reest_new_contract_archive_44_fz_xml",
            "RGK": "recouped_contract_archive_44_fz_xml",
            "RI223": "reest_new_contract_archive_223_fz_xml",
            "RD223": "recouped_contract_archive_223_fz_xml",
        }

        # Определяем тип ФЗ для описания
        fz_type = "44-ФЗ" if subsystem in ["PRIZ", "RGK"] else "223-ФЗ"
        # Имена задач для прогресс-баров (44-ФЗ -> download_44, 223-ФЗ -> download_223)
        if fz_type == "44-ФЗ":
            download_task_name = "download_44"
            process_task_name = "process_44"
        else:
            download_task_name = "download_223"
            process_task_name = "process_223"

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

        # Создаём FileDeleter с уже известным save_path
        file_deleter = FileDeleter(save_path)

        urls_count = len(urls)

        if not urls:
            debug_log(
                "FD1",
                "file_downloader.py:download_files",
                "Пустой список URL, скачивание пропущено",
                {
                    "subsystem": subsystem,
                    "region_code": region_code,
                    "fz_type": fz_type,
                },
            )
            return save_path

        # Обновляем единый прогресс-бар скачивания
        if progress_manager:
            progress_manager.update_task("download_all", advance=0)
            progress_manager.set_description("download_all", f"⬇️ Скачивание архивов | Регион {region_code} | {subsystem} | {fz_type}")
        logger.info(f"Начало скачивания {urls_count} архивов ({fz_type}, регион {region_code})")
        debug_log(
            "FD2",
            "file_downloader.py:download_files",
            "Начало скачивания архивов",
            {
                "urls_count": urls_count,
                "subsystem": subsystem,
                "region_code": region_code,
                "fz_type": fz_type,
            },
        )

        # Перебираем все URL в списке - скачиваем файлы
        downloaded_count = 0
        success_count = 0
        fail_count = 0
        start_time = time.time()

        for idx, url in enumerate(urls, start=1):
            try:
                # Строим URL через локальный прокси/stunnel (localhost:8080)
                proxy_url = self._build_proxy_url(url)

                # Разбираем ИСХОДНЫЙ URL для получения имени файла (чтобы имя было предсказуемым)
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path) or f"file_{uuid.uuid4().hex[:8]}.zip"
                file_path = os.path.join(save_path, filename)

                # Устанавливаем заголовки для запроса
                headers = {'individualPerson_token': self.token}

                debug_log(
                    "FD3",
                    "file_downloader.py:download_files",
                    "Запрос архива",
                    {
                        "index": idx,
                        "urls_count": urls_count,
                        "url": url,
                        "proxy_url": proxy_url,
                        "region_code": region_code,
                        "subsystem": subsystem,
                        "fz_type": fz_type,
                    },
                )

                # Отправляем GET-запрос для скачивания файла через локальный прокси/stunnel
                response = requests.get(proxy_url, stream=True, headers=headers, timeout=120)
                response.raise_for_status()  # Проверка на успешность запроса

                # Записываем скачанный файл на диск
                with open(file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)

                # После скачивания сразу разархивируем файл
                self.archive_extractor.unzip_files(save_path)

                # ВРЕМЕННО: архив НЕ удаляем, чтобы можно было анализировать его содержимое на сервере
                # file_deleter.delete_single_file(file_path)

                downloaded_count += 1
                success_count += 1

                debug_log(
                    "FD4",
                    "file_downloader.py:download_files",
                    "Архив успешно скачан и распакован",
                    {
                        "index": idx,
                        "urls_count": urls_count,
                        "url": url,
                        "region_code": region_code,
                        "subsystem": subsystem,
                        "fz_type": fz_type,
                        "status_code": response.status_code,
                    },
                )
                # Обновляем единый прогресс-бар скачивания
                if progress_manager:
                    progress_manager.update_task("download_all", advance=1)
                    progress_manager.set_description("download_all", f"⬇️ Скачивание архивов | Регион {region_code} | {subsystem} | {downloaded_count}/{len(urls)}")
                
                logger.info(f"Скачан и распакован архив {downloaded_count}/{len(urls)} ({fz_type}, регион {region_code})")

            except requests.exceptions.RequestException as e:
                fail_count += 1
                logger.error(f"Ошибка при скачивании {url}: {e}")
                debug_log(
                    "FD5",
                    "file_downloader.py:download_files",
                    "Ошибка HTTP при скачивании архива",
                    {
                        "index": idx,
                        "urls_count": urls_count,
                        "url": url,
                        "proxy_url": proxy_url,
                        "region_code": region_code,
                        "subsystem": subsystem,
                        "fz_type": fz_type,
                        "error": str(e),
                    },
                )
            except Exception as e:
                fail_count += 1
                logger.error(f"Неожиданная ошибка при скачивании файла {url}: {e}", exc_info=True)
                debug_log(
                    "FD6",
                    "file_downloader.py:download_files",
                    "Неожиданная ошибка при скачивании архива",
                    {
                        "index": idx,
                        "urls_count": urls_count,
                        "url": url,
                        "region_code": region_code,
                        "subsystem": subsystem,
                        "fz_type": fz_type,
                        "error": str(e),
                    },
                )

        elapsed = time.time() - start_time
        logger.info(f"Скачивание завершено: {downloaded_count}/{urls_count} архивов ({fz_type}, регион {region_code})")
        debug_log(
            "FD7",
            "file_downloader.py:download_files",
            "Скачивание завершено",
            {
                "urls_count": urls_count,
                "downloaded_count": downloaded_count,
                "success_count": success_count,
                "fail_count": fail_count,
                "region_code": region_code,
                "subsystem": subsystem,
                "fz_type": fz_type,
                "duration_seconds": round(elapsed, 2),
            },
        )

        # Если были ошибки скачивания, считаем, что дата/регион нельзя считать успешно обработанными
        # и НЕ имеем права "тихо" идти дальше.
        if fail_count > 0:
            error_message = (
                f"Не удалось скачать {fail_count} из {urls_count} архивов "
                f"({fz_type}, регион {region_code}, подсистема {subsystem})"
            )
            logger.error(error_message)
            debug_log(
                "FD8",
                "file_downloader.py:download_files",
                "Критическая ошибка скачивания архивов",
                {
                    "urls_count": urls_count,
                    "downloaded_count": downloaded_count,
                    "success_count": success_count,
                    "fail_count": fail_count,
                    "region_code": region_code,
                    "subsystem": subsystem,
                    "fz_type": fz_type,
                },
            )
            # Бросаем исключение, чтобы верхний уровень НЕ продолжал обработку как будто всё ок
            raise RuntimeError(error_message)
        
        # Обновляем описание задачи обработки
        if progress_manager:
            progress_manager.set_description("process_all", f"⚙️ Обработка файлов | Регион {region_code} | {subsystem} | {fz_type}")
        
        # После скачивания всех файлов обрабатываем данные
        logger.info(f"Начало обработки файлов ({fz_type}, регион {region_code})")
        process_okpd_files(save_path, region_code, progress_manager)
        logger.info(f"Обработка файлов завершена ({fz_type}, регион {region_code})")

        # Возвращаем путь, в который были сохранены архивы
        return save_path
    
    def download_files_only(self, urls, subsystem, region_code, progress_manager: Optional[ProgressManager] = None):
        """
        ТОЛЬКО скачивает и распаковывает файлы (без обработки).
        :param urls: Список URL для скачивания файлов.
        :param subsystem: Тип документа.
        :param region_code: Код региона.
        :param progress_manager: Менеджер прогресс-баров.
        :return: Словарь с информацией: {"path": save_path, "count": downloaded_count, "subsystem": subsystem, "region_code": region_code, "fz_type": fz_type}
        """
        path_mapping = {
            "PRIZ": "reest_new_contract_archive_44_fz_xml",
            "RGK": "recouped_contract_archive_44_fz_xml",
            "RI223": "reest_new_contract_archive_223_fz_xml",
            "RD223": "recouped_contract_archive_223_fz_xml",
        }

        fz_type = "44-ФЗ" if subsystem in ["PRIZ", "RGK"] else "223-ФЗ"
        path_key = path_mapping.get(subsystem)
        if not path_key:
            logger.error(f"Не найден путь для типа документа: {subsystem}")
            return {"path": None, "count": 0, "subsystem": subsystem, "region_code": region_code, "fz_type": fz_type}

        save_path = self.config.get("path", path_key, fallback=None)
        if not save_path:
            logger.error(f"Путь не найден в конфигурации для {path_key}")
            return {"path": None, "count": 0, "subsystem": subsystem, "region_code": region_code, "fz_type": fz_type}

        file_deleter = FileDeleter(save_path)

        if not urls:
            return {"path": save_path, "count": 0, "subsystem": subsystem, "region_code": region_code, "fz_type": fz_type}

        downloaded_count = 0
        for url in urls:
            try:
                # Строим URL через локальный прокси/stunnel (localhost:8080)
                proxy_url = self._build_proxy_url(url)

                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path) or f"file_{uuid.uuid4().hex[:8]}.zip"
                file_path = os.path.join(save_path, filename)

                headers = {'individualPerson_token': self.token}
                response = requests.get(proxy_url, stream=True, headers=headers, timeout=120)
                response.raise_for_status()

                with open(file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)

                # Распаковываем архив
                self.archive_extractor.unzip_files(save_path)
                # ВРЕМЕННО: архив НЕ удаляем, чтобы можно было анализировать его содержимое на сервере
                # file_deleter.delete_single_file(file_path)

                downloaded_count += 1
                if progress_manager:
                    progress_manager.update_task("download_all", advance=1)
                    progress_manager.set_description("download_all", f"⬇️ Скачивание архивов | Регион {region_code} | {subsystem} | {downloaded_count}")

                logger.info(f"Скачан и распакован архив {downloaded_count}/{len(urls)} ({fz_type}, регион {region_code})")

            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка при скачивании {url}: {e}")
            except Exception as e:
                logger.error(f"Неожиданная ошибка при скачивании файла {url}: {e}", exc_info=True)
        
        logger.info(f"Скачивание завершено: {downloaded_count}/{len(urls)} архивов ({fz_type}, регион {region_code})")
        
        return {"path": save_path, "count": downloaded_count, "subsystem": subsystem, "region_code": region_code, "fz_type": fz_type}