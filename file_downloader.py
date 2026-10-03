import os
import shutil
import zipfile
import uuid
import requests
from datetime import date
from pathlib import Path
from urllib.parse import urlparse
from eis_download_fix import rewrite_eis_url_via_stunnel
from typing import Optional

from utils.logger_config import get_logger
from utils.progress import ProgressManager
from secondary_functions import load_config, load_token
import time

from archive_extractor import ArchiveExtractor
from parsing_xml.okpd_parser import process_okpd_files  # Импортируем функцию для проверки ОКПД
from file_delete.file_deleter import FileDeleter  # Импортируем класс FileDeleter
from runtime_safety import cleanup_files, ensure_download_allowed
from database_work.source_archive_lineage import persist_archive

# Получаем logger (только ошибки в файл)
logger = get_logger()


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
        self.ingestion_direction = os.getenv("TENDERMONITOR_DIRECTION", "forward").upper()
        self.source_date = date.fromisoformat(self.config.get("eis", "date"))
        self.raw_archive_root = Path(os.getenv("TENDERMONITOR_RAW_ARCHIVE_ROOT", "/opt/tendermonitor/raw_eis_archives"))

    def _retain_archive(self, file_path, subsystem):
        law_family = "44_FZ" if subsystem in ("PRIZ", "RGK") else "223_FZ"
        raw_path = self.raw_archive_root / self.ingestion_direction / law_family / self.source_date.isoformat() / Path(file_path).name
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(file_path), str(raw_path))
        persist_archive(raw_path, law_family, self.ingestion_direction, self.source_date, [], subsystem)
        return raw_path

    @staticmethod
    def _archive_xml_members(file_path):
        with zipfile.ZipFile(file_path) as archive:
            return [name for name in archive.namelist() if name.lower().endswith(".xml")]

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
            "PRIZ": ("path", "reest_new_contract_archive_44_fz_xml"),
            "RGK": ("path", "recouped_contract_archive_44_fz_xml"),
            "RI223": ("path", "reest_new_contract_archive_223_fz_xml"),
            "RD223": ("path", "recouped_contract_archive_223_fz_xml"),
            # 615_RD — legacy-ключ; 615_RD615 — после фикса subsystem=RD615
            "615_RD": ("eis_615", "archive_xml"),
            "615_RD615": ("eis_615", "archive_xml"),
            "615_PPRF615": ("eis_615", "archive_xml"),
        }

        # Определяем тип ФЗ для описания
        if subsystem in ["PRIZ", "RGK"]:
            fz_type = "44-ФЗ"
        elif subsystem in ("615_RD", "615_RD615", "615_PPRF615") or str(subsystem).startswith("615_"):
            fz_type = "615-ПП"
        else:
            fz_type = "223-ФЗ"

        # Проверяем, есть ли subsystem в словаре
        mapping = path_mapping.get(subsystem)
        if not mapping:
            logger.error(f"Не найден путь для типа документа: {subsystem}")
            return None

        section, path_key = mapping
        # Получаем путь из config.ini
        save_path = self.config.get(section, path_key, fallback=None)
        if not save_path:
            logger.error(f"Путь не найден в конфигурации для {path_key}")
            return None

        # Создаём FileDeleter с уже известным save_path
        file_deleter = FileDeleter(save_path)

        if not urls:
            return save_path

        # Обновляем единый прогресс-бар скачивания
        if progress_manager:
            progress_manager.update_task("download_all", advance=0)
            progress_manager.set_description("download_all", f"⬇️ Скачивание архивов | Регион {region_code} | {subsystem} | {fz_type}")

        logger.info(f"Начало скачивания {len(urls)} архивов ({fz_type}, регион {region_code})")

        # Перебираем все URL в списке - скачиваем файлы
        downloaded_count = 0
        created_xml_paths = []
        downloaded_archive_paths = []
        retained_archive_paths = []
        for url in urls:
            try:
                ensure_download_allowed(save_path, self.ingestion_direction)
                # Разбираем URL для получения имени файла
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path) or f"file_{uuid.uuid4().hex[:8]}.zip"
                if not filename.lower().endswith(".zip"):
                    filename = f"{filename}_{uuid.uuid4().hex[:8]}.zip"
                file_path = os.path.join(save_path, filename)

                # Устанавливаем заголовки для запроса
                headers = {'individualPerson_token': self.token}

                # Отправляем GET-запрос для скачивания файла
                download_url = rewrite_eis_url_via_stunnel(url)
                response = requests.get(download_url, stream=True, headers=headers, timeout=120, verify=False)
                response.raise_for_status()  # Проверка на успешность запроса

                # Записываем скачанный файл на диск
                with open(file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
                downloaded_archive_paths.append(file_path)

                # EIS can return an empty/truncated body with HTTP 200.  Do not
                # leave it in the shared directory: unzip_files() scans every
                # *.zip there, so one bad response would be logged repeatedly
                # for every subsequent download and counted as successful.
                if not zipfile.is_zipfile(file_path):
                    content_type = response.headers.get("Content-Type", "unknown")
                    content_length = os.path.getsize(file_path)
                    cleanup_files([file_path])
                    downloaded_archive_paths.remove(file_path)
                    logger.error(
                        "Ответ ЕИС не является ZIP: %s (status=%s, content_type=%s, bytes=%s)",
                        url,
                        response.status_code,
                        content_type,
                        content_length,
                    )
                    continue

                # Сохраняем исходный контейнер до extraction и parsing.
                raw_path = self._retain_archive(file_path, subsystem)
                retained_archive_paths.append(raw_path)
                archive_xml_members = self._archive_xml_members(file_path)
                extracted_before = {p.resolve() for p in Path(save_path).rglob("*.xml") if p.is_file()}
                ensure_download_allowed(save_path, self.ingestion_direction)
                self.archive_extractor.unzip_files(save_path)
                extracted_xml = [p for p in Path(save_path).rglob("*.xml") if p.is_file() and p.resolve() not in extracted_before]
                created_xml_paths.extend(extracted_xml)
                persist_archive(raw_path, "44_FZ" if subsystem in ("PRIZ", "RGK") else "223_FZ", self.ingestion_direction, self.source_date, archive_xml_members, subsystem)

                # Удаляем архив после распаковки
                file_deleter.delete_single_file(file_path)
                downloaded_archive_paths.remove(file_path)

                downloaded_count += 1
                # Обновляем единый прогресс-бар скачивания
                if progress_manager:
                    progress_manager.update_task("download_all", advance=1)
                    progress_manager.set_description("download_all", f"⬇️ Скачивание архивов | Регион {region_code} | {subsystem} | {downloaded_count}/{len(urls)}")

                logger.info(f"Скачан и распакован архив {downloaded_count}/{len(urls)} ({fz_type}, регион {region_code})")

            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка при скачивании {url}: {e}")
            except Exception as e:
                logger.error(f"Неожиданная ошибка при скачивании файла {url}: {e}", exc_info=True)

        logger.info(f"Скачивание завершено: {downloaded_count}/{len(urls)} архивов ({fz_type}, регион {region_code})")

        # Обновляем описание задачи обработки
        if progress_manager:
            progress_manager.set_description("process_all", f"⚙️ Обработка файлов | Регион {region_code} | {subsystem} | {fz_type}")

        # После скачивания всех файлов обрабатываем данные
        logger.info(f"Начало обработки файлов ({fz_type}, регион {region_code})")
        try:
            process_okpd_files(save_path, region_code, progress_manager)
        finally:
            cleanup_files(created_xml_paths + downloaded_archive_paths + retained_archive_paths)
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
        created_xml_paths = []
        downloaded_archive_paths = []
        retained_archive_paths = []
        for url in urls:
            try:
                ensure_download_allowed(save_path, self.ingestion_direction)
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path) or f"file_{uuid.uuid4().hex[:8]}.zip"
                if not filename.lower().endswith(".zip"):
                    filename = f"{filename}_{uuid.uuid4().hex[:8]}.zip"
                file_path = os.path.join(save_path, filename)

                headers = {'individualPerson_token': self.token}
                download_url = rewrite_eis_url_via_stunnel(url)
                response = requests.get(download_url, stream=True, headers=headers, timeout=120, verify=False)
                response.raise_for_status()

                with open(file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
                downloaded_archive_paths.append(file_path)

                if not zipfile.is_zipfile(file_path):
                    content_type = response.headers.get("Content-Type", "unknown")
                    content_length = os.path.getsize(file_path)
                    cleanup_files([file_path])
                    downloaded_archive_paths.remove(file_path)
                    logger.error(
                        "Ответ ЕИС не является ZIP: %s (status=%s, content_type=%s, bytes=%s)",
                        url,
                        response.status_code,
                        content_type,
                        content_length,
                    )
                    continue

                # Сохраняем исходный контейнер до extraction и parsing.
                raw_path = self._retain_archive(file_path, subsystem)
                retained_archive_paths.append(raw_path)
                archive_xml_members = self._archive_xml_members(file_path)
                extracted_before = {p.resolve() for p in Path(save_path).rglob("*.xml") if p.is_file()}
                ensure_download_allowed(save_path, self.ingestion_direction)
                self.archive_extractor.unzip_files(save_path)
                extracted_xml = [p for p in Path(save_path).rglob("*.xml") if p.is_file() and p.resolve() not in extracted_before]
                created_xml_paths.extend(extracted_xml)
                persist_archive(raw_path, "44_FZ" if subsystem in ("PRIZ", "RGK") else "223_FZ", self.ingestion_direction, self.source_date, archive_xml_members, subsystem)
                # Удаляем архив
                file_deleter.delete_single_file(file_path)
                downloaded_archive_paths.remove(file_path)

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
        cleanup_files(created_xml_paths + downloaded_archive_paths + retained_archive_paths)

        return {"path": save_path, "count": downloaded_count, "subsystem": subsystem, "region_code": region_code, "fz_type": fz_type}
