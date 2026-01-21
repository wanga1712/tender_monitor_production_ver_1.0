from datetime import datetime, timezone
import json
import time
from pathlib import Path
from typing import Optional

import requests

from utils.logger_config import get_logger
from utils.progress import ProgressManager
from utils import XMLParser
from utils import stats as stats_collector
from secondary_functions import load_token, load_config
from database_work.database_requests import get_region_codes
from file_downloader import FileDownloader

logger = get_logger()

# Путь для отладочных логов (NDJSON) – используется для диагностики сети/SOAP
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
            "runId": "soap-debug",
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


class EISRequester:
    def __init__(self, config_path: str = "config.ini", date: Optional[str] = None):
        self.config = load_config(config_path)
        if not self.config:
            raise ValueError("Ошибка загрузки конфигурации!")

        self.url = "http://localhost:8080/eis-integration/services/getDocsIP"
        self.token = load_token(self.config)
        # Если дата передана напрямую, используем её, иначе читаем из конфига
        if date:
            self.date = date
        else:
            self.date = self.config.get("eis", "date")
        self.regions = get_region_codes()
        self.subsystems_44 = [s.strip() for s in self.config.get("eis", "subsystems_44").split(",")]
        # Используем правильные ключи из конфига (с заглавной буквы или без - проверяем оба варианта)
        try:
            self.documentType44_PRIZ = [doc.strip() for doc in self.config.get("eis", "documentType44_PRIZ").split(",")]
        except:
            self.documentType44_PRIZ = [doc.strip() for doc in self.config.get("eis", "documenttype44_priz").split(",")]
        try:
            self.documentType44_RGK = [doc.strip() for doc in self.config.get("eis", "documentType44_RGK").split(",")]
        except:
            self.documentType44_RGK = [doc.strip() for doc in self.config.get("eis", "documenttype44_rgk").split(",")]
        self.subsystems_223 = [s.strip() for s in self.config.get("eis", "subsystems_223").split(",")]
        try:
            self.documentType223_RI223 = [doc.strip() for doc in self.config.get("eis", "documentType223_RI223").split(",")]
        except:
            self.documentType223_RI223 = [doc.strip() for doc in self.config.get("eis", "documenttype223_ri223").split(",")]
        try:
            self.documentType223_RD223 = [doc.strip() for doc in self.config.get("eis", "documentType223_RD223").split(",")]
        except:
            self.documentType223_RD223 = [doc.strip() for doc in self.config.get("eis", "documenttype223_rd223").split(",")]
        
        self.xml_parser = XMLParser()
        self.file_downloader = FileDownloader()
        self.progress_manager: Optional[ProgressManager] = None

    def get_current_time_utc(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def generate_soap_request(self, region_code: int, subsystem: str, document_type: str) -> str:
        import uuid
        # Генерация уникального идентификатора для запроса
        id_value = str(uuid.uuid4())
        # Получаем текущее время в формате UTC
        current_time = self.get_current_time_utc()

        # Формируем SOAP-запрос в формате XML (оригинальный формат)
        soap_request = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:ws="http://zakupki.gov.ru/fz44/get-docs-ip/ws">
            <soapenv:Header>
                <individualPerson_token>{self.token}</individualPerson_token>
            </soapenv:Header>
            <soapenv:Body>
                <ws:getDocsByOrgRegionRequest>
                    <index>
                        <id>{id_value}</id>
                        <createDateTime>{current_time}</createDateTime>
                        <mode>PROD</mode>
                    </index>
                    <selectionParams>
                        <orgRegion>{region_code}</orgRegion>
                        <subsystemType>{subsystem}</subsystemType>
                        <documentType44>{document_type}</documentType44>
                        <periodInfo>
                            <exactDate>{self.date}</exactDate>
                        </periodInfo>
                    </selectionParams>
                </ws:getDocsByOrgRegionRequest>
            </soapenv:Body>
        </soapenv:Envelope>
        """
        return soap_request

    def send_soap_request(self, soap_request: str, region_code: int, document_type: str, subsystem: str) -> str:
        """
        Отправляет SOAP-запрос с повторными попытками при ошибках подключения.
        При ошибке подключения повторяет попытку с увеличивающейся паузой: 5, 10, 15... до 60 минут, потом цикл заново.
        """
        headers = {
            "Content-Type": "text/xml",
            "Authorization": f"Bearer {self.token}"
        }
        
        # Начальная пауза и максимальная пауза
        current_pause = 5 * 60  # 5 минут в секундах
        max_pause = 60 * 60  # 60 минут в секундах
        attempt = 0
        
        while True:
            try:
                attempt += 1
                debug_log(
                    "SOAP1",
                    "eis_requester.py:send_soap_request",
                    "Отправка SOAP-запроса",
                    {
                        "attempt": attempt,
                        "region_code": region_code,
                        "subsystem": subsystem,
                        "document_type": document_type,
                        "url": self.url,
                    },
                )

                response = requests.post(self.url, data=soap_request.encode("utf-8"), headers=headers, verify=False)
                status_code = response.status_code
                debug_log(
                    "SOAP2",
                    "eis_requester.py:send_soap_request",
                    "Ответ от прокси",
                    {
                        "attempt": attempt,
                        "region_code": region_code,
                        "subsystem": subsystem,
                        "document_type": document_type,
                        "url": self.url,
                        "status_code": status_code,
                    },
                )
                response.raise_for_status()
                return response.text
            except requests.exceptions.ConnectionError as e:
                error_msg = f"Ошибка подключения (регион {region_code}, {subsystem}, {document_type}): {e}"
                logger.error(error_msg)
                debug_log(
                    "SOAP3",
                    "eis_requester.py:send_soap_request",
                    "Ошибка подключения к прокси",
                    {
                        "attempt": attempt,
                        "region_code": region_code,
                        "subsystem": subsystem,
                        "document_type": document_type,
                        "url": self.url,
                        "error": str(e),
                        "current_pause_seconds": current_pause,
                    },
                )
                
                # Выводим информацию о попытке переподключения
                pause_minutes = current_pause // 60
                print(f"\n⚠️  Ошибка подключения | Попытка {attempt} | Пауза {pause_minutes} мин | Переподключение...")
                
                # Ждем перед следующей попыткой
                time.sleep(current_pause)
                
                # Увеличиваем паузу на 5 минут, но не больше 60 минут
                current_pause = min(current_pause + 5 * 60, max_pause)
                
                # Если достигли максимума, сбрасываем на 5 минут
                if current_pause >= max_pause:
                    current_pause = 5 * 60
                    print(f"🔄 Цикл пауз сброшен, начинаем с 5 минут")
                
                # Продолжаем цикл для повторной попытки
                continue
            except requests.exceptions.RequestException as e:
                # Для других ошибок (не подключение) просто пробрасываем исключение
                error_msg = f"Ошибка при выполнении SOAP-запроса (регион {region_code}, подсистема {subsystem}, документ {document_type}): {e}"
                logger.error(error_msg, exc_info=True)
                status = getattr(getattr(e, "response", None), "status_code", None)
                debug_log(
                    "SOAP4",
                    "eis_requester.py:send_soap_request",
                    "Ошибка HTTP при выполнении SOAP-запроса",
                    {
                        "attempt": attempt,
                        "region_code": region_code,
                        "subsystem": subsystem,
                        "document_type": document_type,
                        "url": self.url,
                        "error": str(e),
                        "status_code": status,
                    },
                )
                raise

    def process_requests(self, processed_regions=None, on_region_processed=None):
        """
        Обрабатывает запросы к ЕИС для всех регионов.
        
        :param processed_regions: Множество кодов регионов, которые уже обработаны (будут пропущены)
        :param on_region_processed: Callback функция, вызываемая после обработки каждого региона (region_code)
        """
        if processed_regions is None:
            processed_regions = set()
        
        self.progress_manager = ProgressManager()
        self.progress_manager.start()
        
        try:
            # Фильтруем регионы, исключая уже обработанные
            regions_to_process = [r for r in self.regions if r not in processed_regions]
            
            if not regions_to_process:
                logger.info(f"Все регионы для даты {self.date} уже обработаны, пропускаем")
                return
            
            if processed_regions:
                logger.info(f"Пропущено уже обработанных регионов: {len(processed_regions)}, осталось обработать: {len(regions_to_process)}")
                print(f"ℹ️  Пропущено уже обработанных регионов: {len(processed_regions)}, осталось обработать: {len(regions_to_process)}")
            
            total_requests = 0
            for region_code in regions_to_process:
                for subsystem in self.subsystems_44:
                    if subsystem == "PRIZ":
                        total_requests += len(self.documentType44_PRIZ)
                    elif subsystem == "RGK":
                        total_requests += len(self.documentType44_RGK)
                for subsystem in self.subsystems_223:
                    if subsystem == "RI223":
                        total_requests += len(self.documentType223_RI223)
                    elif subsystem == "RD223":
                        total_requests += len(self.documentType223_RD223)
            
            # Единый прогресс-бар для всех регионов
            self.progress_manager.add_task("regions", f"🌍 Регионы", total=len(regions_to_process))
            self.progress_manager.add_task("requests", f"📡 Запросы к ЕИС", total=total_requests)
            self.progress_manager.add_task("download_all", f"⬇️ Скачивание архивов", total=None)
            self.progress_manager.add_task("process_all", f"⚙️ Обработка файлов", total=None)
            
            for region_idx, region_code in enumerate(regions_to_process, 1):
                # Обновляем прогресс регионов
                self.progress_manager.update_task("regions", advance=1)
                self.progress_manager.set_description("regions", f"🌍 Регионы | {region_idx}/{len(self.regions)}")
                
                # Снимок статистики ДО обработки региона
                stats_before = stats_collector.get_snapshot()
                downloaded_archives = 0  # Счетчик скачанных архивов для региона
                had_download_errors = False  # Были ли ошибки скачивания архивов в этом регионе
                
                for subsystem in self.subsystems_44:
                    document_types = []
                    if subsystem == "PRIZ":
                        document_types = self.documentType44_PRIZ
                    elif subsystem == "RGK":
                        document_types = self.documentType44_RGK
                    
                    # Обновляем описание только при смене подсистемы
                    self.progress_manager.set_description("requests", f"📡 Запросы к ЕИС | Регион {region_code} | {subsystem}")
                    
                    for doc_type in document_types:
                        # НЕ переходим к следующему запросу пока не обработаем текущий
                        # send_soap_request сам будет повторять попытки при ошибках подключения
                        self.progress_manager.update_task("requests", advance=1)
                        
                        soap_request = self.generate_soap_request(region_code, subsystem, doc_type)
                        # send_soap_request будет повторять попытки при ошибках подключения до успеха
                        response_xml = self.send_soap_request(soap_request, region_code, doc_type, subsystem)
                        archive_urls = self.xml_parser.extract_archive_urls(response_xml)
                        
                        debug_log(
                            "SOAP4",
                            "eis_requester.py:process_requests",
                            "Извлечение archiveUrl из SOAP-ответа (44-ФЗ)",
                            {
                                "region_code": region_code,
                                "subsystem": subsystem,
                                "document_type": doc_type,
                                "archive_urls_count": len(archive_urls) if archive_urls else 0,
                                "archive_urls": archive_urls[:3] if archive_urls else [],  # Первые 3 для примера
                                "response_xml_length": len(response_xml) if response_xml else 0,
                            },
                        )
                        
                        if archive_urls:
                            downloaded_archives += len(archive_urls)
                            # Скачиваем и сразу обрабатываем
                            try:
                                self.file_downloader.download_files(
                                    archive_urls,
                                    subsystem,
                                    region_code,
                                    self.progress_manager,
                                )
                            except RuntimeError as download_error:
                                # Критическая ошибка скачивания архивов – помечаем регион как проблемный,
                                # НЕ считаем его успешно обработанным и переходим к следующему региону.
                                had_download_errors = True
                                logger.error(
                                    "Критическая ошибка при скачивании архивов "
                                    f"(регион {region_code}, подсистема {subsystem}): {download_error}"
                                )
                                debug_log(
                                    "SOAP5",
                                    "eis_requester.py:process_requests",
                                    "Ошибка скачивания архивов для региона (44-ФЗ)",
                                    {
                                        "region_code": region_code,
                                        "subsystem": subsystem,
                                        "document_type": doc_type,
                                        "error": str(download_error),
                                    },
                                )
                                break
                        
                        if had_download_errors:
                            break

                        time.sleep(0.5)

                    if had_download_errors:
                        break
                
                if had_download_errors:
                    # Переходим к следующему региону, НЕ фиксируя этот регион как успешно обработанный
                    continue
                
                for subsystem in self.subsystems_223:
                    document_types = []
                    if subsystem == "RI223":
                        document_types = self.documentType223_RI223
                    elif subsystem == "RD223":
                        document_types = self.documentType223_RD223
                    
                    # Обновляем описание только при смене подсистемы
                    self.progress_manager.set_description("requests", f"📡 Запросы к ЕИС | Регион {region_code} | {subsystem}")
                    
                    for doc_type in document_types:
                        # НЕ переходим к следующему запросу пока не обработаем текущий
                        # send_soap_request сам будет повторять попытки при ошибках подключения
                        self.progress_manager.update_task("requests", advance=1)
                        
                        soap_request = self.generate_soap_request(region_code, subsystem, doc_type)
                        # send_soap_request будет повторять попытки при ошибках подключения до успеха
                        response_xml = self.send_soap_request(soap_request, region_code, doc_type, subsystem)
                        archive_urls = self.xml_parser.extract_archive_urls(response_xml)
                        
                        debug_log(
                            "SOAP4",
                            "eis_requester.py:process_requests",
                            "Извлечение archiveUrl из SOAP-ответа (223-ФЗ)",
                            {
                                "region_code": region_code,
                                "subsystem": subsystem,
                                "document_type": doc_type,
                                "archive_urls_count": len(archive_urls) if archive_urls else 0,
                                "archive_urls": archive_urls[:3] if archive_urls else [],  # Первые 3 для примера
                                "response_xml_length": len(response_xml) if response_xml else 0,
                            },
                        )
                        
                        if archive_urls:
                            downloaded_archives += len(archive_urls)
                            # Скачиваем и сразу обрабатываем
                            try:
                                self.file_downloader.download_files(
                                    archive_urls,
                                    subsystem,
                                    region_code,
                                    self.progress_manager,
                                )
                            except RuntimeError as download_error:
                                had_download_errors = True
                                logger.error(
                                    "Критическая ошибка при скачивании архивов "
                                    f"(регион {region_code}, подсистема {subsystem}): {download_error}"
                                )
                                debug_log(
                                    "SOAP6",
                                    "eis_requester.py:process_requests",
                                    "Ошибка скачивания архивов для региона (223-ФЗ)",
                                    {
                                        "region_code": region_code,
                                        "subsystem": subsystem,
                                        "document_type": doc_type,
                                        "error": str(download_error),
                                    },
                                )
                                break
                        
                        if had_download_errors:
                            break

                        time.sleep(0.5)

                    if had_download_errors:
                        break
                
                if had_download_errors:
                    # Переходим к следующему региону, НЕ фиксируя этот регион как успешно обработанный
                    continue
                
                # Снимок статистики ПОСЛЕ обработки региона
                stats_after = stats_collector.get_snapshot()
                
                # Вычисляем дельту (что добавилось за этот регион)
                region_stats = {}
                for key in stats_after:
                    before_value = stats_before.get(key, 0)
                    after_value = stats_after.get(key, 0)
                    delta = after_value - before_value
                    if delta > 0:
                        region_stats[key] = delta
                
                # Выводим статистику по региону
                if downloaded_archives > 0 or region_stats:
                    parts = []
                    if downloaded_archives > 0:
                        parts.append(f"📥 Скачано архивов: {downloaded_archives}")
                    if region_stats:
                        db_parts = []
                        # Маппинг ключей на русские названия
                        ru_labels = {
                            'customer': 'Заказчиков',
                            'contractor': 'Подрядчиков',
                            'reestr_contract_44_fz': 'Торгов 44-ФЗ',
                            'reestr_contract_223_fz': 'Торгов 223-ФЗ',
                            'links_documentation_44_fz': 'Ссылок 44-ФЗ',
                            'links_documentation_223_fz': 'Ссылок 223-ФЗ',
                            'trading_platform': 'Торговых площадок',
                        }
                        for key, value in region_stats.items():
                            label = ru_labels.get(key, key)
                            db_parts.append(f"{label}: {value}")
                        if db_parts:
                            parts.append(f"💾 В БД: {', '.join(db_parts)}")
                    
                    if parts:
                        print(f"\r{' '*100}\r✅ Регион {region_code} ({region_idx}/{len(regions_to_process)}): {' | '.join(parts)}", flush=True)
                
                # Сохраняем прогресс обработки региона
                if on_region_processed:
                    try:
                        on_region_processed(region_code)
                    except Exception as e:
                        logger.error(f"Ошибка при сохранении прогресса региона {region_code}: {e}", exc_info=True)
        finally:
            self.progress_manager.stop()
