import os
import sys
import time
import json
import configparser
from datetime import datetime, timedelta
import os as _os_env  # локальный импорт для управления переменными окружения
from typing import Optional
from pathlib import Path

from config import CONFIG_INI_PATH, PROCESSED_DATES_FILE, REGION_PROGRESS_FILE
from orchestration.monitoring_service import MonitoringConfig, TenderMonitorService

# Импортируем настроенный logger (только ошибки в файл)
from utils.logger_config import get_logger
from utils.progress import ProgressManager
from utils import stats as stats_collector
from utils.memory_guard import check_memory_and_exit_if_needed
from proxy_runner import ProxyRunner
from eis_requester import EISRequester
# ВРЕМЕННО отключаем миграцию завершённых контрактов, чтобы не блокировать основной мониторинг.
# from database_work.contracts_migration import migrate_completed_contracts, check_tables_exist


# Получаем logger
logger = get_logger()

# Пути к файлам из централизованной конфигурации
CONFIG_PATH: Path = CONFIG_INI_PATH

# По умолчанию используем Rich, если доступен, иначе простой режим с визуальной полоской
# Можно переопределить внешне: PROGRESS_MODE=simple или PROGRESS_MODE=rich
if not _os_env.getenv("PROGRESS_MODE"):
    # Пытаемся использовать Rich, если доступен
    try:
        import rich
        _os_env.environ["PROGRESS_MODE"] = "rich"
    except ImportError:
        _os_env.environ["PROGRESS_MODE"] = "simple"

START_DATE = datetime(2024, 1, 11)  # Начальная дата
TODAY = datetime.today()  # Текущая дата

# Настройки мониторинга
MONITORING_INTERVAL = 30 * 60  # Интервал проверки в секундах (30 минут)
EIS_DATA_UPLOAD_TIME = 2  # Время загрузки данных в ЕИС (2:00 ночи)

def load_processed_dates():
    """Загружает список уже обработанных дат из JSON-файла."""
    if PROCESSED_DATES_FILE.exists():
        with PROCESSED_DATES_FILE.open("r", encoding="utf-8") as file:
            return set(json.load(file))  # Храним даты в виде множества
    return set()

def save_processed_date(date_str):
    """Сохраняет отработанную дату в JSON-файл."""
    processed_dates = load_processed_dates()
    processed_dates.add(date_str)

    with PROCESSED_DATES_FILE.open("w", encoding="utf-8") as file:
        json.dump(list(processed_dates), file, indent=4)

def load_region_progress():
    """Загружает прогресс обработки регионов по датам из JSON-файла."""
    if REGION_PROGRESS_FILE.exists():
        try:
            with REGION_PROGRESS_FILE.open("r", encoding="utf-8") as file:
                return json.load(file)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_region_progress(progress_data):
    """Сохраняет прогресс обработки регионов по датам в JSON-файл."""
    with REGION_PROGRESS_FILE.open("w", encoding="utf-8") as file:
        json.dump(progress_data, file, indent=4, ensure_ascii=False)

def mark_region_processed(date_str, region_code):
    """Отмечает регион как обработанный для указанной даты."""
    progress = load_region_progress()
    if date_str not in progress:
        progress[date_str] = {"processed_regions": []}
    if region_code not in progress[date_str]["processed_regions"]:
        progress[date_str]["processed_regions"].append(region_code)
    save_region_progress(progress)

def get_processed_regions_for_date(date_str):
    """Возвращает список обработанных регионов для указанной даты."""
    progress = load_region_progress()
    if date_str in progress:
        return set(progress[date_str].get("processed_regions", []))
    return set()

def clear_region_progress_for_date(date_str):
    """Очищает прогресс обработки регионов для указанной даты (после успешного завершения)."""
    progress = load_region_progress()
    if date_str in progress:
        del progress[date_str]
        save_region_progress(progress)

def get_current_date():
    """Читает текущую дату из config.ini, исправлена проблема с кодировкой."""
    config = configparser.ConfigParser()
    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        config.read_file(file)  # Читаем файл с явной кодировкой UTF-8

    return datetime.strptime(config.get("eis", "date", fallback=START_DATE.strftime("%Y-%m-%d")), "%Y-%m-%d")


def update_config_date(new_date):
    """Обновляет дату в config.ini с явной кодировкой UTF-8."""
    config = configparser.ConfigParser()

    # Читаем файл с правильной кодировкой
    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        config.read_file(file)

    config.set("eis", "date", new_date.strftime("%Y-%m-%d"))

    # Записываем обратно в файл с нужной кодировкой
    with CONFIG_PATH.open("w", encoding="utf-8") as config_file:
        config.write(config_file)


def check_data_available(date_str: str) -> bool:
    """
    Проверяет наличие данных для указанной даты в ЕИС.
    Делает легкий запрос к ЕИС для проверки доступности данных.
    
    :param date_str: Дата в формате YYYY-MM-DD
    :return: True если данные доступны, False если нет
    """
    try:
        from eis_requester import EISRequester
        from database_work.database_requests import get_region_codes
        
        # Создаем EISRequester для проверки
        eis_requester = EISRequester(date=date_str)
        
        # Получаем первый регион для тестового запроса
        regions = get_region_codes()
        if not regions:
            return False
        
        # Делаем тестовый запрос к первому региону и первой подсистеме
        test_region = regions[0]
        test_subsystem = eis_requester.subsystems_44[0] if eis_requester.subsystems_44 else None
        
        if not test_subsystem:
            return False
        
        # Генерируем тестовый SOAP запрос
        if test_subsystem == "PRIZ":
            test_doc_type = eis_requester.documentType44_PRIZ[0] if eis_requester.documentType44_PRIZ else None
        elif test_subsystem == "RGK":
            test_doc_type = eis_requester.documentType44_RGK[0] if eis_requester.documentType44_RGK else None
        else:
            return False
        
        if not test_doc_type:
            return False
        
        # Отправляем тестовый запрос
        soap_request = eis_requester.generate_soap_request(test_region, test_subsystem, test_doc_type)
        response = eis_requester.send_soap_request(soap_request, test_region, test_doc_type, test_subsystem)
        
        # Если получили ответ и в нем есть данные (не пустой ответ или ошибка)
        if response and len(response) > 100:  # Минимальный размер ответа с данными
            # Проверяем, что это не ошибка
            if "error" not in response.lower() and "exception" not in response.lower():
                return True
        
        return False
    except Exception as e:
        logger.debug(f"Ошибка при проверке наличия данных для {date_str}: {e}")
        return False


def monitor_for_new_data(target_date: datetime):
    """
    Мониторит появление новых данных для указанной даты.
    Периодически проверяет наличие данных и обрабатывает их при появлении.
    
    :param target_date: Дата для мониторинга
    """
    date_str = target_date.strftime("%Y-%m-%d")
    check_count = 0
    
    print(f"\n{'='*60}")
    print(f"🔍 РЕЖИМ МОНИТОРИНГА: ожидание данных за {date_str}")
    print(f"{'='*60}")
    print(f"ℹ️  Данные в ЕИС загружаются в {EIS_DATA_UPLOAD_TIME}:00 ночи за предыдущий день")
    print(f"ℹ️  Проверка наличия данных каждые {MONITORING_INTERVAL // 60} минут")
    print(f"{'='*60}\n")
    logger.info(f"Включен режим мониторинга для даты {date_str}")
    
    while True:
        try:
            check_count += 1
            current_time = datetime.now()
            
            print(f"\n[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] Проверка #{check_count} наличия данных за {date_str}...")
            logger.info(f"Проверка #{check_count} наличия данных за {date_str}")
            
            # Проверяем наличие данных
            if check_data_available(date_str):
                print(f"✅ Данные за {date_str} обнаружены! Начинаем обработку...")
                logger.info(f"Данные за {date_str} обнаружены, начинаем обработку")
                
                # Обрабатываем дату
                return True  # Возвращаем True, чтобы обработать дату
            else:
                print(f"⏳ Данные за {date_str} еще не загружены. Следующая проверка через {MONITORING_INTERVAL // 60} минут...")
                logger.debug(f"Данные за {date_str} еще не доступны, ожидание...")
            
            # ВРЕМЕННО: отключаем плановую миграцию завершённых контрактов, чтобы не мешать мониторингу.
            # if check_count % 48 == 0:
            #     logger.info("Выполнение плановой миграции завершенных контрактов...")
            #     print("\n🔄 Плановая миграция завершенных контрактов (таймаут 60 сек)...")
            #     try:
            #         import threading
            #         migration_result = {"completed": False, "error": None}
            #         
            #         def run_migration():
            #             try:
            #                 migrate_completed_contracts()
            #                 migration_result["completed"] = True
            #             except Exception as e:
            #                 migration_result["error"] = e
            #         
            #         migration_thread = threading.Thread(target=run_migration, daemon=True)
            #         migration_thread.start()
            #         migration_thread.join(timeout=60)  # Таймаут 60 секунд
            #         
            #         if migration_thread.is_alive():
            #             logger.warning("Плановая миграция превысила таймаут 60 секунд")
            #             print("⚠️  Миграция превысила таймаут, продолжаем мониторинг...")
            #         elif migration_result["error"]:
            #             logger.error(f"Ошибка при плановой миграции: {migration_result['error']}")
            #         elif migration_result["completed"]:
            #             print("✅ Плановая миграция завершена")
            #     except Exception as e:
            #         logger.error(f"Ошибка при плановой миграции: {e}", exc_info=True)
            #         print(f"⚠️  Ошибка при миграции: {e}")
            
            # Ждем перед следующей проверкой
            time.sleep(MONITORING_INTERVAL)
            
        except KeyboardInterrupt:
            print("\n⚠️  Мониторинг прерван пользователем")
            logger.info("Мониторинг прерван пользователем")
            raise
        except Exception as e:
            logger.error(f"Ошибка в режиме мониторинга: {e}", exc_info=True)
            print(f"⚠️  Ошибка при мониторинге: {e}")
            print(f"   Продолжаем мониторинг через {MONITORING_INTERVAL // 60} минут...")
            time.sleep(MONITORING_INTERVAL)


if __name__ == "__main__":
    try:
        print("🚀 Запуск программы TenderMonitor...")
        
        # Запуск прокси (stunnel на Windows и Linux)
        print("📡 Проверка прокси-соединения...")
        try:
            proxy_runner = ProxyRunner()
            proxy_runner.run_proxy()
            # На обеих платформах используем stunnel, nginx не используется
            print("✅ Stunnel успешно настроен")
        except RuntimeError as proxy_error:
            error_msg = str(proxy_error)
            logger.critical(f"Ошибка при настройке прокси: {error_msg}", exc_info=True)
            print(f"\n{'='*60}")
            print(f"❌ ОШИБКА ПРИ НАСТРОЙКЕ ПРОКСИ")
            print(f"{'='*60}")
            print(error_msg)
            print(f"{'='*60}")
            print("⚠️  Программа завершена из-за ошибки прокси.")
            print("   Проверьте конфигурацию stunnel на сервере.")
            print(f"{'='*60}\n")
            sys.exit(1)
        except Exception as proxy_error:
            error_msg = f"Неожиданная ошибка при настройке прокси: {proxy_error}"
            logger.critical(error_msg, exc_info=True)
            print(f"\n{'='*60}")
            print(f"❌ ОШИБКА ПРИ НАСТРОЙКЕ ПРОКСИ")
            print(f"{'='*60}")
            print(error_msg)
            print(f"{'='*60}\n")
            sys.exit(1)
        
        # Проверка подключения к БД перед началом работы
        print("🔍 Проверка подключения к БД...")
        try:
            from database_work.database_requests import get_region_codes
            
            # ВРЕМЕННО: только проверяем подключение к БД, без запуска миграций.
            test_regions = get_region_codes()
            print(f"✅ Подключение к БД успешно (найдено регионов: {len(test_regions)})")
        except Exception as db_test_error:
            from utils.exceptions import DatabaseError
            import psycopg2
            
            is_db_error = (
                isinstance(db_test_error, DatabaseError) or
                isinstance(db_test_error, psycopg2.Error) or
                (hasattr(db_test_error, '__cause__') and isinstance(db_test_error.__cause__, (DatabaseError, psycopg2.Error)))
            )
            
            if is_db_error:
                error_msg = f"❌ ОШИБКА ПОДКЛЮЧЕНИЯ К БД: {db_test_error}"
                logger.critical(error_msg, exc_info=True)
                print(f"\n{'='*60}")
                print(error_msg)
                print(f"{'='*60}")
                print("⚠️  Программа завершена из-за ошибки подключения к БД.")
                print("   Пожалуйста, проверьте:")
                print("   - Запущена ли база данных")
                print("   - Правильность настроек подключения в database_work/db_credintials.env")
                print("   - Доступность БД по указанному адресу и порту")
                print(f"{'='*60}\n")
                sys.exit(1)
            else:
                raise
        
        # Читаем начальную дату из конфигурации (ИСХОДНАЯ дата пользователя)
        initial_date = get_current_date()
        logger.info(
            f"Начальная дата из config.ini: {initial_date.strftime('%Y-%m-%d')}"
        )

        monitoring_config = MonitoringConfig(
            start_date=initial_date,
            today=datetime.today(),
            monitoring_interval_seconds=MONITORING_INTERVAL,
            eis_data_upload_hour=EIS_DATA_UPLOAD_TIME,
        )

        def create_eis_requester_for_date(date_str: str) -> EISRequester:
            return EISRequester(date=date_str)

        def on_memory_check(context: str) -> None:
            check_memory_and_exit_if_needed(
                logger=logger,
                grace_sleep_seconds=5,
                context=context,
            )

        service = TenderMonitorService(
            config=monitoring_config,
            logger=logger,
            check_data_available=check_data_available,
            monitor_for_new_data=monitor_for_new_data,
            get_processed_regions_for_date=get_processed_regions_for_date,
            mark_region_processed=mark_region_processed,
            clear_region_progress_for_date=clear_region_progress_for_date,
            update_config_date=update_config_date,
            get_stats_snapshot=stats_collector.get_snapshot,
            create_eis_requester=create_eis_requester_for_date,
            on_memory_check=on_memory_check,
        )

        service.run()
        
    except KeyboardInterrupt:
        print("\n⚠️  Программа прервана пользователем")
        logger.error("Программа прервана пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        logger.error(f"Критическая ошибка в main.py: {e}", exc_info=True)
        raise
