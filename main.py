import os
import sys
import time
import json
import configparser
from datetime import datetime, timedelta
import os as _os_env  # локальный импорт для управления переменными окружения

# Импортируем настроенный logger (только ошибки в файл)
from utils.logger_config import get_logger
from utils.progress import ProgressManager
from utils import stats as stats_collector
from proxy_runner import ProxyRunner
from eis_requester import EISRequester
from database_work.contracts_migration import migrate_completed_contracts, check_tables_exist

# Получаем logger
logger = get_logger()

# Пути к файлам
CONFIG_PATH = "config.ini"
PROCESSED_DATES_FILE = "processed_dates.json"
REGION_PROGRESS_FILE = "region_progress.json"

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
    if os.path.exists(PROCESSED_DATES_FILE):
        with open(PROCESSED_DATES_FILE, "r") as file:
            return set(json.load(file))  # Храним даты в виде множества
    return set()

def save_processed_date(date_str):
    """Сохраняет отработанную дату в JSON-файл."""
    processed_dates = load_processed_dates()
    processed_dates.add(date_str)

    with open(PROCESSED_DATES_FILE, "w") as file:
        json.dump(list(processed_dates), file, indent=4)

def load_region_progress():
    """Загружает прогресс обработки регионов по датам из JSON-файла."""
    if os.path.exists(REGION_PROGRESS_FILE):
        try:
            with open(REGION_PROGRESS_FILE, "r", encoding="utf-8") as file:
                return json.load(file)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_region_progress(progress_data):
    """Сохраняет прогресс обработки регионов по датам в JSON-файл."""
    with open(REGION_PROGRESS_FILE, "w", encoding="utf-8") as file:
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
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config.read_file(file)  # Читаем файл с явной кодировкой UTF-8

    return datetime.strptime(config.get("eis", "date", fallback=START_DATE.strftime("%Y-%m-%d")), "%Y-%m-%d")


def update_config_date(new_date):
    """Обновляет дату в config.ini с явной кодировкой UTF-8."""
    config = configparser.ConfigParser()

    # Читаем файл с правильной кодировкой
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config.read_file(file)

    config.set("eis", "date", new_date.strftime("%Y-%m-%d"))

    # Записываем обратно в файл с нужной кодировкой
    with open(CONFIG_PATH, "w", encoding="utf-8") as config_file:
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
            
            # Периодически выполняем миграцию завершенных контрактов (раз в день)
            # Выполняем миграцию каждые ~48 проверок (48 * 30 мин = 24 часа)
            # С таймаутом, чтобы не блокировать мониторинг
            if check_count % 48 == 0:
                logger.info("Выполнение плановой миграции завершенных контрактов...")
                print("\n🔄 Плановая миграция завершенных контрактов (таймаут 60 сек)...")
                try:
                    import threading
                    migration_result = {"completed": False, "error": None}
                    
                    def run_migration():
                        try:
                            migrate_completed_contracts()
                            migration_result["completed"] = True
                        except Exception as e:
                            migration_result["error"] = e
                    
                    migration_thread = threading.Thread(target=run_migration, daemon=True)
                    migration_thread.start()
                    migration_thread.join(timeout=60)  # Таймаут 60 секунд
                    
                    if migration_thread.is_alive():
                        logger.warning("Плановая миграция превысила таймаут 60 секунд")
                        print("⚠️  Миграция превысила таймаут, продолжаем мониторинг...")
                    elif migration_result["error"]:
                        logger.error(f"Ошибка при плановой миграции: {migration_result['error']}")
                    elif migration_result["completed"]:
                        print("✅ Плановая миграция завершена")
                except Exception as e:
                    logger.error(f"Ошибка при плановой миграции: {e}", exc_info=True)
                    print(f"⚠️  Ошибка при миграции: {e}")
            
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
        
        # Запуск прокси (stunnel на Windows, проверка nginx на Linux)
        print("📡 Проверка прокси-соединения...")
        try:
            proxy_runner = ProxyRunner()
            proxy_runner.run_proxy()
            platform_name = "Stunnel" if proxy_runner.platform == 'windows' else "Nginx"
            print(f"✅ {platform_name} успешно настроен")
        except RuntimeError as proxy_error:
            error_msg = str(proxy_error)
            logger.critical(f"Ошибка при настройке прокси: {error_msg}", exc_info=True)
            print(f"\n{'='*60}")
            print(f"❌ ОШИБКА ПРИ НАСТРОЙКЕ ПРОКСИ")
            print(f"{'='*60}")
            print(error_msg)
            print(f"{'='*60}")
            if proxy_runner.platform == 'windows':
                print("⚠️  Программа завершена из-за ошибки Stunnel.")
                print("   Проверьте логи в файле stunnel.log для получения подробной информации.")
            else:
                print("⚠️  Программа завершена из-за ошибки Nginx.")
                print("   Проверьте статус: systemctl status nginx")
                print("   Проверьте логи: tail -f /var/log/nginx/eis_error.log")
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
            
            # Проверяем и создаем таблицы для завершенных контрактов если нужно
            check_tables_exist()
            
            test_regions = get_region_codes()
            print(f"✅ Подключение к БД успешно (найдено регионов: {len(test_regions)})")
            
            # Выполняем миграцию завершенных контрактов при запуске (с таймаутом через threading)
            print("\n🔄 Проверка завершенных контрактов (таймаут 30 сек)...")
            try:
                import threading
                migration_result = {"completed": False, "error": None}
                
                def run_migration():
                    try:
                        migrate_completed_contracts()
                        migration_result["completed"] = True
                    except Exception as e:
                        migration_result["error"] = e
                
                migration_thread = threading.Thread(target=run_migration, daemon=True)
                migration_thread.start()
                migration_thread.join(timeout=30)  # Таймаут 30 секунд
                
                if migration_thread.is_alive():
                    logger.warning("Миграция завершенных контрактов превысила таймаут 30 секунд, пропускаем")
                    print("⚠️  Миграция завершенных контрактов превысила таймаут, продолжаем работу...")
                elif migration_result["error"]:
                    raise migration_result["error"]
                elif migration_result["completed"]:
                    print("✅ Миграция завершенных контрактов завершена")
            except Exception as migration_error:
                logger.error(f"Ошибка при миграции завершенных контрактов: {migration_error}", exc_info=True)
                print(f"⚠️  Ошибка при миграции завершенных контрактов: {migration_error}")
                print("   Продолжаем работу без миграции...")
            
            # Выполняем миграцию неизвестных и плохих контрактов при запуске (с таймаутом)
            print("\n🔄 Проверка неизвестных и плохих контрактов (таймаут 30 сек)...")
            try:
                from database_work.contracts_migration import migrate_unknown_and_bad_contracts
                
                migration_result = {"completed": False, "error": None}
                
                def run_migration_unknown():
                    try:
                        migrate_unknown_and_bad_contracts()
                        migration_result["completed"] = True
                    except Exception as e:
                        migration_result["error"] = e
                
                migration_thread = threading.Thread(target=run_migration_unknown, daemon=True)
                migration_thread.start()
                migration_thread.join(timeout=30)
                
                if migration_thread.is_alive():
                    logger.warning("Миграция неизвестных/плохих контрактов превысила таймаут 30 секунд, пропускаем")
                    print("⚠️  Миграция неизвестных/плохих контрактов превысила таймаут, продолжаем работу...")
                elif migration_result["error"]:
                    raise migration_result["error"]
                elif migration_result["completed"]:
                    print("✅ Миграция неизвестных/плохих контрактов завершена")
            except Exception as migration_error:
                logger.error(f"Ошибка при миграции неизвестных/плохих контрактов: {migration_error}", exc_info=True)
                print(f"⚠️  Ошибка при миграции неизвестных/плохих контрактов: {migration_error}")
                print("   Продолжаем работу без миграции...")
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
        logger.info(f"Начальная дата из config.ini: {initial_date.strftime('%Y-%m-%d')}")
        
        # Сохраняем исходную дату для возможного восстановления
        original_date = initial_date
        
        # Вычисляем количество дней для обработки
        total_days = (TODAY - initial_date).days + 1
        
        if total_days <= 0:
            print(f"⚠️  Внимание: Дата из config.ini ({initial_date.strftime('%Y-%m-%d')}) больше или равна текущей дате ({TODAY.strftime('%Y-%m-%d')})")
            print(f"   Программа завершена. Обновите дату в config.ini на более раннюю дату.")
            sys.exit(0)
        
        processed_count = 0
        error_count = 0
        
        # Обрабатываем все даты от initial_date до TODAY
        # НЕ пропускаем даты - файлы проверяются в БД автоматически
        # Это позволяет обработать новые ОКПД/регионы для старых дат
        print(f"\n📅 ПЛАН ОБРАБОТКИ:")
        print(f"   Начальная дата (из config.ini): {initial_date.strftime('%Y-%m-%d')}")
        print(f"   Конечная дата (сегодня): {TODAY.strftime('%Y-%m-%d')}")
        print(f"   Всего дней для обработки: {total_days}")
        print(f"   ℹ️  Файлы проверяются в БД - уже обработанные файлы будут автоматически пропущены")
        print(f"   ℹ️  Прогресс обработки регионов кешируется - при перезапуске продолжение с места остановки")
        print(f"\n{'='*60}\n")
        
        date_to_process = initial_date
        current_day = 0
        monitoring_mode = False
        
        while True:  # Бесконечный цикл для непрерывной работы
            # Проверяем, достигли ли мы текущей даты
            if date_to_process > TODAY:
                # Переходим в режим мониторинга для текущей даты
                if not monitoring_mode:
                    monitoring_mode = True
                    date_to_process = TODAY  # Обрабатываем текущую дату
                    print(f"\n{'='*60}")
                    print(f"📅 ДОСТИГНУТА ТЕКУЩАЯ ДАТА: {TODAY.strftime('%Y-%m-%d')}")
                    print(f"{'='*60}")
                    print(f"🔄 Переход в режим непрерывного мониторинга...")
                    logger.info(f"Достигнута текущая дата, переход в режим мониторинга")
            
            current_day += 1
            date_str = date_to_process.strftime("%Y-%m-%d")
            
            # Если в режиме мониторинга и это текущая дата
            if monitoring_mode and date_to_process == TODAY:
                # Проверяем наличие данных перед обработкой
                if not check_data_available(date_str):
                    # Данных нет - переходим в режим ожидания
                    monitor_for_new_data(date_to_process)
                    # После выхода из мониторинга (данные появились) продолжаем обработку
                    continue
            
            print(f"\n{'='*60}")
            if monitoring_mode:
                print(f"📅 [МОНИТОРИНГ] ОБРАБОТКА ДАТЫ: {date_str}")
            else:
                print(f"📅 [{current_day}/{total_days}] ОБРАБОТКА ДАТЫ: {date_str}")
            print(f"{'='*60}")
            logger.info(f"Начало обработки даты {date_str}")
            
            # Обновляем дату в конфиге ТОЛЬКО для текущей обработки
            update_config_date(date_to_process)
            logger.info(f"Дата в config.ini обновлена на {date_str} для обработки")
            
            # Загружаем прогресс обработки регионов для текущей даты
            processed_regions = get_processed_regions_for_date(date_str)
            if processed_regions:
                logger.info(f"Найдено уже обработанных регионов для даты {date_str}: {len(processed_regions)}")
            
            try:
                # Снимок статистики ДО обработки даты
                stats_before = stats_collector.get_snapshot()
                
                # Создаем новый EISRequester для каждой даты с правильной датой
                # Ошибки БД при инициализации обрабатываются здесь
                try:
                    eis_requester = EISRequester(date=date_str)
                except Exception as init_error:
                    # Проверяем, является ли ошибка критической ошибкой БД
                    from utils.exceptions import DatabaseError
                    import psycopg2
                    
                    is_db_error = (
                        isinstance(init_error, DatabaseError) or
                        isinstance(init_error, psycopg2.Error) or
                        (hasattr(init_error, '__cause__') and isinstance(init_error.__cause__, (DatabaseError, psycopg2.Error)))
                    )
                    
                    if is_db_error:
                        # КРИТИЧЕСКАЯ ОШИБКА БД при инициализации - завершаем программу
                        error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА БД при инициализации для даты {date_str}: {init_error}"
                        logger.critical(error_msg, exc_info=True)
                        print(f"\n{'='*60}")
                        print(error_msg)
                        print(f"{'='*60}")
                        print("⚠️  Программа завершена из-за ошибки БД.")
                        print("   Пожалуйста, проверьте подключение к БД и перезапустите программу вручную.")
                        print(f"{'='*60}\n")
                        sys.exit(1)
                    else:
                        # Другие ошибки инициализации - пробрасываем дальше
                        raise
                
                # Callback для сохранения прогресса обработки региона
                def save_region_progress_callback(region_code):
                    mark_region_processed(date_str, region_code)
                    logger.debug(f"Прогресс сохранен: регион {region_code} для даты {date_str}")
                
                # Обрабатываем запросы с учетом уже обработанных регионов
                eis_requester.process_requests(
                    processed_regions=processed_regions,
                    on_region_processed=save_region_progress_callback
                )
                
                # Снимок статистики ПОСЛЕ обработки даты
                stats_after = stats_collector.get_snapshot()
                
                # Вычисляем дельту (что добавилось за эту дату)
                date_stats = {}
                skipped_stats = {}
                
                # Собираем все ключи из обоих снимков
                all_keys = set(stats_before.keys()) | set(stats_after.keys())
                
                for key in all_keys:
                    before_value = stats_before.get(key, 0)
                    after_value = stats_after.get(key, 0)
                    delta = after_value - before_value
                    if delta > 0:
                        # Разделяем на добавленные и пропущенные
                        if "_skipped" in key:
                            skipped_stats[key] = delta
                        else:
                            date_stats[key] = delta
                
                # НЕ сохраняем дату в processed_dates.json
                # Файлы проверяются в БД - если файл уже обработан, он будет пропущен
                # Это позволяет обработать новые ОКПД/регионы для старых дат
                processed_count += 1
                
                # Очищаем прогресс обработки регионов для этой даты после успешного завершения
                clear_region_progress_for_date(date_str)
                logger.info(f"Прогресс обработки регионов для даты {date_str} очищен")
                
                # Выводим статистику по дате
                print(f"\n{'='*60}")
                print(f"✅ Дата {date_str} успешно обработана ({processed_count} из {total_days})")
                print(f"{'='*60}")
                
                # Всегда выводим статистику, даже если есть только пропуски
                print(f"📊 СТАТИСТИКА ПО ДАТЕ {date_str}:")
                
                # Детальная статистика по основным сущностям
                customers_added = date_stats.get('customer', 0)
                customers_skipped_dup = skipped_stats.get('customer_skipped_duplicate', 0)
                customers_skipped_contact = skipped_stats.get('customer_skipped_contact', 0)
                customers_total_skipped = customers_skipped_dup + customers_skipped_contact
                
                contractors_added = date_stats.get('contractor', 0)
                contractors_skipped_dup = skipped_stats.get('contractor_skipped_duplicate', 0)
                contractors_skipped_contact = skipped_stats.get('contractor_skipped_contact', 0)
                contractors_total_skipped = contractors_skipped_dup + contractors_skipped_contact
                
                contracts_44_added = date_stats.get('reestr_contract_44_fz', 0)
                contracts_223_added = date_stats.get('reestr_contract_223_fz', 0)
                contracts_total = contracts_44_added + contracts_223_added
                
                print(f"   👥 Заказчики: добавлено {customers_added}, пропущено {customers_total_skipped} (дубликаты: {customers_skipped_dup}, существующий контакт: {customers_skipped_contact})")
                print(f"   🏢 Подрядчики: добавлено {contractors_added}, пропущено {contractors_total_skipped} (дубликаты: {contractors_skipped_dup}, существующий контакт: {contractors_skipped_contact})")
                print(f"   📋 Контракты: добавлено {contracts_total} (44-ФЗ: {contracts_44_added}, 223-ФЗ: {contracts_223_added})")
                
                # Остальная статистика
                other_stats = {}
                for key, value in date_stats.items():
                    if key not in ['customer', 'contractor', 'reestr_contract_44_fz', 'reestr_contract_223_fz']:
                        other_stats[key] = value
                
                if other_stats:
                    print(f"   📦 Прочее:")
                    ru_labels = {
                        'links_documentation_44_fz': 'Ссылок 44-ФЗ',
                        'links_documentation_223_fz': 'Ссылок 223-ФЗ',
                        'trading_platform': 'Торговых площадок',
                        'file_names_xml': 'Файлов XML',
                    }
                    for key, value in other_stats.items():
                        label = ru_labels.get(key, key)
                        print(f"      • {label}: {value}")
                
                # Статистика пропущенных файлов
                files_skipped_processed = skipped_stats.get('files_skipped_already_processed', 0)
                files_skipped_no_okpd = skipped_stats.get('files_skipped_no_okpd', 0)
                files_total_skipped = files_skipped_processed + files_skipped_no_okpd
                
                if files_total_skipped > 0:
                    print(f"   📄 Файлы пропущены: {files_total_skipped} (уже обработаны: {files_skipped_processed}, нет ОКПД: {files_skipped_no_okpd})")
                
                logger.info(f"Дата {date_str} успешно обработана")
                
            except Exception as e:
                # Проверяем, является ли ошибка критической ошибкой БД
                from utils.exceptions import DatabaseError
                import psycopg2
                
                is_db_error = (
                    isinstance(e, DatabaseError) or
                    isinstance(e, psycopg2.Error) or
                    (hasattr(e, '__cause__') and isinstance(e.__cause__, (DatabaseError, psycopg2.Error)))
                )
                
                if is_db_error:
                    # КРИТИЧЕСКАЯ ОШИБКА БД - завершаем программу
                    error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА БД при обработке даты {date_str}: {e}"
                    logger.critical(error_msg, exc_info=True)
                    print(f"\n{'='*60}")
                    print(error_msg)
                    print(f"{'='*60}")
                    print("⚠️  Программа завершена из-за ошибки БД.")
                    print("   Пожалуйста, проверьте подключение к БД и перезапустите программу вручную.")
                    print(f"{'='*60}\n")
                    sys.exit(1)
                else:
                    # Другие ошибки - логируем и продолжаем
                    error_count += 1
                    logger.error(f"Ошибка при обработке даты {date_str}: {e}", exc_info=True)
                    print(f"❌ Ошибка при обработке даты {date_str}: {e}")
                    # НЕ сохраняем дату в processed_dates при ошибке, чтобы можно было повторить
            
            # Переходим к следующей дате
            if not monitoring_mode:
                date_to_process += timedelta(days=1)
            else:
                # В режиме мониторинга: после обработки текущей даты
                # проверяем вчерашнюю дату (данные за которую должны загрузиться в 2:00 ночи)
                yesterday = datetime.today() - timedelta(days=1)
                if date_to_process < yesterday:
                    # Если мы обработали дату раньше вчерашней, переходим к вчерашней
                    date_to_process = yesterday
                    print(f"📅 Переход к вчерашней дате для мониторинга: {date_to_process.strftime('%Y-%m-%d')}")
                elif date_to_process == yesterday:
                    # Если обработали вчерашнюю дату, переходим к сегодняшней
                    date_to_process = datetime.today()
                    print(f"📅 Переход к текущей дате для мониторинга: {date_to_process.strftime('%Y-%m-%d')}")
                else:
                    # Остаемся на текущей дате и продолжаем мониторинг
                    time.sleep(MONITORING_INTERVAL)
                    continue
        
        # Этот код не должен выполняться, так как цикл бесконечный
        # Но оставляем для совместимости на случай прерывания
        if processed_count > 0:
            last_processed_date = date_to_process - timedelta(days=1) if date_to_process > initial_date else date_to_process
            update_config_date(last_processed_date)
            logger.info(f"Дата в config.ini обновлена на последнюю обработанную: {last_processed_date.strftime('%Y-%m-%d')}")
            print(f"\n💾 Дата в config.ini обновлена на последнюю обработанную: {last_processed_date.strftime('%Y-%m-%d')}")
        
        # Выводим статистику (если цикл был прерван)
        print(f"\n{'='*60}")
        print(f"📊 СТАТИСТИКА:")
        print(f"   - Обработано дат: {processed_count}")
        if error_count > 0:
            print(f"   - Ошибок при обработке: {error_count}")
        logger.info(f"Обработка прервана. Обработано: {processed_count}, ошибок: {error_count}")

        # Снимок счётчиков
        stats = stats_collector.get_snapshot()

        # Удобные отображаемые названия
        RU_LABELS = {
            'customer': 'Заказчики (customer)',
            'contractor': 'Подрядчики (contractor)',
            'reestr_contract_44_fz': 'Контракты 44-ФЗ (reestr_contract_44_fz)',
            'reestr_contract_223_fz': 'Контракты 223-ФЗ (reestr_contract_223_fz)',
            'links_documentation_44_fz': 'Ссылки документов 44-ФЗ',
            'links_documentation_223_fz': 'Ссылки документов 223-ФЗ',
            'trading_platform': 'Торговые площадки',
            'file_names_xml': 'Файлы XML (учтено)',
        }

        # Печатаем только непустые счетчики
        if stats:
            print("   - Добавлено записей:")
            for key in RU_LABELS:
                if key in stats:
                    print(f"      • {RU_LABELS[key]}: {stats.get(key, 0)}")
        else:
            print("   - Нет новых вставок в БД за текущий запуск.")
        
    except KeyboardInterrupt:
        print("\n⚠️  Программа прервана пользователем")
        logger.error("Программа прервана пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        logger.error(f"Критическая ошибка в main.py: {e}", exc_info=True)
        raise
