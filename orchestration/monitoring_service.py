"""
Оркестратор основного цикла TenderMonitor.

Задачи:
- управлять датами обработки (от начальной до текущей и далее в режиме мониторинга);
- координировать запросы к ЕИС и обработку регионов;
- сохранять и восстанавливать прогресс по регионам и датам;
- взаимодействовать с измерением статистики и контролем памяти.

ВНИМАНИЕ:
- В этом модуле нет ОС-специфичной логики (никакого /proc, systemd и т.п.);
- Вся работа с окружением и запуском сервиса остаётся в main.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional, Set, Dict, Any


@dataclass
class MonitoringConfig:
    """Конфигурация цикла мониторинга и обработки дат."""

    start_date: datetime
    today: datetime
    monitoring_interval_seconds: int
    eis_data_upload_hour: int
    direction: str = "forward"  # forward | backward
    stop_before_date: datetime | None = None  # for backward: stop when date < this


class TenderMonitorService:
    """
    Класс-оркестратор, инкапсулирующий основной цикл обработки дат.

    Отвечает только за доменную и оркестрационную логику:
    - выбор дат;
    - запуск мониторинга появления данных в ЕИС;
    - вызов обработчиков даты и сохранение прогресса.

    Взаимодействие с:
    - логгером (loguru) через переданный экземпляр;
    - EISRequester через переданные фабрики/коллбеки;
    - БД и файловой системой — только через переданные функции.
    """

    def __init__(
        self,
        config: MonitoringConfig,
        logger,
        *,
        check_data_available: Callable[[str], bool],
        monitor_for_new_data: Callable[[datetime], bool],
        get_processed_regions_for_date: Callable[[str], Set[int]],
        mark_region_processed: Callable[[str, int], None],
        clear_region_progress_for_date: Callable[[str], None],
        update_config_date: Callable[[datetime], None],
        get_stats_snapshot: Callable[[], Dict[str, int]],
        create_eis_requester: Callable[[str], Any],
        on_memory_check: Callable[[str], None],
    ) -> None:
        """
        Инициализация сервиса TenderMonitor.

        Все внешние зависимости передаются через параметры конструктора,
        чтобы не смешивать слои и упростить тестирование.
        """
        self._cfg = config
        self._logger = logger
        self._check_data_available = check_data_available
        self._monitor_for_new_data = monitor_for_new_data
        self._get_processed_regions_for_date = get_processed_regions_for_date
        self._mark_region_processed = mark_region_processed
        self._clear_region_progress_for_date = clear_region_progress_for_date
        self._update_config_date = update_config_date
        self._get_stats_snapshot = get_stats_snapshot
        self._create_eis_requester = create_eis_requester
        self._on_memory_check = on_memory_check

    def run(self) -> None:
        """
        Запускает основной бесконечный цикл обработки дат и мониторинга.
        """
        import json
        import os
        from datetime import datetime, timedelta

        class PendingDatesManager:
            def __init__(self, filepath="/opt/tendermonitor/pending_dates.json"):
                self.filepath = filepath
                self.pending = {}
                self.load()

            def load(self):
                if os.path.exists(self.filepath):
                    try:
                        with open(self.filepath, "r") as f:
                            self.pending = json.load(f)
                    except:
                        self.pending = {}

            def save(self):
                with open(self.filepath, "w") as f:
                    json.dump(self.pending, f, indent=4)

            def add_pending(self, date_str: str):
                if date_str not in self.pending:
                    self.pending[date_str] = {
                        "first_seen": datetime.now().isoformat(),
                        "last_tried": datetime.now().isoformat(),
                        "retries": 0
                    }
                    self.save()

            def remove_pending(self, date_str: str):
                if date_str in self.pending:
                    del self.pending[date_str]
                    self.save()

            def get_date_to_retry(self) -> str | None:
                now = datetime.now()
                for date_str, info in self.pending.items():
                    try:
                        last_tried = datetime.fromisoformat(info["last_tried"])
                        if now - last_tried > timedelta(hours=12):
                            return date_str
                    except:
                        pass
                return None

            def mark_tried(self, date_str: str):
                if date_str in self.pending:
                    self.pending[date_str]["last_tried"] = datetime.now().isoformat()
                    self.pending[date_str]["retries"] += 1
                    self.save()

        pending_mgr = PendingDatesManager()

        processed_count = 0
        error_count = 0

        initial_date = self._cfg.start_date
        direction = (self._cfg.direction or "forward").lower()
        stop_before = self._cfg.stop_before_date

        if direction == "backward":
            if stop_before is None:
                print("⚠️  backward: не задан stop_before_date")
                return
            total_days = (initial_date - stop_before).days
            if total_days <= 0:
                print(f"⚠️  backward: старт не позже стопа")
                return
        else:
            print(f"\n📅 ПЛАН ОБРАБОТКИ FORWARD:")
            print(f"   Начальная дата: {initial_date.strftime('%Y-%m-%d')}")

        date_to_process = initial_date
        current_day = 0

        while True:
            # Re-evaluate physical calendar today
            today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

            # Retry pending dates if any (only in forward mode)
            if direction != "backward":
                retry_date = pending_mgr.get_date_to_retry()
                if retry_date:
                    print(f"\n🔄 [RETRY] Попытка обработки пропущенной даты: {retry_date}")
                    self._process_single_date(retry_date, is_retry=True, pending_mgr=pending_mgr)
                    pending_mgr.mark_tried(retry_date)
                    continue

            if direction == "backward":
                if date_to_process < stop_before:
                    print(f"\n✅ BACKWARD: достигнута граница {stop_before.strftime('%Y-%m-%d')}, выход")
                    return
            else:
                # В FORWARD режиме: если date_to_process >= сегодня, данных быть не может
                if date_to_process >= today:
                    print(f"\n{'=' * 60}")
                    print(f"📅 ОЖИДАНИЕ: дата {date_to_process.strftime('%Y-%m-%d')} ещё не наступила/не завершилась.")
                    print(f"🔄 Программа засыпает на {self._cfg.monitoring_interval_seconds // 60} минут...")
                    print(f"{'=' * 60}")
                    import time
                    time.sleep(self._cfg.monitoring_interval_seconds)
                    continue

            current_day += 1
            date_str = date_to_process.strftime("%Y-%m-%d")

            print(f"\n{'=' * 60}")
            print(f"📅 ОБРАБОТКА ДАТЫ: {date_str}")
            print(f"{'=' * 60}")

            # Обновляем дату в конфиге только для основной обработки (не для retry)
            self._update_config_date(date_to_process)

            self._process_single_date(date_str, is_retry=False, pending_mgr=pending_mgr)

            # Переход к следующей дате
            if direction == "backward":
                date_to_process -= timedelta(days=1)
            else:
                date_to_process += timedelta(days=1)

    def _process_single_date(self, date_str: str, is_retry: bool, pending_mgr) -> None:
        processed_regions = self._get_processed_regions_for_date(date_str)
        try:
            stats_before = self._get_stats_snapshot()
            eis_requester = self._create_eis_requester(date_str)

            def on_region_processed(region_code: int) -> None:
                self._mark_region_processed(date_str, region_code)

            eis_requester.process_requests(
                processed_regions=processed_regions,
                on_region_processed=on_region_processed,
            )

            stats_after = self._get_stats_snapshot()

            date_stats = {}
            skipped_stats = {}
            all_keys = set(stats_before.keys()) | set(stats_after.keys())
            for key in all_keys:
                delta = stats_after.get(key, 0) - stats_before.get(key, 0)
                if delta > 0:
                    if "_skipped" in key:
                        skipped_stats[key] = delta
                    else:
                        date_stats[key] = delta

            # Check if any XMLs were successfully downloaded and processed
            xmls_added = date_stats.get("file_names_xml", 0)
            contracts_added = date_stats.get("reestr_contract_44_fz", 0) + date_stats.get("reestr_contract_223_fz", 0)

            if xmls_added == 0 and contracts_added == 0:
                print(f"⚠️  ДЛЯ ДАТЫ {date_str} ДАННЫЕ НЕ НАЙДЕНЫ (0 файлов).")
                if not is_retry:
                    print(f"📌 Добавляем {date_str} в очередь отложенной проверки (PENDING).")
                    pending_mgr.add_pending(date_str)
            else:
                if is_retry:
                    print(f"✅ Успешно извлечены данные для пропущенной даты {date_str}!")
                    pending_mgr.remove_pending(date_str)

            self._clear_region_progress_for_date(date_str)
            self._print_date_stats(date_str, 1, 1, date_stats, skipped_stats)

        except Exception as e:
            self._logger.error(f"Ошибка при обработке {date_str}: {e}")
            raise
        finally:
            self._on_memory_check(f"после обработки даты {date_str}")

    def _print_date_stats(
        self,
        date_str: str,
        processed_count: int,
        total_days: int,
        date_stats: Dict[str, int],
        skipped_stats: Dict[str, int],
    ) -> None:
        """Выводит человекочитаемую статистику по одной дате."""
        print(f"\n{'=' * 60}")
        print(
            f"✅ Дата {date_str} успешно обработана "
            f"({processed_count} из {total_days})"
        )
        print(f"{'=' * 60}")

        print(f"📊 СТАТИСТИКА ПО ДАТЕ {date_str}:")

        customers_added = date_stats.get("customer", 0)
        customers_skipped_dup = skipped_stats.get("customer_skipped_duplicate", 0)
        customers_skipped_contact = skipped_stats.get("customer_skipped_contact", 0)
        customers_total_skipped = customers_skipped_dup + customers_skipped_contact

        contractors_added = date_stats.get("contractor", 0)
        contractors_skipped_dup = skipped_stats.get("contractor_skipped_duplicate", 0)
        contractors_skipped_contact = skipped_stats.get("contractor_skipped_contact", 0)
        contractors_total_skipped = (
            contractors_skipped_dup + contractors_skipped_contact
        )

        contracts_44_added = date_stats.get("reestr_contract_44_fz", 0)
        contracts_223_added = date_stats.get("reestr_contract_223_fz", 0)
        contracts_total = contracts_44_added + contracts_223_added

        print(
            "   👥 Заказчики: добавлено "
            f"{customers_added}, пропущено {customers_total_skipped} "
            f"(дубликаты: {customers_skipped_dup}, существующий контакт: {customers_skipped_contact})"
        )
        print(
            "   🏢 Подрядчики: добавлено "
            f"{contractors_added}, пропущено {contractors_total_skipped} "
            f"(дубликаты: {contractors_skipped_dup}, существующий контакт: {contractors_skipped_contact})"
        )
        print(
            "   📋 Контракты: добавлено "
            f"{contracts_total} (44-ФЗ: {contracts_44_added}, 223-ФЗ: {contracts_223_added})"
        )

        other_stats: Dict[str, int] = {}
        for key, value in date_stats.items():
            if key not in [
                "customer",
                "contractor",
                "reestr_contract_44_fz",
                "reestr_contract_223_fz",
            ]:
                other_stats[key] = value

        if other_stats:
            print("   📦 Прочее:")
            ru_labels = {
                "links_documentation_44_fz": "Ссылок 44-ФЗ",
                "links_documentation_223_fz": "Ссылок 223-ФЗ",
                "trading_platform": "Торговых площадок",
                "file_names_xml": "Файлов XML",
            }
            for key, value in other_stats.items():
                label = ru_labels.get(key, key)
                print(f"      • {label}: {value}")

        files_skipped_processed = skipped_stats.get(
            "files_skipped_already_processed", 0
        )
        files_skipped_no_okpd = skipped_stats.get("files_skipped_no_okpd", 0)
        files_total_skipped = files_skipped_processed + files_skipped_no_okpd

        if files_total_skipped > 0:
            print(
                "   📄 Файлы пропущены: "
                f"{files_total_skipped} (уже обработаны: {files_skipped_processed}, нет ОКПД: {files_skipped_no_okpd})"
            )


