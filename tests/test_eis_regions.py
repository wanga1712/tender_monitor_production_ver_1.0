"""
Тестовый скрипт для получения данных из ЕИС по регионам без запуска основного цикла main.py.

Что делает:
- инициализирует EISRequester с датой из config.ini (или переданной через аргумент);
- проходит по всем регионам (как в main), но без обновления config.ini и без мониторинга;
- пишет ход работы в консоль и логи.

Запуск на сервере:
  cd /opt/tendermonitor
  source venv/bin/activate
  python tests/test_eis_regions.py
"""

from __future__ import annotations

import sys
from datetime import datetime

from eis_requester import EISRequester
from utils.logger_config import get_logger


logger = get_logger()


def run_for_date(date_str: str | None = None) -> None:
    """
    Запускает EISRequester для указанной даты (или даты из config.ini)
    и обрабатывает все регионы.

    :param date_str: Дата в формате YYYY-MM-DD. Если None, берётся из config.ini.
    """
    if date_str:
        try:
            # Валидация формата даты
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError as e:
            print(f"❌ Некорректный формат даты '{date_str}': {e}")
            sys.exit(1)

    requester = EISRequester(date=date_str)

    print("\n============================================================")
    print("ТЕСТОВЫЙ ЗАПУСК EISRequester ПО РЕГИОНАМ")
    print("============================================================")
    print(f"Дата для выборки: {requester.date}")
    print(f"Всего регионов в конфигурации/БД: {len(requester.regions)}")
    print("============================================================\n")

    try:
        requester.process_requests()
    except KeyboardInterrupt:
        print("\n⚠️  Прервано пользователем")
        logger.info("Тестовый запуск EISRequester прерван пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка в тестовом запуске EISRequester: {e}")
        logger.error(f"Критическая ошибка в тестовом запуске EISRequester: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Можно передать дату аргументом: python tests/test_eis_regions.py 2025-12-15
    cli_date = sys.argv[1] if len(sys.argv) > 1 else None
    run_for_date(cli_date)

