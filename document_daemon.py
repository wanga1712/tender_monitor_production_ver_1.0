import os
import time
import signal
from datetime import datetime, timedelta

from loguru import logger

from config.settings import Config
from core.database import DatabaseManager
from core.tender_database import TenderDatabaseManager
from services.archive_background_runner import ArchiveBackgroundRunner


running = True


def handle_signal(signum, frame) -> None:
    global running
    running = False
    logger.info("Получен сигнал завершения: %s", signum)


def _sleep_until_next_midnight(interval_seconds: int) -> None:
    try:
        now = datetime.now()
        next_day = now.date() + timedelta(days=1)
        next_midnight = datetime.combine(next_day, datetime.min.time())
        seconds = (next_midnight - now).total_seconds()
        if seconds <= 0:
            seconds = float(interval_seconds)
        logger.info(
            "Нет новых торгов для обработки, спим до полуночи (%s секунд)",
            int(seconds),
        )
        time.sleep(seconds)
    except Exception as e:
        logger.warning("Ошибка при расчете времени до полуночи: %s", e)
        logger.info("Пауза %s секунд до следующего цикла", interval_seconds)
        time.sleep(interval_seconds)


def main() -> None:
    global running

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    config = Config()

    tender_db_manager = TenderDatabaseManager(config.tender_database)
    product_db_manager = DatabaseManager(config.database)

    tender_db_manager.connect()
    product_db_manager.connect()

    user_id = int(os.getenv("TENDER_MONITOR_USER_ID", "1"))
    interval_seconds = int(os.getenv("TENDER_MONITOR_INTERVAL_SEC", "300"))

    runner = ArchiveBackgroundRunner(
        tender_db_manager=tender_db_manager,
        product_db_manager=product_db_manager,
        user_id=user_id,
        max_workers=2,
        batch_size=5,
        batch_delay=10.0,
    )

    while running:
        logger.info("=== Запуск цикла обработки документов (tender_type=full) ===")
        result = None
        try:
            result = runner.run(tender_type="full")
            logger.info("Цикл обработки завершен")
        except Exception as e:
            logger.exception("Критическая ошибка в цикле демона: %s", e)

        if not running:
            break

        total_tenders = 0
        if isinstance(result, dict):
            try:
                total_tenders = int(result.get("total_tenders", 0))
            except Exception:
                total_tenders = 0

        if total_tenders == 0:
            _sleep_until_next_midnight(interval_seconds)
        else:
            logger.info("Пауза %s секунд до следующего цикла", interval_seconds)
            time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
