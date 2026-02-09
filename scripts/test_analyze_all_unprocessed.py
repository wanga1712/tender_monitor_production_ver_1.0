"""
MODULE: scripts.test_analyze_all_unprocessed
RESPONSIBILITY: Testing bulk processing of all unprocessed won 44FZ tenders.
ALLOWED: sys, pathlib, loguru, core.dependency_injection, services.archive_background_runner.
FORBIDDEN: None.
ERRORS: None.

Запуск обработки ВСЕХ необработанных выигранных 44ФЗ (без specific_tender_ids)."""

import sys
from pathlib import Path
from loguru import logger

# Гарантируем, что корень проекта в sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.dependency_injection import container
from services.archive_background_runner import ArchiveBackgroundRunner


def main():
    user_id = 1
    
    # Получаем менеджеры БД через контейнер
    tender_db_manager = container.get_tender_database_manager()
    product_db_manager = container.get_commercial_database_manager()

    logger.info("Запуск обработки ВСЕХ необработанных выигранных торгов 44ФЗ")
    logger.info("Система сама найдёт необработанные торги через батч-проверку")

    runner = ArchiveBackgroundRunner(
        tender_db_manager=tender_db_manager,
        product_db_manager=product_db_manager,
        user_id=user_id,
        max_workers=2,
        batch_size=10,
        batch_delay=1.0,
    )
    
    # БЕЗ specific_tender_ids - система сама найдёт необработанные
    result = runner.run(
        specific_tender_ids=None,  # Не указываем конкретные ID
        registry_type="44fz",
        tender_type="won"
    )
    
    logger.info(f"Завершено: {result}")
    logger.info(f"Обработано: {result.get('processed', 0)}")
    logger.info(f"Ошибок: {result.get('errors', 0)}")
    logger.info(f"Найдено совпадений: {result.get('total_matches', 0)}")


if __name__ == "__main__":
    main()
