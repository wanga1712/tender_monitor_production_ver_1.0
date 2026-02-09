"""
MODULE: scripts.test_existing_folders
RESPONSIBILITY: Testing processing of already downloaded tenders (without downloading new files).
ALLOWED: sys, pathlib, loguru, core.dependency_injection, services.archive_background_runner.
FORBIDDEN: None.
ERRORS: None.

Тест обработки УЖЕ СКАЧАННЫХ торгов (без скачивания)."""

import sys
from pathlib import Path
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.dependency_injection import container
from services.archive_background_runner import ArchiveBackgroundRunner


def main():
    user_id = 1
    
    tender_db_manager = container.get_tender_database_manager()
    product_db_manager = container.get_commercial_database_manager()
    
    logger.info("=" * 80)
    logger.info("ТЕСТ: Обработка УЖЕ СКАЧАННЫХ торгов (без скачивания)")
    logger.info("=" * 80)
    logger.info("Этот скрипт обрабатывает существующие папки с документами")
    logger.info("БЕЗ скачивания новых файлов")
    logger.info("=" * 80)
    
    runner = ArchiveBackgroundRunner(
        tender_db_manager=tender_db_manager,
        product_db_manager=product_db_manager,
        user_id=user_id,
        max_workers=2,
        batch_size=10,
        batch_delay=1.0,
    )
    
    # Вызываем ТОЛЬКО обработку существующих папок
    # specific_tender_ids=None означает: обработать все существующие папки
    result = runner.run(
        specific_tender_ids=None,  # Не указываем конкретные ID
        registry_type="44fz",
        tender_type="won",
        # Система автоматически найдёт и обработает существующие папки
    )
    
    logger.info("=" * 80)
    logger.info("РЕЗУЛЬТАТ")
    logger.info("=" * 80)
    logger.info(f"Существующих папок обработано: {result.get('existing_folders', 0)}")
    logger.info(f"Новых торгов: {result.get('total_tenders', 0)}")
    logger.info(f"Успешно обработано: {result.get('processed', 0)}")
    logger.info(f"Ошибок: {result.get('errors', 0)}")
    logger.info(f"Найдено совпадений: {result.get('total_matches', 0)}")
    logger.info(f"Общее время: {result.get('total_time', 0):.1f} сек")
    logger.info("=" * 80)
    
    if result.get('errors', 0) > 0:
        logger.warning(f"Есть ошибки! Проверьте .cursor/debug.log для деталей")


if __name__ == "__main__":
    main()
