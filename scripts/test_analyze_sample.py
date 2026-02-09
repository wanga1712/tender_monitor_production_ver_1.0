"""
MODULE: scripts.test_analyze_sample
RESPONSIBILITY: Testing processing of a sample of unprocessed won 44FZ tenders.
ALLOWED: sys, pathlib, loguru, core.dependency_injection, services.archive_background_runner.
FORBIDDEN: None.
ERRORS: None.

Тестовый запуск обработки документов для НЕОБРАБОТАННЫХ выигранных 44ФЗ."""

import sys
from pathlib import Path
from loguru import logger

# Гарантируем, что корень проекта в sys.path (запуск из любого каталога)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.dependency_injection import container
from services.archive_background_runner import ArchiveBackgroundRunner


def main():
    user_id = 1
    category_id = 2  # "Стройка" из сохранённых настроек
    limit = 50  # Берём больше, чтобы найти необработанные

    # Получаем менеджеры БД и репозитории через контейнер
    tender_db_manager = container.get_tender_database_manager()
    product_db_manager = container.get_commercial_database_manager()
    tender_repo = container.get_tender_repository()
    tender_match_repo = container.get_tender_match_repository()

    user_okpd_codes = [
        item.get("okpd_code")
        for item in tender_repo.get_user_okpd_codes(user_id, category_id)
        if item.get("okpd_code")
    ]
    user_stop_words = [
        item.get("stop_word")
        for item in tender_repo.get_user_stop_words(user_id)
        if item.get("stop_word")
    ]

    logger.info(
        f"Пробный выбор закупок: okpd={len(user_okpd_codes)}, stop_words={len(user_stop_words)}, limit={limit}"
    )
    if not user_okpd_codes:
        logger.error("ОКПД пустые — проверьте сохранённые настройки пользователя/категории.")
        return
    
    tenders = tender_repo.get_won_tenders_44fz(
        user_id=user_id,
        user_okpd_codes=user_okpd_codes,
        user_stop_words=user_stop_words,
        region_id=None,
        category_id=category_id,
        limit=limit,
    )
    logger.info(f"Получено закупок из БД: {len(tenders)}")
    if not tenders:
        logger.error("Закупок не найдено по текущим фильтрам.")
        return

    # Фильтруем только НЕОБРАБОТАННЫЕ торги
    tender_ids = [t.get("id") for t in tenders if t.get("id")]
    processed_results = tender_match_repo.get_match_results_batch(tender_ids, "44fz")
    
    unprocessed_tenders = [
        t for t in tenders
        if t.get("id") not in processed_results
    ]
    
    logger.info(f"Уже обработано: {len(processed_results)}, необработанных: {len(unprocessed_tenders)}")
    
    if not unprocessed_tenders:
        logger.warning("Все торги уже обработаны! Нечего анализировать.")
        return

    # Берём первые 10 необработанных
    tenders_to_process = unprocessed_tenders[:10]
    specific_ids = [
        {"id": t.get("id"), "registry_type": "44fz"}
        for t in tenders_to_process
        if t.get("id")
    ]
    
    logger.info(f"Будем обрабатывать {len(specific_ids)} необработанных торгов: {[s['id'] for s in specific_ids]}")

    runner = ArchiveBackgroundRunner(
        tender_db_manager=tender_db_manager,
        product_db_manager=product_db_manager,
        user_id=user_id,
        max_workers=2,
        batch_size=5,
        batch_delay=1.0,
    )
    result = runner.run(specific_tender_ids=specific_ids, registry_type="44fz", tender_type="won")
    logger.info(f"Завершено: {result}")


if __name__ == "__main__":
    main()
