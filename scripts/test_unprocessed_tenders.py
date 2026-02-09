"""
MODULE: scripts.test_unprocessed_tenders
RESPONSIBILITY: Testing document analysis for unprocessed tenders.
ALLOWED: sys, pathlib, loguru, core.dependency_injection, services.archive_background_runner.
FORBIDDEN: None.
ERRORS: None.

Тест анализа документов для НЕОБРАБОТАННЫХ торгов (tdm.id IS NULL)."""

import sys
from pathlib import Path
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.dependency_injection import container
from services.archive_background_runner import ArchiveBackgroundRunner


def get_unprocessed_won_tenders_44fz(
    tender_db_manager,
    user_id: int,
    category_id: int,
    limit: int = 200
):
    """Получить НЕОБРАБОТАННЫЕ выигранные торги 44ФЗ."""
    
    # Получаем фильтры пользователя
    tender_repo = container.get_tender_repository()
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
    
    if not user_okpd_codes:
        logger.error("ОКПД пустые — проверьте настройки пользователя/категории.")
        return []
    
    # Получаем OKPD ID
    query_okpd = """
        SELECT DISTINCT id FROM collection_codes_okpd
        WHERE main_code = ANY(%s) OR sub_code = ANY(%s)
    """
    okpd_results = tender_db_manager.execute_query(
        query_okpd,
        (user_okpd_codes, user_okpd_codes)
    )
    okpd_ids = [row["id"] for row in okpd_results]
    
    if not okpd_ids:
        logger.warning("OKPD ID не найдены в БД")
        return []
    
    # Строим условие для стоп-слов
    stop_conditions = []
    stop_params = []
    if user_stop_words:
        for word in user_stop_words:
            stop_conditions.append("r.auction_name NOT ILIKE %s")
            stop_params.append(f"%{word}%")
    
    stop_filter = " AND " + " AND ".join(stop_conditions) if stop_conditions else ""
    
    # SQL запрос для НЕОБРАБОТАННЫХ торгов (tdm.id IS NULL)
    okpd_placeholders = ",".join(["%s"] * len(okpd_ids))
    query = f"""
        SELECT DISTINCT 
            r.id,
            r.contract_number,
            r.tender_link,
            r.auction_name,
            r.okpd_id,
            r.customer_id,
            r.region_id,
            r.status_id,
            r.initial_price,
            r.final_price,
            r.start_date,
            r.end_date,
            r.delivery_end_date,
            '44fz' as registry_type
        FROM reestr_contract_44_fz r
        LEFT JOIN tender_document_matches tdm ON tdm.tender_id = r.id AND tdm.registry_type = '44fz'
        WHERE 1=1
            AND r.status_id = 3  -- Разыграна
            AND r.okpd_id IN ({okpd_placeholders})
            {stop_filter}
            AND tdm.id IS NULL  -- ТОЛЬКО необработанные
            AND NOT EXISTS (
                SELECT 1 FROM sales_deals sd
                WHERE sd.tender_id = r.id AND sd.user_id = %s
            )
        ORDER BY r.id DESC
        LIMIT %s
    """
    
    params = list(okpd_ids) + stop_params + [user_id, limit]
    
    logger.info(f"Запрос необработанных торгов: okpd={len(okpd_ids)}, stop_words={len(user_stop_words)}, limit={limit}")
    results = tender_db_manager.execute_query(query, tuple(params))
    
    return results or []


def main():
    user_id = 1
    category_id = 2  # "Стройка"
    limit_to_find = 200  # Ищем среди 200 последних
    limit_to_process = 10  # Обрабатываем только 10
    
    tender_db_manager = container.get_tender_database_manager()
    product_db_manager = container.get_commercial_database_manager()
    
    logger.info("=" * 80)
    logger.info("ТЕСТ: Поиск и обработка НЕОБРАБОТАННЫХ торгов 44ФЗ")
    logger.info("=" * 80)
    
    # Получаем необработанные торги
    unprocessed_tenders = get_unprocessed_won_tenders_44fz(
        tender_db_manager,
        user_id,
        category_id,
        limit=limit_to_find
    )
    
    logger.info(f"Найдено необработанных торгов: {len(unprocessed_tenders)}")
    
    if not unprocessed_tenders:
        logger.warning("Необработанных торгов не найдено!")
        logger.info("Попробуйте:")
        logger.info("1. Увеличить limit_to_find (сейчас 200)")
        logger.info("2. Проверить фильтры ОКПД и стоп-слов")
        logger.info("3. Проверить что есть разыгранные торги (status_id=3)")
        return
    
    # Берём первые N для обработки
    tenders_to_process = unprocessed_tenders[:limit_to_process]
    specific_ids = [
        {"id": t["id"], "registry_type": "44fz"}
        for t in tenders_to_process
    ]
    
    logger.info(f"Будем обрабатывать {len(specific_ids)} торгов:")
    for tender in tenders_to_process:
        logger.info(f"  - ID {tender['id']}: {tender.get('auction_name', 'N/A')[:100]}")
    
    logger.info("=" * 80)
    logger.info("Запуск обработки...")
    logger.info("=" * 80)
    
    runner = ArchiveBackgroundRunner(
        tender_db_manager=tender_db_manager,
        product_db_manager=product_db_manager,
        user_id=user_id,
        max_workers=2,
        batch_size=5,
        batch_delay=1.0,
    )
    
    result = runner.run(
        specific_tender_ids=specific_ids,
        registry_type="44fz",
        tender_type="won"
    )
    
    logger.info("=" * 80)
    logger.info("РЕЗУЛЬТАТ")
    logger.info("=" * 80)
    logger.info(f"Обработано: {result.get('processed', 0)}")
    logger.info(f"Ошибок: {result.get('errors', 0)}")
    logger.info(f"Пропущено (нет документов): {result.get('skipped_no_docs', 0)}")
    logger.info(f"Найдено совпадений: {result.get('total_matches', 0)}")
    logger.info(f"Общее время: {result.get('total_time', 0):.1f} сек")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
