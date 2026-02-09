"""
MODULE: scripts.show_new_tenders_query
RESPONSIBILITY: Displaying the SQL query used to fetch new tenders.
ALLOWED: sys, pathlib, datetime, services.tender_repositories.tender_query_builder, services.tender_repositories.feeds.feed_filters, services.tender_repositories.feeds.new_tenders_service, core.tender_database, services.tender_repositories.tender_documents_repository, config.settings, loguru, services.tender_repository.
FORBIDDEN: None.
ERRORS: None.

Скрипт для вывода SQL запроса для получения новых торгов.
"""

import sys
from pathlib import Path
from datetime import date

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.tender_repositories.tender_query_builder import TenderQueryBuilder
from services.tender_repositories.feeds.feed_filters import FeedFilters
from services.tender_repositories.feeds.new_tenders_service import NewTendersService
from core.tender_database import TenderDatabaseManager
from services.tender_repositories.tender_documents_repository import TenderDocumentsRepository
from config.settings import config
from loguru import logger


def show_query(user_id: int = 1, registry_type: str = "44fz", limit: int = 1000):
    """Выводит SQL запрос для получения новых торгов"""
    
    if not config.tender_database:
        logger.error("Конфигурация БД tender_monitor не задана")
        return
    
    db_manager = TenderDatabaseManager()
    db_manager.connect()
    
    documents_repo = TenderDocumentsRepository(db_manager)
    service = NewTendersService(db_manager, documents_repo)
    
    # Получаем ОКПД коды пользователя
    from services.tender_services.tender_repository_facade import TenderRepositoryFacade
    tender_repo = TenderRepositoryFacade(db_manager)
    user_okpd = tender_repo.get_user_okpd_codes(user_id)
    okpd_codes = [okpd.get('okpd_code') for okpd in user_okpd if okpd.get('okpd_code')]
    
    if not okpd_codes:
        logger.warning(f"У пользователя {user_id} нет ОКПД кодов")
        return
    
    # Получаем стоп-слова
    user_stop_words = tender_repo.get_user_stop_words(user_id)
    stop_words = [sw.get('stop_word') for sw in user_stop_words if sw.get('stop_word')]
    
    # Создаем фильтры
    filters = FeedFilters(
        user_id=user_id,
        okpd_codes=okpd_codes,
        stop_words=stop_words,
        region_id=None,
        category_id=None,
        limit=limit,
    )
    
    # Получаем okpd_ids
    okpd_ids = service._resolve_okpd_ids(okpd_codes)
    
    if not okpd_ids:
        logger.warning("Не удалось получить okpd_ids")
        return
    
    # Строим запрос
    select_query, select_params = service._build_new_query(registry_type, filters, okpd_ids)
    count_query, count_params = service._build_new_count_query(registry_type, filters, okpd_ids)
    
    print("\n" + "="*80)
    print(f"SQL ЗАПРОС ДЛЯ ПОЛУЧЕНИЯ НОВЫХ ТОРГОВ {registry_type.upper()}")
    print("="*80)
    print("\n--- SELECT запрос (для получения данных): ---")
    print(select_query)
    print("\n--- Параметры: ---")
    for i, param in enumerate(select_params, 1):
        print(f"  ${i} = {param}")
    
    print("\n--- COUNT запрос (для получения количества): ---")
    print(count_query)
    print("\n--- Параметры: ---")
    for i, param in enumerate(count_params, 1):
        print(f"  ${i} = {param}")
    
    print("\n--- Для выполнения в psql (SELECT): ---")
    # Заменяем %s на $1, $2, и т.д.
    psql_query = select_query
    for i, param in enumerate(select_params, 1):
        psql_query = psql_query.replace("%s", f"${i}", 1)
    print(psql_query)
    
    print("\n--- Для выполнения в psql (COUNT): ---")
    psql_count = count_query
    for i, param in enumerate(count_params, 1):
        psql_count = psql_count.replace("%s", f"${i}", 1)
    print(psql_count)
    
    print("\n" + "="*80)
    print(f"ОКПД коды пользователя ({len(okpd_codes)}): {okpd_codes[:10]}...")
    print(f"ОКПД ID ({len(okpd_ids)}): {okpd_ids[:10]}...")
    print(f"Стоп-слова ({len(stop_words)}): {stop_words[:5]}...")
    print("="*80 + "\n")
    
    db_manager.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Вывод SQL запроса для новых торгов")
    parser.add_argument("--user-id", type=int, default=1, help="ID пользователя")
    parser.add_argument("--registry", type=str, default="44fz", choices=["44fz", "223fz"], help="Тип реестра")
    parser.add_argument("--limit", type=int, default=1000, help="Лимит записей")
    
    args = parser.parse_args()
    
    show_query(args.user_id, args.registry, args.limit)

