"""
MODULE: check_tender_matches
RESPONSIBILITY: Initial verification and statistical analysis of tender_document_matches table.
ALLOWED: sys, config.settings, core.tender_database, loguru.
FORBIDDEN: None.
ERRORS: None.
"""
import sys
sys.path.insert(0, '.')

from config.settings import Config
from core.tender_database import TenderDatabaseManager
from loguru import logger

config = Config()
db_manager = TenderDatabaseManager(config.tender_database)
db_manager.connect()

logger.info('=== ПРОВЕРКА tender_document_matches ===')

try:
    # Посмотрим примеры данных
    query_sample = '''
    SELECT id, tender_id, registry_type, folder_name, processed_at, is_interesting
    FROM tender_document_matches
    WHERE folder_name IS NOT NULL
    LIMIT 5
    '''

    result_sample = db_manager.execute_query(query_sample)
    logger.info(f'Примеры данных ({len(result_sample)} записей):')
    for row in result_sample:
        logger.info(f'  ID: {row["id"]}, Tender: {row["tender_id"]}, Registry: {row["registry_type"]}, Folder: {row["folder_name"]}, Processed: {row["processed_at"]}, Interesting: {row["is_interesting"]}')

    # Статистика
    query_stats = '''
    SELECT
        COUNT(*) as total,
        COUNT(CASE WHEN folder_name IS NOT NULL THEN 1 END) as with_folder_name,
        COUNT(DISTINCT folder_name) as unique_folders
    FROM tender_document_matches
    '''

    result_stats = db_manager.execute_query(query_stats)
    if result_stats:
        stats = result_stats[0]
        logger.info('Статистика:')
        logger.info(f'  Всего записей: {stats["total"]}')
        logger.info(f'  С folder_name: {stats["with_folder_name"]}')
        logger.info(f'  Уникальных папок: {stats["unique_folders"]}')

except Exception as e:
    logger.error(f'Ошибка: {e}')

db_manager.disconnect()
