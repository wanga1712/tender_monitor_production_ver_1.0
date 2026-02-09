"""
MODULE: check_514805
RESPONSIBILITY: Checking processed data for specific tender 514805.
ALLOWED: sys, config.settings, core.tender_database, loguru, services.archive_runner.processed_tenders_repository.
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

try:
    # Проверим, есть ли торг 514805 в tender_document_matches
    query = '''
    SELECT id, tender_id, registry_type, folder_name, processed_at, is_interesting, error_reason
    FROM tender_document_matches
    WHERE tender_id = 514805
    '''

    result = db_manager.execute_query(query)
    logger.info(f'Записи для торга 514805: {len(result) if result else 0}')

    if result:
        for row in result:
            processed_at = row.get('processed_at')
            is_interesting = row.get('is_interesting')
            error_reason = row.get('error_reason')
            logger.info(f'ID: {row["id"]}, Processed: {processed_at}, Interesting: {is_interesting}, Error: {error_reason}')

    # Также проверим, обрабатывается ли она сейчас
    from services.archive_runner.processed_tenders_repository import ProcessedTendersRepository
    repo = ProcessedTendersRepository(db_manager)

    is_processed = repo.is_tender_processed(514805, '44fz', '44fz_514805_won')
    logger.info(f'Торг 514805 обработана по данным ProcessedTendersRepository: {is_processed}')

finally:
    db_manager.disconnect()
