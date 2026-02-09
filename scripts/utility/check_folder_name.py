"""
MODULE: check_folder_name
RESPONSIBILITY: Checking folder name accuracy for specific tender 514805.
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

try:
    # Проверим folder_name для торга 514805
    query = '''
    SELECT id, tender_id, registry_type, folder_name, processed_at, is_interesting, error_reason
    FROM tender_document_matches
    WHERE tender_id = 514805
    '''

    result = db_manager.execute_query(query)

    if result:
        for row in result:
            folder_name = row.get('folder_name')
            logger.info(f'folder_name для торга 514805: "{folder_name}"')

            # Проверим, совпадает ли с ожидаемым
            expected = '44fz_514805_won'
            logger.info(f'Ожидаемое folder_name: "{expected}"')
            logger.info(f'Совпадает: {folder_name == expected}')

finally:
    db_manager.disconnect()
