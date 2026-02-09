"""
MODULE: services.tender_repositories.tender_documents_repository
RESPONSIBILITY: Access tender documents (44FZ/223FZ).
ALLOWED: typing, loguru, core.tender_database, psycopg2.extras.
FORBIDDEN: Business logic outside DB operations.
ERRORS: Database exceptions.

Репозиторий для работы с документами тендеров.
"""

from typing import List, Dict, Any
from loguru import logger
from core.tender_database import TenderDatabaseManager
from psycopg2.extras import RealDictCursor


class TenderDocumentsRepository:
    """Репозиторий для работы с документами тендеров"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
    
    def get_tender_documents_44fz(self, contract_id: int) -> List[Dict[str, Any]]:
        """Получение ссылок на документы для торга 44ФЗ"""
        try:
            query = """
                SELECT 
                    id,
                    contract_id,
                    document_links,
                    file_name
                FROM links_documentation_44_fz
                WHERE contract_id = %s
            """
            results = self.db_manager.execute_query(query, (contract_id,), RealDictCursor)
            return [dict(row) for row in results] if results else []
        except Exception as e:
            logger.error(f"Ошибка при получении ссылок на документы 44ФЗ: {e}")
            return []
    
    def get_tender_documents_44fz_batch(self, contract_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        """Получение ссылок на документы для нескольких торгов 44ФЗ"""
        if not contract_ids:
            return {}
        
        try:
            placeholders = ','.join(['%s'] * len(contract_ids))
            query = f"""
                SELECT 
                    id,
                    contract_id,
                    document_links,
                    file_name
                FROM links_documentation_44_fz
                WHERE contract_id IN ({placeholders})
            """
            results = self.db_manager.execute_query(query, tuple(contract_ids), RealDictCursor)
            
            documents_dict = {}
            for row in results:
                contract_id = row['contract_id']
                if contract_id not in documents_dict:
                    documents_dict[contract_id] = []
                documents_dict[contract_id].append(dict(row))
            
            return documents_dict
        except Exception as e:
            logger.error(f"Ошибка при получении ссылок на документы 44ФЗ (batch): {e}")
            return {}
    
    def get_tender_documents_223fz(self, contract_id: int) -> List[Dict[str, Any]]:
        """Получение ссылок на документы для торга 223ФЗ"""
        try:
            query = """
                SELECT 
                    id,
                    contract_id,
                    document_links,
                    file_name
                FROM links_documentation_223_fz
                WHERE contract_id = %s
            """
            results = self.db_manager.execute_query(query, (contract_id,), RealDictCursor)
            return [dict(row) for row in results] if results else []
        except Exception as e:
            logger.error(f"Ошибка при получении ссылок на документы 223ФЗ: {e}")
            return []
    
    def get_tender_documents_223fz_batch(self, contract_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        """Получение ссылок на документы для нескольких торгов 223ФЗ"""
        if not contract_ids:
            return {}
        
        try:
            placeholders = ','.join(['%s'] * len(contract_ids))
            query = f"""
                SELECT 
                    id,
                    contract_id,
                    document_links,
                    file_name
                FROM links_documentation_223_fz
                WHERE contract_id IN ({placeholders})
            """
            results = self.db_manager.execute_query(query, tuple(contract_ids), RealDictCursor)
            
            documents_dict = {}
            for row in results:
                contract_id = row['contract_id']
                if contract_id not in documents_dict:
                    documents_dict[contract_id] = []
                documents_dict[contract_id].append(dict(row))
            
            return documents_dict
        except Exception as e:
            logger.error(f"Ошибка при получении ссылок на документы 223ФЗ (batch): {e}")
            return {}
    
    def get_tender_documents(self, tender_id: int, registry_type: str) -> List[Dict[str, Any]]:
        """Получение документов торга по ID и типу реестра"""
        if registry_type.lower() == "223fz":
            return self.get_tender_documents_223fz(tender_id)
        return self.get_tender_documents_44fz(tender_id)

