"""
Сервис для работы с документами тендеров.

Отвечает за:
- Получение документов тендеров
- Работа с информацией о тендерах
- Прикрепление документов к данным тендеров
"""

from typing import List, Dict, Any, Optional
from loguru import logger

from core.tender_database import TenderDatabaseManager
from services.tender_repositories.tender_documents_repository import TenderDocumentsRepository
from services.tender_repositories.tender_query_builder import TenderQueryBuilder


class DocumentService:
    """Сервис для работы с документами тендеров."""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
        self.documents_repo = TenderDocumentsRepository(db_manager)
        self.query_builder = TenderQueryBuilder()
    
    def get_tender_documents(self, tender_id: int, registry_type: str) -> List[Dict[str, Any]]:
        """Получение документов тендера."""
        return self.documents_repo.get_tender_documents(tender_id, registry_type)
    
    def get_tenders_by_ids(self, tender_ids_44fz: Optional[List[int]] = None, 
                         tender_ids_223fz: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """Получение тендеров по IDs."""
        return self.query_builder.get_tenders_by_ids(tender_ids_44fz, tender_ids_223fz)
    
    def _attach_documents(self, tenders: List[Dict[str, Any]]) -> None:
        """Прикрепление документов к данным тендеров."""
        for tender in tenders:
            tender_id = tender.get('id')
            registry_type = tender.get('registry_type')
            if tender_id and registry_type:
                documents = self.get_tender_documents(tender_id, registry_type)
                tender['documents'] = documents
    
    def _fetch_registry_records(self, registry_type: str, tender_ids: List[int]) -> List[Dict[str, Any]]:
        """Получение записей из реестра по IDs."""
        if registry_type == '44fz':
            return self.query_builder.get_44fz_tenders_by_ids(tender_ids)
        elif registry_type == '223fz':
            return self.query_builder.get_223fz_tenders_by_ids(tender_ids)
        else:
            return []
