"""
Сервис для скачивания документов тендеров.
Отвечает за загрузку архивов и документов из EIS.
"""

from typing import List, Dict, Any
from pathlib import Path
import logging

from services.document_search.document_downloader import DocumentDownloader
from services.archive_runner.document_download_manager import DocumentDownloadManager

logger = logging.getLogger(__name__)


class TenderDownloadService:
    """Сервис скачивания документов тендеров."""
    
    def __init__(self, download_dir: Path, timeout_calculator: Any = None):
        """
        Инициализация сервиса скачивания.
        
        Args:
            download_dir: Директория для сохранения файлов
            timeout_calculator: Калькулятор таймаутов для скачивания
        """
        self.download_dir = download_dir
        self.downloader = DocumentDownloader(download_dir, timeout_calculator=timeout_calculator)
        self.download_manager = DocumentDownloadManager(download_dir)
    
    def download_tender_documents(self, tender_id: int, registry_type: str) -> List[Dict[str, Any]]:
        """
        Скачивание документов для конкретного тендера.
        
        Args:
            tender_id: ID тендера
            registry_type: Тип реестра ('44fz' или '223fz')
            
        Returns:
            Список скачанных документов
        """
        try:
            return self.downloader.download_tender_documents(tender_id, registry_type)
        except Exception as e:
            logger.error(f"Ошибка скачивания документов для тендера {tender_id}: {e}")
            raise
    
    def get_document_info(self, tender_id: int, registry_type: str) -> List[Dict[str, Any]]:
        """
        Получение информации о документах тендера.
        
        Args:
            tender_id: ID тендера
            registry_type: Тип реестра
            
        Returns:
            Информация о документах
        """
        return self.download_manager.get_tender_documents(tender_id, registry_type)
    
    def cleanup_downloads(self):
        """Очистка временных файлов скачивания."""
        self.download_manager.cleanup()