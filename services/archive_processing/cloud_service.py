"""
Сервис облачной загрузки файлов.
Отвечает за загрузку документов в облачные хранилища.
"""

from typing import Optional, Any
from pathlib import Path
import logging

from services.archive_runner.cloud_uploader import CloudUploader

logger = logging.getLogger(__name__)


class CloudUploadService:
    """Сервис загрузки файлов в облачное хранилище."""
    
    def __init__(self, yandex_disk: Optional[Any] = None):
        """
        Инициализация сервиса облачной загрузки.
        
        Args:
            yandex_disk: Клиент Яндекс.Диска (опционально)
        """
        self.cloud_uploader = CloudUploader(yandex_disk)
    
    def upload_to_cloud(self, folder_path: Path, tender_id: int, 
                       registry_type: str) -> bool:
        """
        Загрузка папки с документами в облачное хранилище.
        
        Args:
            folder_path: Путь к папке с документами
            tender_id: ID тендера
            registry_type: Тип реестра
            
        Returns:
            True если загрузка успешна, иначе False
        """
        try:
            return self.cloud_uploader.upload_folder(folder_path, tender_id, registry_type)
        except Exception as e:
            logger.error(f"Ошибка загрузки в облако для тендера {tender_id}: {e}")
            return False
    
    def sync_with_cloud(self, local_path: Path, cloud_path: str) -> bool:
        """
        Синхронизация локальной папки с облачным хранилищем.
        
        Args:
            local_path: Локальный путь к папке
            cloud_path: Путь в облачном хранилище
            
        Returns:
            True если синхронизация успешна
        """
        try:
            return self.cloud_uploader.sync_folder(local_path, cloud_path)
        except Exception as e:
            logger.error(f"Ошибка синхронизации с облаком: {e}")
            return False
    
    def check_cloud_connection(self) -> bool:
        """
        Проверка соединения с облачным хранилищем.
        
        Returns:
            True если соединение установлено
        """
        try:
            return self.cloud_uploader.is_connected()
        except Exception as e:
            logger.error(f"Ошибка проверки соединения с облаком: {e}")
            return False