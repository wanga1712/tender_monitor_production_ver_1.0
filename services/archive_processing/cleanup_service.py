"""
Сервис очистки и управления файлами.
Отвечает за удаление временных файлов и оптимизацию хранилища.
"""

from typing import List, Optional
from pathlib import Path
import logging

from services.archive_runner.file_cleaner import FileCleaner
from services.archive_runner.existing_files_processor import ExistingFilesProcessor

logger = logging.getLogger(__name__)


class FileCleanupService:
    """Сервис очистки и управления файлами."""
    
    def __init__(self, download_dir: Path):
        """
        Инициализация сервиса очистки.
        
        Args:
            download_dir: Основная директория для скачивания файлов
        """
        self.download_dir = download_dir
        self.file_cleaner = FileCleaner()
        self.existing_processor = ExistingFilesProcessor(download_dir)
    
    def cleanup_temporary_files(self, folder_path: Optional[Path] = None):
        """
        Очистка временных файлов в указанной или основной директории.
        
        Args:
            folder_path: Путь к папке для очистки (опционально)
        """
        target_dir = folder_path or self.download_dir
        try:
            self.file_cleaner.cleanup_folder(target_dir)
            logger.info(f"Очищены временные файлы в {target_dir}")
        except Exception as e:
            logger.error(f"Ошибка очистки временных файлов: {e}")
    
    def list_pending_tenders(self) -> List[dict]:
        """
        Получение списка тендеров с ожидающими обработки файлами.
        
        Returns:
            Список информации о тендерах с файлами
        """
        try:
            return self.existing_processor.list_pending_tenders()
        except Exception as e:
            logger.error(f"Ошибка получения списка ожидающих тендеров: {e}")
            return []
    
    def remove_processed_files(self, tender_id: int, registry_type: str):
        """
        Удаление обработанных файлов для конкретного тендера.
        
        Args:
            tender_id: ID тендера
            registry_type: Тип реестра
        """
        try:
            tender_folder = self.download_dir / f"{registry_type}_{tender_id}"
            if tender_folder.exists():
                self.file_cleaner.cleanup_folder(tender_folder)
                logger.info(f"Удалены файлы тендера {tender_id} ({registry_type})")
        except Exception as e:
            logger.error(f"Ошибка удаления файлов тендера {tender_id}: {e}")
    
    def optimize_storage(self):
        """Оптимизация хранилища путем удаления старых файлов."""
        try:
            # Удаляем файлы старше 7 дней
            self.file_cleaner.remove_old_files(self.download_dir, days=7)
            logger.info("Оптимизация хранилища завершена")
        except Exception as e:
            logger.error(f"Ошибка оптимизации хранилища: {e}")