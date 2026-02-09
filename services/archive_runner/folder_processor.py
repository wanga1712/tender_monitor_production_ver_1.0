"""
MODULE: services.archive_runner.folder_processor
RESPONSIBILITY:
- Обработка файловых операций с папками документов
- Удаление и очистка папок после обработки
- Управление жизненным циклом файловых ресурсов
ALLOWED:
- Операции с файловой системой (Path, shutil, os)
- Логирование через loguru
- Взаимодействие с TenderFolderManager
FORBIDDEN:
- Прямые запросы к базе данных
- Бизнес-логика обработки торгов
- Управление подключениями к БД
ERRORS:
- Должен пробрасывать FileSystemError, ProcessingError
"""

import os
import shutil
from pathlib import Path
from typing import Optional

from loguru import logger
from services.archive_runner.tender_folder_manager import TenderFolderManager


class FolderProcessor:
    """Обработчик файловых операций с папками документов"""

    def __init__(self, folder_manager: TenderFolderManager):
        self.folder_manager = folder_manager

    def process_existing_folders(self, registry_type: Optional[str] = None) -> int:
        """Обработать все существующие папки с документами"""
        try:
            processed_count = 0
            folders = self.folder_manager.get_existing_folders(registry_type)
            
            for folder_path in folders:
                if self._process_single_folder(folder_path):
                    processed_count += 1
            
            return processed_count
            
        except Exception as e:
            logger.error(f"Ошибка при обработке существующих папок: {e}")
            raise

    def _process_single_folder(self, folder_path: Path) -> bool:
        """Обработать одну папку с документами"""
        try:
            # Проверяем, что папка существует и не пуста
            if not folder_path.exists() or not any(folder_path.iterdir()):
                logger.warning(f"Пропускаем пустую или несуществующую папку: {folder_path}")
                return False
            
            # Здесь будет логика обработки конкретной папки
            logger.info(f"Обрабатываем папку: {folder_path}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при обработке папки {folder_path}: {e}")
            return False

    def delete_folder_after_processing(self, folder_path: Path, 
                                    tender_id: Optional[int] = None, 
                                    registry_type: Optional[str] = None) -> None:
        """Безопасно удалить папку после обработки"""
        try:
            if folder_path.exists():
                shutil.rmtree(folder_path)
                logger.info(f"Папка удалена: {folder_path}")
            
        except Exception as e:
            logger.error(f"Ошибка при удалении папки {folder_path}: {e}")

    def delete_folder_and_schedule_download(self, folder_path: Path, 
                                         tender_id: int, 
                                         registry_type: str) -> None:
        """Удалить папку и запланировать повторное скачивание"""
        try:
            self.delete_folder_after_processing(folder_path, tender_id, registry_type)
            # Здесь можно добавить логику планирования повторного скачивания
            logger.info(f"Запланировано повторное скачивание для tender_id={tender_id}")
            
        except Exception as e:
            logger.error(f"Ошибка при планировании повторного скачивания: {e}")