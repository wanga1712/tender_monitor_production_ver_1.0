"""
Сервис для управления директориями торгов.

Отвечает за:
- Создание рабочих директорий для файлов торгов
- Генерацию имен директорий
- Управление структурой папок
"""

from pathlib import Path
from typing import Optional


class TenderFolderService:
    """Сервис управления директориями торгов."""

    def __init__(self, base_download_dir: Path):
        """
        Инициализация сервиса директорий.
        
        Args:
            base_download_dir: Базовая директория для загрузок
        """
        self.base_download_dir = Path(base_download_dir)
        self.base_download_dir.mkdir(parents=True, exist_ok=True)

    def prepare_tender_folder(self, tender_id: Optional[int], 
                             registry_type: Optional[str]) -> Path:
        """
        Создает рабочую директорию для файлов торга.
        
        Args:
            tender_id: ID торга
            registry_type: Тип реестра (44fz/223fz)
        
        Returns:
            Путь к директории торга
        """
        if not tender_id:
            # Используем временную директорию если tender_id не указан
            fallback_dir = self.base_download_dir / "tender_temp"
            fallback_dir.mkdir(parents=True, exist_ok=True)
            return fallback_dir

        # Создаем безопасное имя директории
        safe_type = (registry_type or "tender").strip().lower() or "tender"
        folder_name = f"{safe_type}_{tender_id}"
        target_dir = self.base_download_dir / folder_name
        target_dir.mkdir(parents=True, exist_ok=True)
        
        return target_dir

    def ensure_directory_exists(self, directory: Path) -> None:
        """
        Гарантирует, что директория существует.
        
        Args:
            directory: Путь к директории
        """
        directory.mkdir(parents=True, exist_ok=True)

    def get_temp_folder(self) -> Path:
        """
        Возвращает путь к временной директории.
        
        Returns:
            Путь к временной директории
        """
        temp_dir = self.base_download_dir / "temp"
        self.ensure_directory_exists(temp_dir)
        return temp_dir