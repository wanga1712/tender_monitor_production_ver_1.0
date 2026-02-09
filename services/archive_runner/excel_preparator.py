"""
MODULE: services.archive_runner.excel_preparator
RESPONSIBILITY: Prepare Excel files for processing (copy, verify).
ALLOWED: shutil, pathlib, ExcelFileTester, logging.
FORBIDDEN: Parsing logic.
ERRORS: None.

Модуль для подготовки Excel файлов к обработке.

Содержит логику копирования и проверки Excel файлов.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from typing import Optional

from loguru import logger

from services.document_search.excel_file_tester import ExcelFileTester


class ExcelPreparator:
    """Класс для подготовки Excel файлов к обработке."""
    
    def __init__(self):
        """Инициализация подготовщика Excel файлов."""
        self._excel_tester = ExcelFileTester(max_cells=50)
    
    def prepare_excel_file(self, source_path: Path, tender_folder: Path) -> Optional[Path]:
        """
        Копируем Excel файл в рабочую папку и убеждаемся, что он читается ExcelParser.
        
        Args:
            source_path: Исходный путь к Excel файлу
            tender_folder: Папка тендера для кэширования
            
        Returns:
            Путь к подготовленному файлу или None если не удалось подготовить
        """
        try:
            cache_dir = tender_folder / "excel_cache"
            cache_dir.mkdir(parents=True, exist_ok=True)

            target_name = self._build_unique_excel_name(cache_dir, source_path.name)
            target_path = cache_dir / target_name
            shutil.copy2(source_path, target_path)
            logger.debug(f"Excel файл {source_path.name} скопирован в {target_path}")

            if self._excel_tester.verify(target_path):
                return target_path

            logger.warning(
                f"Файл {source_path.name} не прошел проверку после копирования, удаляем поврежденный файл"
            )
            # Удаляем поврежденный файл
            self._remove_file_force(target_path)
            # Удаляем исходный поврежденный файл для повторного скачивания
            if source_path.exists():
                logger.info(f"Удаление поврежденного исходного файла {source_path.name} для повторного скачивания")
                self._remove_file_force(source_path)
            return None
        except Exception as error:
            logger.error(f"Не удалось подготовить Excel файл {source_path.name}: {error}")
            return None
    
    @staticmethod
    def _build_unique_excel_name(cache_dir: Path, original_name: str) -> str:
        """
        Создает уникальное имя для Excel файла в кэше.
        
        Args:
            cache_dir: Директория кэша
            original_name: Оригинальное имя файла
            
        Returns:
            Уникальное имя файла
        """
        target_path = cache_dir / original_name
        if not target_path.exists():
            return original_name
        stem = target_path.stem
        suffix = target_path.suffix
        return f"{stem}_{uuid.uuid4().hex[:6]}{suffix}"
    
    @staticmethod
    def _remove_file_force(path: Path) -> None:
        """
        Принудительное удаление файла.
        
        Args:
            path: Путь к файлу для удаления
        """
        try:
            if path.exists():
                path.unlink()
        except Exception as error:
            logger.debug(f"Не удалось удалить файл {path}: {error}")

