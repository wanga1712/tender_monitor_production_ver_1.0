"""
MODULE: services.archive_runner.existing_files_processor
RESPONSIBILITY: Scan and register existing files in tender directories.
ALLOWED: pathlib, regex, logging.
FORBIDDEN: Modifying files (read-only scan).
ERRORS: None.

Модуль для обработки уже существующих файлов в директории проектов.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import re

from loguru import logger


class ExistingFilesProcessor:
    """Обработчик ранее скачанных файлов в директории проектов."""

    FOLDER_PATTERN = re.compile(r"^(?P<registry>44fz|223fz)_(?P<tender_id>\d+)(?:_won)?$", re.IGNORECASE)
    ARCHIVE_EXTENSIONS = {".rar", ".zip", ".7z"}
    EXCEL_EXTENSIONS = {".xlsx", ".xls"}

    def __init__(self, download_dir: Path):
        self.download_dir = Path(download_dir)

    def list_pending_tenders(self) -> List[Dict[str, Path]]:
        """
        Возвращает список директорий с проектами, в которых есть файлы для обработки.
        """
        pending: List[Dict[str, Path]] = []
        if not self.download_dir.exists():
            return pending

        for entry in self.download_dir.iterdir():
            if not entry.is_dir():
                continue
            match = self.FOLDER_PATTERN.match(entry.name)
            if not match:
                continue
            tender_id = int(match.group("tender_id"))
            registry_type = match.group("registry").lower()
            # Определяем тип торгов из имени папки
            tender_type = 'won' if entry.name.endswith('_won') else 'new'
            if self._folder_contains_documents(entry):
                pending.append(
                    {
                        "folder_path": entry,
                        "tender_id": tender_id,
                        "registry_type": registry_type,
                        "tender_type": tender_type,
                    }
                )
        logger.info(f"Найдено директорий с существующими файлами: {len(pending)}")
        return pending

    def build_records(self, folder: Path) -> List[Dict[str, List[Path]]]:
        """
        Собирает список записей (архивы/Excel) из существующей директории.
        Защита от MemoryError при обработке файлов с очень длинными именами.
        """
        records: List[Dict[str, List[Path]]] = []
        if not folder.exists():
            return records

        try:
            for file_path in folder.rglob("*"):
                try:
                    if not file_path.is_file():
                        continue
                    
                    # Защита от MemoryError при обработке длинных путей
                    try:
                        # Пытаемся получить имя файла безопасным способом
                        file_name = str(file_path.name)
                        # Если имя слишком длинное (> 250 символов), пропускаем
                        if len(file_name) > 250:
                            logger.debug(f"Пропуск файла с очень длинным именем (> 250 символов): {file_name[:50]}...")
                            continue
                        
                        if file_name.startswith("~$"):
                            continue
                        
                        suffix = file_path.suffix.lower()
                    except (MemoryError, OSError, ValueError) as path_error:
                        # Ошибка при работе с путем (слишком длинный путь или другие проблемы)
                        logger.warning(f"Ошибка при обработке пути файла: {path_error}, пропускаем")
                        continue
                    
                    if suffix in self.ARCHIVE_EXTENSIONS or suffix in self.EXCEL_EXTENSIONS:
                        records.append(
                            {
                                "doc": None,
                                "paths": [file_path],
                                "source": "existing",
                                "retries": 0,
                            }
                        )
                except Exception as file_error:
                    # Пропускаем файлы, которые не удалось обработать
                    logger.debug(f"Ошибка при проверке файла: {file_error}, пропускаем")
                    continue
        except Exception as folder_error:
            logger.warning(f"Ошибка при сканировании папки {folder.name}: {folder_error}")
        
        logger.debug(f"В папке {folder.name} найдено файлов для повторной обработки: {len(records)}")
        return records

    def _folder_contains_documents(self, folder: Path) -> bool:
        """
        Проверяет, есть ли в папке документы с поддерживаемыми расширениями.
        Защита от MemoryError при обработке файлов с очень длинными именами.
        """
        try:
            for file_path in folder.rglob("*"):
                try:
                    if not file_path.is_file():
                        continue
                    
                    # Защита от MemoryError при обработке длинных путей
                    try:
                        # Пытаемся получить имя файла безопасным способом
                        file_name = str(file_path.name)
                        # Если имя слишком длинное (> 250 символов), пропускаем
                        if len(file_name) > 250:
                            logger.debug(f"Пропуск файла с очень длинным именем (> 250 символов): {file_name[:50]}...")
                            continue
                        
                        suffix = file_path.suffix.lower()
                    except (MemoryError, OSError, ValueError) as path_error:
                        # Ошибка при работе с путем (слишком длинный путь или другие проблемы)
                        logger.warning(f"Ошибка при обработке пути файла: {path_error}, пропускаем")
                        continue
                    
                    if suffix in self.ARCHIVE_EXTENSIONS or suffix in self.EXCEL_EXTENSIONS:
                        return True
                except Exception as file_error:
                    # Пропускаем файлы, которые не удалось обработать
                    logger.debug(f"Ошибка при проверке файла: {file_error}, пропускаем")
                    continue
        except Exception as folder_error:
            logger.warning(f"Ошибка при сканировании папки {folder.name}: {folder_error}")
        return False

