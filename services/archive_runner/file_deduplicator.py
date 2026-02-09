"""
MODULE: services.archive_runner.file_deduplicator
RESPONSIBILITY: Deduplicate files based on name and size.
ALLOWED: pathlib, logging.
FORBIDDEN: IO operations beyond stat().
ERRORS: None.

Модуль для дедупликации файлов при подготовке к обработке.

Содержит логику добавления файлов в словарь с проверкой дубликатов.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Set, Tuple

from loguru import logger


def add_file_to_dict(
    path: Path,
    workbook_paths_dict: Dict[Tuple[str, int], Path],
    workbook_paths_set: Set[Path],
    file_type: str = "",
) -> int:
    """
    Добавляет файл в словарь с дедупликацией.
    
    Args:
        path: Путь к файлу
        workbook_paths_dict: Словарь для дедупликации
        workbook_paths_set: Множество путей
        file_type: Тип файла для логирования (Excel, PDF, Word)
        
    Returns:
        1 если файл дубликат, 0 если добавлен
    """
    try:
        file_size = path.stat().st_size
        dedup_key = (path.name, file_size)
        if dedup_key in workbook_paths_dict:
            return 1
        workbook_paths_dict[dedup_key] = path
        workbook_paths_set.add(path)
        if file_type:
            logger.info(f"{file_type} файл {path.name} добавлен для обработки")
        return 0
    except OSError as error:
        logger.warning(f"Не удалось получить размер файла {path}: {error}")
        if path not in workbook_paths_set:
            workbook_paths_set.add(path)
            dedup_key = (path.name, 0)
            workbook_paths_dict[dedup_key] = path
        return 0

