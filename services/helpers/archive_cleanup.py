"""
MODULE: services.helpers.archive_cleanup
RESPONSIBILITY: Cleanup of temporary archives and extracted files.
ALLOWED: pathlib, shutil, loguru.
FORBIDDEN: Deleting non-temporary files.
ERRORS: None.

Вспомогательные функции для очистки архивов и временных директорий.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Sequence, Dict, Any
import shutil

from loguru import logger


class ArchiveCleanupManager:
    """
    Удаляет скачанные архивы и распакованные директории, если не найдено
    значимых совпадений.
    """

    def __init__(self, preserve_threshold: float = 85.0):
        """
        Args:
            preserve_threshold: Порог точности совпадения, при котором
                файлы сохраняются (например, 85% и 100%).
        """
        self.preserve_threshold = preserve_threshold

    def cleanup(
        self,
        downloaded_files: Sequence[Path],
        extract_dirs: Sequence[Path],
        matches: Sequence[Dict[str, Any]],
    ) -> None:
        """
        Удаляет архивы и распакованные директории, если не найдено точных
        (100%) или высокоточных (>= threshold) совпадений.
        """
        if self._should_preserve(matches):
            logger.info(
                "Найдены совпадения %.0f%%+, временные файлы сохранены",
                self.preserve_threshold,
            )
            return

        for file_path in downloaded_files:
            self._remove_file(Path(file_path))
        for dir_path in extract_dirs:
            self._remove_dir(Path(dir_path))

    def _should_preserve(self, matches: Sequence[Dict[str, Any]]) -> bool:
        """Проверяет наличие совпадений выше порога."""
        for match in matches:
            if match.get("score", 0) >= self.preserve_threshold:
                return True
        return False

    def _remove_file(self, path: Path) -> None:
        if path.exists() and path.is_file():
            try:
                path.unlink()
                logger.debug(f"Удален файл: {path}")
            except OSError as error:
                logger.warning(f"Не удалось удалить файл {path}: {error}")

    def _remove_dir(self, path: Path) -> None:
        if path.exists() and path.is_dir():
            try:
                shutil.rmtree(path, ignore_errors=True)
                logger.debug(f"Удалена директория: {path}")
            except OSError as error:
                logger.warning(f"Не удалось удалить директорию {path}: {error}")

