"""
MODULE: scripts.archive_debug
RESPONSIBILITY: Debugging archive extraction and search functionality.
ALLOWED: argparse, sys, pathlib, typing, loguru, config.settings, core.database, core.exceptions, services.document_search_service.
FORBIDDEN: None.
ERRORS: None.

Утилита для тестирования распаковки архивов и поиска внутри XLSX.

Запускается отдельно от GUI, чтобы быстрее диагностировать проблемы
с разархивированием и поиском.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

from loguru import logger

from config.settings import config
from core.database import DatabaseManager
from core.exceptions import DocumentSearchError, DatabaseConnectionError
from services.document_search_service import DocumentSearchService


def _build_document_service(download_dir: Path) -> DocumentSearchService:
    """Создает сервис поиска документации для тестового запуска."""
    db_manager = DatabaseManager(config.database)
    db_manager.connect()

    try:
        service = DocumentSearchService(
            db_manager,
            download_dir,
            unrar_path=config.unrar_tool,
            winrar_path=config.winrar_path,
        )
    except Exception:
        db_manager.close()
        raise

    return service


def run_debug_mode(file_paths: List[str], download_dir: Path) -> None:
    """Запускает тестовый сценарий распаковки и поиска."""
    service: DocumentSearchService | None = None
    try:
        service = _build_document_service(download_dir)
        result = service.debug_process_local_archives(file_paths)

        workbook_path = result["file_path"]
        matches = result["matches"]

        logger.info("Итоговый XLSX файл: %s", workbook_path)
        logger.info("Найдено совпадений: %s", len(matches))

        for idx, match in enumerate(matches, start=1):
            logger.info("  %s. %s (score=%s)", idx, match["product_name"], match["score"])
    except (DocumentSearchError, DatabaseConnectionError) as error:
        logger.error("Тест завершился с ошибкой: %s", error)
        sys.exit(1)
    finally:
        if service and service.db_manager.connection:
            service.db_manager.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Тест распаковки архивов и поиска по XLSX без запуска всего приложения.",
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="Пути к архивам (part1.rar, part2.rar, ...) или XLSX файлам.",
    )
    parser.add_argument(
        "--download-dir",
        dest="download_dir",
        default=None,
        help="Каталог для временных файлов (по умолчанию DOCUMENT_DOWNLOAD_DIR или ./downloads).",
    )
    parser.add_argument(
        "--log-level",
        dest="log_level",
        default="INFO",
        help="Уровень логирования Loguru (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.remove()
    logger.add(sys.stderr, level=args.log_level.upper(), format="{time:HH:mm:ss} | {level} | {message}")

    download_root = (
        Path(args.download_dir).expanduser()
        if args.download_dir
        else Path(config.document_download_dir).expanduser()
        if config.document_download_dir
        else Path.cwd() / "downloads"
    )
    download_root.mkdir(parents=True, exist_ok=True)

    run_debug_mode(args.paths, download_root)


if __name__ == "__main__":
    main()

