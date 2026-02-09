#!/usr/bin/env python3
"""
MODULE: scripts.cleanup_processed_folders
RESPONSIBILITY: Cleaning up folders for processed tenders.
ALLOWED: sys, pathlib, typing, loguru, config.settings, core.tender_database, services.archive_runner.processed_tenders_repository, shutil, argparse.
FORBIDDEN: None.
ERRORS: None.

Скрипт для разовой очистки папок уже обработанных торгов.

Проходит по записям в processed_tenders и удаляет соответствующие папки с диска,
если они еще существуют.
"""

import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

# Добавляем корневую директорию в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from config.settings import Config
from core.tender_database import TenderDatabaseManager
from services.archive_runner.processed_tenders_repository import ProcessedTendersRepository


def get_download_dirs() -> List[Path]:
    """Получает список возможных директорий для скачивания файлов"""
    config = Config()

    dirs = []

    # Основная директория из конфига
    if hasattr(config, 'document_download_dir') and config.document_download_dir:
        dirs.append(Path(config.document_download_dir))

    # Дефолтная директория
    dirs.append(Path.home() / "Downloads" / "ЕИС_Документация")

    # Удаляем дубликаты
    unique_dirs = []
    for d in dirs:
        if d not in unique_dirs:
            unique_dirs.append(d)

    return unique_dirs


def find_tender_folder(download_dirs: List[Path], registry_type: str, tender_id: int, folder_name: str) -> Optional[Path]:
    """Ищет папку торга во всех директориях скачивания"""
    for base_dir in download_dirs:
        # Ищем папку с указанным именем
        folder_path = base_dir / folder_name
        if folder_path.exists() and folder_path.is_dir():
            return folder_path

        # Ищем папку по паттерну registry_tender_type
        for tender_type in ['new', 'won', 'commission']:
            pattern_folder = base_dir / f"{registry_type}_{tender_id}_{tender_type}"
            if pattern_folder.exists() and pattern_folder.is_dir():
                return pattern_folder

    return None


def cleanup_processed_folders(dry_run: bool = True) -> Dict[str, int]:
    """
    Очищает папки уже обработанных торгов.

    Args:
        dry_run: Если True, только показывает что будет удалено

    Returns:
        Статистика выполнения
    """
    logger.info(f"🚀 Начинаем очистку обработанных папок (dry_run={dry_run})")

    config = Config()
    db_manager = TenderDatabaseManager(config.tender_database)
    db_manager.connect()

    try:
        processed_repo = ProcessedTendersRepository(db_manager)
        download_dirs = get_download_dirs()

        logger.info(f"Директории для поиска: {[str(d) for d in download_dirs]}")

        # Получаем все обработанные торги из tender_document_matches
        query = """
            SELECT tender_id, registry_type, folder_name, processed_at
            FROM tender_document_matches
            WHERE folder_name IS NOT NULL
            ORDER BY processed_at DESC
        """

        results = db_manager.execute_query(query)
        if not results:
            logger.info("Нет обработанных торгов для очистки")
            return {'total_processed': 0, 'folders_found': 0, 'folders_deleted': 0}

        logger.info(f"Найдено {len(results)} обработанных торгов")

        stats = {
            'total_processed': len(results),
            'folders_found': 0,
            'folders_deleted': 0,
            'errors': 0
        }

        for row in results:
            tender_id = row['tender_id']
            registry_type = row['registry_type']
            folder_name = row['folder_name']

            # Ищем папку
            folder_path = find_tender_folder(download_dirs, registry_type, tender_id, folder_name)

            if folder_path:
                stats['folders_found'] += 1
                logger.info(f"📁 Найдена папка для торга {tender_id} ({registry_type}): {folder_path}")

                if not dry_run:
                    try:
                        # Удаляем папку
                        import shutil
                        shutil.rmtree(folder_path)
                        stats['folders_deleted'] += 1
                        logger.info(f"✅ Удалена папка: {folder_path}")
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"❌ Ошибка удаления папки {folder_path}: {e}")
            else:
                logger.debug(f"📁 Папка для торга {tender_id} ({registry_type}) не найдена: {folder_name}")

        logger.info("📊 Статистика очистки:")
        logger.info(f"  Обработанных торгов: {stats['total_processed']}")
        logger.info(f"  Папок найдено: {stats['folders_found']}")
        logger.info(f"  Папок удалено: {stats['folders_deleted']}")
        logger.info(f"  Ошибок: {stats['errors']}")

        if dry_run:
            logger.info("🔍 Это был тестовый запуск (dry_run=True). Для реального удаления запустите с dry_run=False")

        return stats

    finally:
        db_manager.disconnect()


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Очистка папок обработанных торгов')
    parser.add_argument('--dry-run', action='store_true', default=True,
                       help='Тестовый режим (по умолчанию True)')
    parser.add_argument('--no-dry-run', action='store_true',
                       help='Реальный режим удаления')

    args = parser.parse_args()

    # Если указан --no-dry-run, отключаем dry_run
    if args.no_dry_run:
        args.dry_run = False

    try:
        stats = cleanup_processed_folders(dry_run=args.dry_run)

        if args.dry_run:
            logger.info("\n🔄 Для реального удаления запустите:")
            logger.info("python scripts/cleanup_processed_folders.py --no-dry-run")

    except Exception as e:
        logger.error(f"Ошибка выполнения: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
