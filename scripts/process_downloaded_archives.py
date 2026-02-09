"""
MODULE: scripts.process_downloaded_archives
RESPONSIBILITY: Processing downloaded archives, extracting XLSX, and matching with the DB.
ALLOWED: sys, time, pathlib, typing, loguru, config.settings, core.database, core.exceptions, services.document_search_service, services.archive_processing_service.
FORBIDDEN: None.
ERRORS: None.

Боевой скрипт для обработки уже скачанных архивов из папки загрузки.

Находит все архивы в папке, объединяет многофайловые, распаковывает,
ищет XLSX файлы и сверяет с данными из БД.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import List, Dict, Any

from loguru import logger

from config.settings import config
from core.database import DatabaseManager
from core.exceptions import DocumentSearchError, DatabaseConnectionError
from services.document_search_service import DocumentSearchService
from services.archive_processing_service import (
    ArchiveProcessingService,
    find_archives_in_directory,
)


def process_archive_group(
    processor: ArchiveProcessingService,
    base_name: str,
    archive_paths: List[Path],
    download_root: Path,
) -> Dict[str, Any]:
    """
    Обрабатывает одну группу архивов (многофайловый или одиночный).
    
    Args:
        service: Сервис поиска документации
        base_name: Базовое имя архива
        archive_paths: Пути к частям архива
        
    Returns:
        Результат обработки: путь к XLSX и найденные совпадения
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"Обработка архива: {base_name}")
    logger.info(f"Количество частей: {len(archive_paths)}")
    logger.info(f"{'='*80}")
    
    start_time = time.time()
    
    try:
        result = processor.process_archive_group(base_name, archive_paths)
    except DocumentSearchError as error:
        elapsed_time = time.time() - start_time
        logger.error(f"❌ Ошибка при обработке архива {base_name}: {error}")
        return {
            "workbook_paths": [],
            "matches": [],
            "error": str(error),
            "processing_time": elapsed_time,
            "total_size": 0,
            "files_count": 0,
        }

    elapsed_time = time.time() - start_time
    workbook_paths = result.workbook_paths
    matches = result.matches
    total_size = result.total_size

    logger.info(f"\n✅ Архив успешно обработан!")
    if workbook_paths:
        logger.info("📄 Найденные XLSX файлы:")
        for path in workbook_paths:
            size_mb = path.stat().st_size / (1024 * 1024) if path.exists() else 0
            logger.info(f"  - {path.name} ({size_mb:.2f} МБ)")
    logger.info(f"🔍 Найдено совпадений с БД: {len(matches)}")
    logger.info(f"⏱️  Время обработки: {elapsed_time:.2f} сек")
    logger.info(f"📊 Размер обработанных файлов: {total_size / (1024 * 1024):.2f} МБ")

    groups = processor.group_matches_by_score(matches)
    if groups["exact"] or groups["good"]:
        logger.info("\n📋 Найденные товары:")
        counter = 0

        def log_group(title: str, items: List[Dict[str, Any]]) -> None:
            nonlocal counter
            if not items:
                return
            logger.info(f"\n{title}")
            for match in items:
                counter += 1
                display = processor.build_display_chunks(match, download_root)
                logger.info(
                    f"  {counter}. {match['product_name']} "
                    f"(совпадение: {match['score']:.1f}%)"
                )
                logger.info(f"      {display['file_info']}")
                if display["summary"]:
                    logger.info(f"      {display['summary']}")
                logger.info(f"      {display['cell_text']}")

        log_group("✅ ТОЧНЫЕ СОВПАДЕНИЯ (100%)", groups["exact"])
        log_group("🔍 ХОРОШИЕ СОВПАДЕНИЯ (85%+)", groups["good"])
    else:
        logger.warning("⚠️  Совпадений с товарами из БД не найдено")

    return {
        "workbook_paths": [str(path) for path in workbook_paths],
        "matches": matches,
        "processing_time": elapsed_time,
        "total_size": total_size,
        "files_count": len(workbook_paths),
    }


def main():
    """Основная функция обработки скачанных архивов."""
    
    # Настройка логирования в консоль
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="{time:HH:mm:ss} | {level: <8} | {message}",
        colorize=True
    )
    
    logger.info("🚀 Запуск обработки скачанных архивов")
    logger.info("="*80)
    
    overall_start_time = time.time()
    
    # Определяем директорию загрузки
    if config.document_download_dir:
        download_dir = Path(config.document_download_dir).expanduser().resolve()
    else:
        logger.error("❌ DOCUMENT_DOWNLOAD_DIR не настроен в .env файле!")
        sys.exit(1)
    
    logger.info(f"📁 Директория загрузки: {download_dir}")
    
    if not download_dir.exists():
        logger.error(f"❌ Директория не существует: {download_dir}")
        sys.exit(1)
    
    # Подключаемся к БД
    try:
        db_manager = DatabaseManager(config.database)
        db_manager.connect()
        logger.info("✅ Подключение к БД установлено")
    except DatabaseConnectionError as error:
        logger.error(f"❌ Ошибка подключения к БД: {error}")
        sys.exit(1)
    
    # Создаем сервисы
    try:
        service = DocumentSearchService(
            db_manager,
            download_dir,
            unrar_path=config.unrar_tool,
            winrar_path=config.winrar_path,
        )
        processor = ArchiveProcessingService(service)
        logger.info("✅ Сервис поиска документации инициализирован")
    except Exception as error:
        logger.error(f"❌ Ошибка инициализации сервиса: {error}")
        db_manager.close()
        sys.exit(1)
    
    # Находим все архивы
    archive_groups = find_archives_in_directory(download_dir)
    
    if not archive_groups:
        logger.warning("⚠️  Архивы не найдены в директории")
        service.db_manager.close()
        return
    
    # Обрабатываем каждую группу архивов
    results = []
    for base_name, archive_paths in archive_groups.items():
        result = process_archive_group(processor, base_name, archive_paths, download_dir)
        results.append({
            "archive_name": base_name,
            "parts_count": len(archive_paths),
            **result
        })
    
    # Итоговая статистика
    overall_elapsed_time = time.time() - overall_start_time
    
    logger.info(f"\n{'='*80}")
    logger.info("📊 ИТОГОВАЯ СТАТИСТИКА")
    logger.info(f"{'='*80}")
    
    total_archives = len(results)
    successful = sum(1 for r in results if r.get("workbook_paths"))
    total_matches = sum(len(r.get("matches", [])) for r in results)
    
    # Собираем общую статистику по файлам
    total_files_processed = sum(r.get("files_count", 0) for r in results)
    total_size_bytes = sum(r.get("total_size", 0) for r in results)
    total_processing_time = sum(r.get("processing_time", 0) for r in results)
    
    logger.info(f"📦 Всего архивов обработано: {total_archives}")
    logger.info(f"✅ Успешно распаковано: {successful}")
    logger.info(f"📄 Проанализировано файлов: {total_files_processed}")
    logger.info(f"💾 Общий размер файлов: {total_size_bytes / (1024 * 1024):.2f} МБ")
    logger.info(f"⏱️  Общее время обработки: {overall_elapsed_time:.2f} сек")
    logger.info(f"⏱️  Время на поиск (суммарно): {total_processing_time:.2f} сек")
    logger.info(f"🔍 Всего найдено совпадений: {total_matches}")
    
    if total_files_processed > 0:
        avg_time_per_file = total_processing_time / total_files_processed
        logger.info(f"⚡ Среднее время на файл: {avg_time_per_file:.2f} сек")
    
    # Закрываем соединение с БД
    service.db_manager.close()
    logger.info("\n✅ Обработка завершена")


if __name__ == "__main__":
    main()

