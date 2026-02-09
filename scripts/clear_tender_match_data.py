"""
MODULE: scripts.clear_tender_match_data
RESPONSIBILITY: Clearing tender match data tables.
ALLOWED: sys, pathlib, loguru, config.settings, core.tender_database, core.exceptions, argparse.
FORBIDDEN: None.
ERRORS: None.

Скрипт для очистки данных результатов поиска совпадений в документации торгов.

Удаляет данные из таблиц:
- tender_document_match_details (детальные совпадения)
- tender_document_matches (основные результаты поиска)

ВАЖНО: Удаление происходит каскадно - сначала удаляются детали (tender_document_match_details),
затем основные записи (tender_document_matches).
"""

import sys
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from loguru import logger
from config.settings import config
from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseConnectionError


def get_table_counts(db_manager: TenderDatabaseManager) -> dict:
    """Получает количество записей в таблицах."""
    counts = {}
    
    try:
        # Количество записей в tender_document_matches
        query = "SELECT COUNT(*) as count FROM tender_document_matches"
        result = db_manager.execute_query(query)
        counts['matches'] = result[0]['count'] if result else 0
        
        # Количество записей в tender_document_match_details
        query = "SELECT COUNT(*) as count FROM tender_document_match_details"
        result = db_manager.execute_query(query)
        counts['details'] = result[0]['count'] if result else 0
        
        # Статистика по типам реестров
        query = """
            SELECT registry_type, COUNT(*) as count 
            FROM tender_document_matches 
            GROUP BY registry_type
        """
        result = db_manager.execute_query(query)
        counts['by_registry'] = {row['registry_type']: row['count'] for row in result} if result else {}
        
        # Статистика по процентам совпадений
        query = """
            SELECT 
                CASE 
                    WHEN match_percentage = 100.0 THEN '100%'
                    WHEN match_percentage >= 85.0 THEN '85-99%'
                    ELSE '0-84%'
                END as match_range,
                COUNT(*) as count
            FROM tender_document_matches
            GROUP BY match_range
        """
        result = db_manager.execute_query(query)
        counts['by_percentage'] = {row['match_range']: row['count'] for row in result} if result else {}
        
    except Exception as error:
        logger.error(f"Ошибка при получении статистики: {error}")
        return counts
    
    return counts


def show_statistics(db_manager: TenderDatabaseManager):
    """Показывает статистику данных перед удалением."""
    logger.info("=" * 80)
    logger.info("СТАТИСТИКА ДАННЫХ ПЕРЕД УДАЛЕНИЕМ")
    logger.info("=" * 80)
    
    counts = get_table_counts(db_manager)
    
    logger.info(f"\n📊 Основные результаты поиска (tender_document_matches):")
    logger.info(f"   Всего записей: {counts.get('matches', 0)}")
    
    if counts.get('by_registry'):
        logger.info(f"   По типам реестров:")
        for registry_type, count in counts['by_registry'].items():
            logger.info(f"     - {registry_type}: {count} записей")
    
    if counts.get('by_percentage'):
        logger.info(f"   По процентам совпадений:")
        for match_range, count in counts['by_percentage'].items():
            logger.info(f"     - {match_range}: {count} записей")
    
    logger.info(f"\n📋 Детальные совпадения (tender_document_match_details):")
    logger.info(f"   Всего записей: {counts.get('details', 0)}")
    
    logger.info("\n" + "=" * 80)
    logger.info("⚠️  ВНИМАНИЕ: Будут удалены ВСЕ данные из обеих таблиц!")
    logger.info("=" * 80)
    
    return counts


def clear_data(db_manager: TenderDatabaseManager, confirm: bool = False):
    """Очищает данные из таблиц."""
    if not confirm:
        logger.error("❌ Очистка не подтверждена. Используйте --confirm для подтверждения.")
        return False
    
    try:
        logger.info("\n🗑️  Начинаю очистку данных...")
        
        # Сначала удаляем детали (из-за CASCADE это не обязательно, но для ясности)
        logger.info("Удаление детальных совпадений (tender_document_match_details)...")
        query = "DELETE FROM tender_document_match_details"
        db_manager.execute_query(query)
        logger.info("✅ Детальные совпадения удалены")
        
        # Затем удаляем основные записи
        logger.info("Удаление основных результатов (tender_document_matches)...")
        query = "DELETE FROM tender_document_matches"
        db_manager.execute_query(query)
        logger.info("✅ Основные результаты удалены")
        
        # Проверяем результат
        counts = get_table_counts(db_manager)
        if counts.get('matches', 0) == 0 and counts.get('details', 0) == 0:
            logger.info("\n✅ Все данные успешно удалены!")
            return True
        else:
            logger.warning(f"\n⚠️  Остались данные: matches={counts.get('matches', 0)}, details={counts.get('details', 0)}")
            return False
            
    except Exception as error:
        logger.error(f"❌ Ошибка при очистке данных: {error}")
        return False


def main():
    """Основная функция."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Очистка данных результатов поиска совпадений')
    parser.add_argument(
        '--confirm',
        action='store_true',
        help='Подтвердить удаление данных (без этого флага данные не будут удалены)'
    )
    parser.add_argument(
        '--stats-only',
        action='store_true',
        help='Только показать статистику, без удаления'
    )
    
    args = parser.parse_args()
    
    # Подключаемся к БД tender_monitor
    try:
        tender_db = TenderDatabaseManager(config.tender_database)
        tender_db.connect()
        logger.info("✅ Подключение к БД tender_monitor установлено")
    except DatabaseConnectionError as error:
        logger.error(f"❌ Ошибка подключения к БД: {error}")
        sys.exit(1)
    
    try:
        # Показываем статистику
        counts = show_statistics(tender_db)
        
        # Если только статистика - выходим
        if args.stats_only:
            logger.info("\n📊 Режим просмотра статистики. Данные не удалены.")
            return
        
        # Если не подтверждено - выходим
        if not args.confirm:
            logger.info("\n💡 Для удаления данных запустите скрипт с флагом --confirm:")
            logger.info("   python scripts/clear_tender_match_data.py --confirm")
            return
        
        # Очищаем данные
        success = clear_data(tender_db, confirm=True)
        
        if success:
            logger.info("\n✅ Очистка завершена успешно!")
        else:
            logger.error("\n❌ Очистка завершена с ошибками")
            sys.exit(1)
        
    except Exception as error:
        logger.error(f"❌ Критическая ошибка: {error}")
        sys.exit(1)
    finally:
        tender_db.disconnect()
        logger.info("Соединение с БД закрыто")


if __name__ == "__main__":
    # Настройка логирования
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="{time:HH:mm:ss} | {level: <8} | {message}",
        colorize=True
    )
    
    main()

