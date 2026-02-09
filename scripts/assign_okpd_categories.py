"""
MODULE: scripts.assign_okpd_categories
RESPONSIBILITY: Mass assignment of OKPD categories based on rules.
ALLOWED: sys, pathlib, loguru, config.settings, core.tender_database, core.exceptions.
FORBIDDEN: None.
ERRORS: None.

Скрипт для массового присвоения категорий существующим ОКПД кодам пользователя.

Правила присвоения:
- id от 2 до 30 включительно → категория "Проектирование"
- id 391 и с 31 по 155 → категория "Стройка"
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


def create_default_categories(db_manager: TenderDatabaseManager, user_id: int) -> dict:
    """
    Создает категории по умолчанию и возвращает словарь {название: id}
    """
    categories = {}
    
    category_names = ["Проектирование", "Стройка", "Компьютеры"]
    
    for category_name in category_names:
        try:
            # Проверяем, существует ли категория
            check_query = """
                SELECT id FROM okpd_categories
                WHERE user_id = %s AND name = %s
            """
            existing = db_manager.execute_query(check_query, (user_id, category_name))
            
            if existing:
                category_id = existing[0].get('id')
                logger.info(f"Категория '{category_name}' уже существует (id={category_id})")
            else:
                # Создаем категорию
                insert_query = """
                    INSERT INTO okpd_categories (user_id, name)
                    VALUES (%s, %s)
                    RETURNING id
                """
                try:
                    result = db_manager.execute_query(insert_query, (user_id, category_name))
                    if result:
                        category_id = result[0].get('id')
                        logger.info(f"Создана категория '{category_name}' (id={category_id})")
                    else:
                        logger.error(f"Не удалось создать категорию '{category_name}' - запрос не вернул результат")
                        continue
                except Exception as insert_error:
                    logger.error(f"Ошибка при создании категории '{category_name}': {insert_error}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue
            
            categories[category_name] = category_id
        except Exception as error:
            logger.error(f"Ошибка при создании категории '{category_name}': {error}")
    
    return categories


def assign_categories_to_okpd(db_manager: TenderDatabaseManager, user_id: int, categories: dict):
    """
    Присваивает категории ОКПД кодам по правилам
    """
    try:
        # Правила присвоения:
        # id от 2 до 30 включительно → Проектирование
        # id 391 и с 31 по 155 → Стройка
        
        project_category_id = categories.get("Проектирование")
        construction_category_id = categories.get("Стройка")
        
        if not project_category_id:
            logger.error("Категория 'Проектирование' не найдена")
            return
        
        if not construction_category_id:
            logger.error("Категория 'Стройка' не найдена")
            return
        
        # Присваиваем категорию "Проектирование" для id от 2 до 30
        project_ids = list(range(2, 31))  # 2-30 включительно
        if project_ids:
            placeholders = ','.join(['%s'] * len(project_ids))
            update_query = f"""
                UPDATE okpd_from_users
                SET category_id = %s
                WHERE user_id = %s AND id IN ({placeholders})
            """
            params = [project_category_id, user_id] + project_ids
            result = db_manager.execute_update(update_query, tuple(params))
            logger.info(f"Присвоена категория 'Проектирование' для {len(project_ids)} ОКПД кодов (id: {project_ids[0]}-{project_ids[-1]})")
        
        # Присваиваем категорию "Стройка" для id 391 и с 31 по 155
        construction_ids = [391] + list(range(31, 156))  # 391 и 31-155
        if construction_ids:
            placeholders = ','.join(['%s'] * len(construction_ids))
            update_query = f"""
                UPDATE okpd_from_users
                SET category_id = %s
                WHERE user_id = %s AND id IN ({placeholders})
            """
            params = [construction_category_id, user_id] + construction_ids
            result = db_manager.execute_update(update_query, tuple(params))
            logger.info(f"Присвоена категория 'Стройка' для {len(construction_ids)} ОКПД кодов (id: 391, {construction_ids[1]}-{construction_ids[-1]})")
        
        # Присваиваем категорию "Компьютеры" всем оставшимся без категории
        computers_category_id = categories.get("Компьютеры")
        if computers_category_id:
            update_query = """
                UPDATE okpd_from_users
                SET category_id = %s
                WHERE user_id = %s AND category_id IS NULL
            """
            result = db_manager.execute_update(update_query, (computers_category_id, user_id))
            logger.info(f"Присвоена категория 'Компьютеры' для всех оставшихся ОКПД кодов без категории")
        
        # Проверяем результат
        check_query = """
            SELECT 
                cat.name as category_name,
                COUNT(*) as count
            FROM okpd_from_users o
            LEFT JOIN okpd_categories cat ON o.category_id = cat.id
            WHERE o.user_id = %s
            GROUP BY cat.name
            ORDER BY cat.name NULLS LAST
        """
        stats = db_manager.execute_query(check_query, (user_id,))
        
        logger.info("\nСтатистика присвоения категорий:")
        for stat in stats:
            category_name = stat.get('category_name') or 'Без категории'
            count = stat.get('count', 0)
            logger.info(f"  {category_name}: {count} ОКПД кодов")
        
    except Exception as error:
        logger.error(f"Ошибка при присвоении категорий: {error}")
        raise


def main():
    """Основная функция."""
    logger.info("Массовое присвоение категорий ОКПД кодам")
    logger.info("=" * 80)
    
    # Подключаемся к БД tender_monitor
    try:
        tender_db = TenderDatabaseManager(config.tender_database)
        tender_db.connect()
        logger.info("✅ Подключение к БД tender_monitor установлено")
    except DatabaseConnectionError as error:
        logger.error(f"❌ Ошибка подключения к БД: {error}")
        sys.exit(1)
    
    try:
        # ID пользователя (по умолчанию 1, можно передать через аргументы)
        user_id = 1
        if len(sys.argv) > 1:
            try:
                user_id = int(sys.argv[1])
            except ValueError:
                logger.warning(f"Неверный user_id '{sys.argv[1]}', используем user_id=1")
        
        logger.info(f"Работаем с пользователем user_id={user_id}")
        
        # Создаем категории по умолчанию
        logger.info("\nСоздание категорий по умолчанию...")
        categories = create_default_categories(tender_db, user_id)
        
        if not categories:
            logger.error("❌ Не удалось создать категории")
            sys.exit(1)
        
        # Присваиваем категории ОКПД кодам
        logger.info("\nПрисвоение категорий ОКПД кодам...")
        assign_categories_to_okpd(tender_db, user_id, categories)
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ Массовое присвоение категорий завершено успешно")
        
    except Exception as error:
        logger.error(f"❌ Критическая ошибка: {error}")
        import traceback
        logger.error(traceback.format_exc())
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

