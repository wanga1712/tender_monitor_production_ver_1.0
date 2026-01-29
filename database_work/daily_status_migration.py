"""
Модуль для ежедневной миграции контрактов по статусам.

Логика миграции:
1. Из основной таблицы (reestr_contract_44_fz/reestr_contract_223_fz):
   - Если end_date <= 1 день от текущей даты → переносим в "Работа комиссии"
   
2. Из таблицы "Работа комиссии":
   - Если end_date > 60 дней от даты переноса → переносим в "неясный"
   - Если есть delivery_start_date и оно не NULL → переносим в "Разыгранные"

3. После переноса удаляем из исходной таблицы.

Запускается ежедневно в 12:00 ночи через systemd timer.
"""

from database_work.database_connection import DatabaseManager
from utils.logger_config import get_logger
from datetime import datetime, timedelta
import subprocess
import os
from pathlib import Path

logger = get_logger()

# Названия таблиц (без пробелов для SQL)
TABLES_44 = {
    'main': 'reestr_contract_44_fz',
    'commission_work': 'reestr_contract_44_fz_commission_work',
    'unclear': 'reestr_contract_44_fz_unclear',
    'awarded': 'reestr_contract_44_fz_awarded'
}

TABLES_223 = {
    'main': 'reestr_contract_223_fz',
    'commission_work': 'reestr_contract_223_fz_commission_work',
    'unclear': 'reestr_contract_223_fz_unclear',
    'awarded': 'reestr_contract_223_fz_awarded'
}


def create_backup(force: bool = False):
    """
    Создает бэкап базы данных.

    Правила:
    - Храним максимум 1 бэкап.
    - Делаем бэкап только при первом запуске (когда бэкапов ещё нет)
      или раз в неделю по воскресеньям.

    :return: str - путь к файлу бэкапа или None (если бэкап не делался / ошибка)
    """
    db = DatabaseManager()
    try:
        # Папка для бэкапов
        backup_dir = Path('/opt/tendermonitor/backups')
        backup_dir.mkdir(parents=True, exist_ok=True)

        # Смотрим уже существующие бэкапы
        existing_backups = sorted(backup_dir.glob('tendermonitor_backup_*.sql'), reverse=True)

        today = datetime.now()
        weekday = today.weekday()  # 0=понедельник, 6=воскресенье

        # Условия:
        # 1) Если force=True — всегда делаем бэкап.
        # 2) Если бэкапов нет — делаем первый.
        # 3) Если сегодня воскресенье — делаем еженедельный.
        if not force:
            if existing_backups and weekday != 6:
                logger.info("Бэкап пропущен: уже есть существующий бэкап и сегодня не воскресенье")
                return None

        # Получаем параметры БД
        db_name = db.db_name
        db_user = db.db_user
        db_host = db.db_host
        db_port = db.db_port

        # Имя файла бэкапа с датой и временем
        timestamp = today.strftime('%Y%m%d_%H%M%S')
        backup_file = backup_dir / f'tendermonitor_backup_{timestamp}.sql'

        logger.info(f"Создание бэкапа БД в {backup_file}...")

        # Формируем команду pg_dump
        env = os.environ.copy()
        env['PGPASSWORD'] = db.db_password

        cmd = [
            'pg_dump',
            '-h', db_host,
            '-p', str(db_port),
            '-U', db_user,
            '-d', db_name,
            '-F', 'c',  # custom format
            '-f', str(backup_file),
        ]

        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 час максимум
        )

        if result.returncode == 0:
            logger.info(f"✅ Бэкап успешно создан: {backup_file}")
            # Храним только один последний бэкап
            cleanup_old_backups(backup_dir, keep=1)
            return str(backup_file)
        else:
            logger.error(f"❌ Ошибка создания бэкапа: {result.stderr}")
            return None

    except Exception as e:
        logger.error(f"❌ Ошибка при создании бэкапа: {e}", exc_info=True)
        return None
    finally:
        db.close()


def cleanup_old_backups(backup_dir: Path, keep: int = 1):
    """
    Удаляет старые бэкапы, оставляя только последние N (по умолчанию 1).

    :param backup_dir: Директория с бэкапами
    :param keep: Количество бэкапов для сохранения
    """
    try:
        backups = sorted(backup_dir.glob('tendermonitor_backup_*.sql'), reverse=True)
        if len(backups) > keep:
            for old_backup in backups[keep:]:
                old_backup.unlink()
                logger.info(f"Удален старый бэкап: {old_backup.name}")
    except Exception as e:
        logger.warning(f"Ошибка при очистке старых бэкапов: {e}")


def check_and_create_status_tables():
    """
    Проверяет существование статусных таблиц и создает их, если нужно.
    
    :return: bool - True если все таблицы существуют или созданы успешно
    """
    db = DatabaseManager()
    
    try:
        # Проверяем и создаем таблицы для 44-ФЗ
        for table_key, table_name in TABLES_44.items():
            if table_key == 'main':
                continue  # Основная таблица уже существует
            
            db.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (table_name,))
            exists = db.cursor.fetchone()[0]
            
            if not exists:
                logger.info(f"Создание таблицы {table_name}...")
                # Создаем таблицу на основе основной, включая все структуры и связи
                db.cursor.execute(f"""
                    CREATE TABLE {table_name} (LIKE {TABLES_44['main']} INCLUDING ALL);
                """)
                db.connection.commit()
                logger.info(f"✅ Таблица {table_name} создана")
        
        # Проверяем и создаем таблицы для 223-ФЗ
        for table_key, table_name in TABLES_223.items():
            if table_key == 'main':
                continue  # Основная таблица уже существует
            
            db.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (table_name,))
            exists = db.cursor.fetchone()[0]
            
            if not exists:
                logger.info(f"Создание таблицы {table_name}...")
                db.cursor.execute(f"""
                    CREATE TABLE {table_name} (LIKE {TABLES_223['main']} INCLUDING ALL);
                """)
                db.connection.commit()
                logger.info(f"✅ Таблица {table_name} создана")
        
        return True
        
    except Exception as e:
        db.connection.rollback()
        error_msg = f"Ошибка при проверке/создании статусных таблиц: {e}"
        logger.error(error_msg, exc_info=True)
        return False
    finally:
        db.close()


def migrate_from_main_to_commission_work(fz_type: str = '44'):
    """
    Мигрирует контракты из основной таблицы в "Работа комиссии".
    Условие: end_date <= 1 день от текущей даты.
    
    :param fz_type: '44' или '223'
    :return: tuple (migrated_count, deleted_count)
    """
    db = DatabaseManager()
    tables = TABLES_44 if fz_type == '44' else TABLES_223
    
    try:
        # Получаем ID контрактов для миграции
        db.cursor.execute(f"""
            SELECT id FROM {tables['main']}
            WHERE end_date IS NOT NULL
            AND end_date <= CURRENT_DATE + INTERVAL '1 day'
            AND id NOT IN (SELECT id FROM {tables['commission_work']})
            LIMIT 1000;
        """)
        ids_to_migrate = [row[0] for row in db.cursor.fetchall()]
        
        if not ids_to_migrate:
            logger.info(f"{fz_type}-ФЗ: Нет контрактов для миграции в 'Работа комиссии'")
            return (0, 0)
        
        logger.info(f"{fz_type}-ФЗ: Найдено {len(ids_to_migrate)} контрактов для миграции в 'Работа комиссии'")
        
        # Вставляем контракты батчами
        batch_size = 50
        migrated_ids = []
        
        for i in range(0, len(ids_to_migrate), batch_size):
            batch_ids = ids_to_migrate[i:i+batch_size]
            
            for contract_id in batch_ids:
                try:
                    # Вставляем контракт
                    db.cursor.execute(f"""
                        INSERT INTO {tables['commission_work']}
                        SELECT * FROM {tables['main']}
                        WHERE id = %s
                        AND end_date IS NOT NULL
                        AND end_date <= CURRENT_DATE + INTERVAL '1 day'
                    """, (contract_id,))
                    
                    if db.cursor.rowcount > 0:
                        migrated_ids.append(contract_id)
                        db.connection.commit()
                    else:
                        db.connection.rollback()
                        
                except Exception as e:
                    error_msg = str(e)
                    db.connection.rollback()
                    
                    # Если это ошибка уникальности - контракт уже существует
                    if "unique" in error_msg.lower() or "duplicate" in error_msg.lower():
                        migrated_ids.append(contract_id)  # Добавляем для удаления
                        continue
                    else:
                        logger.error(f"Ошибка вставки контракта {contract_id}: {error_msg}")
                        continue
        
        migrated_count = len(migrated_ids)
        logger.info(f"{fz_type}-ФЗ: Вставлено {migrated_count} контрактов в 'Работа комиссии'")
        
        # Удаляем из основной таблицы
        if migrated_ids:
            # Временно отключаем проверку внешних ключей
            db.cursor.execute("SET session_replication_role = 'replica'")
            
            delete_batch_size = 50
            deleted_count = 0
            
            for i in range(0, len(migrated_ids), delete_batch_size):
                delete_batch = migrated_ids[i:i+delete_batch_size]
                ids_placeholder = ','.join(['%s'] * len(delete_batch))
                
                try:
                    db.cursor.execute(f"""
                        DELETE FROM {tables['main']}
                        WHERE id IN ({ids_placeholder})
                    """, delete_batch)
                    
                    deleted_count += db.cursor.rowcount
                    db.connection.commit()
                except Exception as e:
                    logger.error(f"Ошибка удаления батча: {e}")
                    db.connection.rollback()
                    continue
            
            # Восстанавливаем проверку внешних ключей
            db.cursor.execute("SET session_replication_role = 'origin'")
            
            logger.info(f"{fz_type}-ФЗ: Удалено {deleted_count} контрактов из основной таблицы")
            return (migrated_count, deleted_count)
        else:
            return (0, 0)
            
    except Exception as e:
        db.connection.rollback()
        error_msg = f"Ошибка при миграции из основной таблицы в 'Работа комиссии' ({fz_type}-ФЗ): {e}"
        logger.error(error_msg, exc_info=True)
        return (0, 0)
    finally:
        db.close()


def migrate_from_commission_work(fz_type: str = '44'):
    """
    Мигрирует контракты из "Работа комиссии" в "неясный" или "Разыгранные".
    
    Условия:
    - Если end_date > 60 дней от даты переноса → "неясный"
    - Если есть delivery_start_date и оно не NULL → "Разыгранные"
    
    :param fz_type: '44' или '223'
    :return: dict с результатами миграции
    """
    db = DatabaseManager()
    tables = TABLES_44 if fz_type == '44' else TABLES_223
    
    results = {
        'unclear_migrated': 0,
        'unclear_deleted': 0,
        'awarded_migrated': 0,
        'awarded_deleted': 0
    }
    
    try:
        # 1. Миграция в "неясный" (end_date > 60 дней от даты переноса)
        # Для упрощения используем CURRENT_DATE - 60 дней
        db.cursor.execute(f"""
            SELECT id FROM {tables['commission_work']}
            WHERE end_date IS NOT NULL
            AND end_date < CURRENT_DATE - INTERVAL '60 days'
            AND id NOT IN (SELECT id FROM {tables['unclear']})
            LIMIT 1000;
        """)
        ids_to_unclear = [row[0] for row in db.cursor.fetchall()]
        
        if ids_to_unclear:
            logger.info(f"{fz_type}-ФЗ: Найдено {len(ids_to_unclear)} контрактов для миграции в 'неясный'")
            
            migrated_to_unclear = []
            batch_size = 50
            
            for i in range(0, len(ids_to_unclear), batch_size):
                batch_ids = ids_to_unclear[i:i+batch_size]
                
                for contract_id in batch_ids:
                    try:
                        db.cursor.execute(f"""
                            INSERT INTO {tables['unclear']}
                            SELECT * FROM {tables['commission_work']}
                            WHERE id = %s
                            AND end_date IS NOT NULL
                            AND end_date < CURRENT_DATE - INTERVAL '60 days'
                        """, (contract_id,))
                        
                        if db.cursor.rowcount > 0:
                            migrated_to_unclear.append(contract_id)
                            db.connection.commit()
                        else:
                            db.connection.rollback()
                            
                    except Exception as e:
                        error_msg = str(e)
                        db.connection.rollback()
                        
                        if "unique" in error_msg.lower() or "duplicate" in error_msg.lower():
                            migrated_to_unclear.append(contract_id)
                            continue
                        else:
                            logger.error(f"Ошибка вставки контракта {contract_id} в 'неясный': {error_msg}")
                            continue
            
            results['unclear_migrated'] = len(migrated_to_unclear)
            
            # Удаляем из "Работа комиссии"
            if migrated_to_unclear:
                db.cursor.execute("SET session_replication_role = 'replica'")
                
                for i in range(0, len(migrated_to_unclear), batch_size):
                    delete_batch = migrated_to_unclear[i:i+batch_size]
                    ids_placeholder = ','.join(['%s'] * len(delete_batch))
                    
                    try:
                        db.cursor.execute(f"""
                            DELETE FROM {tables['commission_work']}
                            WHERE id IN ({ids_placeholder})
                        """, delete_batch)
                        
                        results['unclear_deleted'] += db.cursor.rowcount
                        db.connection.commit()
                    except Exception as e:
                        logger.error(f"Ошибка удаления батча из 'Работа комиссии': {e}")
                        db.connection.rollback()
                        continue
                
                db.cursor.execute("SET session_replication_role = 'origin'")
                logger.info(f"{fz_type}-ФЗ: Мигрировано в 'неясный': {results['unclear_migrated']}, удалено из 'Работа комиссии': {results['unclear_deleted']}")
        
        # 2. Миграция в "Разыгранные" (есть delivery_start_date и оно не NULL)
        db.cursor.execute(f"""
            SELECT id FROM {tables['commission_work']}
            WHERE delivery_start_date IS NOT NULL
            AND id NOT IN (SELECT id FROM {tables['awarded']})
            LIMIT 1000;
        """)
        ids_to_awarded = [row[0] for row in db.cursor.fetchall()]
        
        if ids_to_awarded:
            logger.info(f"{fz_type}-ФЗ: Найдено {len(ids_to_awarded)} контрактов для миграции в 'Разыгранные'")
            
            migrated_to_awarded = []
            batch_size = 50
            
            for i in range(0, len(ids_to_awarded), batch_size):
                batch_ids = ids_to_awarded[i:i+batch_size]
                
                for contract_id in batch_ids:
                    try:
                        db.cursor.execute(f"""
                            INSERT INTO {tables['awarded']}
                            SELECT * FROM {tables['commission_work']}
                            WHERE id = %s
                            AND delivery_start_date IS NOT NULL
                        """, (contract_id,))
                        
                        if db.cursor.rowcount > 0:
                            migrated_to_awarded.append(contract_id)
                            db.connection.commit()
                        else:
                            db.connection.rollback()
                            
                    except Exception as e:
                        error_msg = str(e)
                        db.connection.rollback()
                        
                        if "unique" in error_msg.lower() or "duplicate" in error_msg.lower():
                            migrated_to_awarded.append(contract_id)
                            continue
                        else:
                            logger.error(f"Ошибка вставки контракта {contract_id} в 'Разыгранные': {error_msg}")
                            continue
            
            results['awarded_migrated'] = len(migrated_to_awarded)
            
            # Удаляем из "Работа комиссии"
            if migrated_to_awarded:
                db.cursor.execute("SET session_replication_role = 'replica'")
                
                for i in range(0, len(migrated_to_awarded), batch_size):
                    delete_batch = migrated_to_awarded[i:i+batch_size]
                    ids_placeholder = ','.join(['%s'] * len(delete_batch))
                    
                    try:
                        db.cursor.execute(f"""
                            DELETE FROM {tables['commission_work']}
                            WHERE id IN ({ids_placeholder})
                        """, delete_batch)
                        
                        results['awarded_deleted'] += db.cursor.rowcount
                        db.connection.commit()
                    except Exception as e:
                        logger.error(f"Ошибка удаления батча из 'Работа комиссии': {e}")
                        db.connection.rollback()
                        continue
                
                db.cursor.execute("SET session_replication_role = 'origin'")
                logger.info(f"{fz_type}-ФЗ: Мигрировано в 'Разыгранные': {results['awarded_migrated']}, удалено из 'Работа комиссии': {results['awarded_deleted']}")
        
        return results
        
    except Exception as e:
        db.connection.rollback()
        error_msg = f"Ошибка при миграции из 'Работа комиссии' ({fz_type}-ФЗ): {e}"
        logger.error(error_msg, exc_info=True)
        return results
    finally:
        db.close()


def run_daily_status_migration():
    """
    Основная функция для запуска ежедневной миграции по статусам.
    
    :return: dict с результатами миграции
    """
    logger.info("=" * 60)
    logger.info("Начало ежедневной миграции контрактов по статусам")
    logger.info("=" * 60)
    
    # Создаем бэкап:
    # - при первом запуске (нет бэкапов)
    # - или раз в неделю по воскресеньям
    backup_file = create_backup()
    if backup_file:
        logger.info(f"Бэкап для текущего запуска: {backup_file}")
    
    # Проверяем и создаем статусные таблицы
    if not check_and_create_status_tables():
        logger.error("❌ Ошибка при проверке/создании статусных таблиц")
        return {'success': False, 'error': 'Ошибка создания таблиц'}
    
    results = {
        'success': True,
        'backup_file': backup_file,
        '44_fz': {},
        '223_fz': {}
    }
    
    # Миграция 44-ФЗ
    logger.info("\n--- Миграция 44-ФЗ ---")
    
    # 1. Из основной в "Работа комиссии"
    migrated, deleted = migrate_from_main_to_commission_work('44')
    results['44_fz']['main_to_commission'] = {'migrated': migrated, 'deleted': deleted}
    
    # 2. Из "Работа комиссии" в "неясный"/"Разыгранные"
    commission_results = migrate_from_commission_work('44')
    results['44_fz']['commission_migration'] = commission_results
    
    # Миграция 223-ФЗ
    logger.info("\n--- Миграция 223-ФЗ ---")
    
    # 1. Из основной в "Работа комиссии"
    migrated, deleted = migrate_from_main_to_commission_work('223')
    results['223_fz']['main_to_commission'] = {'migrated': migrated, 'deleted': deleted}
    
    # 2. Из "Работа комиссии" в "неясный"/"Разыгранные"
    commission_results = migrate_from_commission_work('223')
    results['223_fz']['commission_migration'] = commission_results
    
    # Итоговая статистика
    total_44 = (
        results['44_fz']['main_to_commission']['migrated'] +
        results['44_fz']['commission_migration']['unclear_migrated'] +
        results['44_fz']['commission_migration']['awarded_migrated']
    )
    total_223 = (
        results['223_fz']['main_to_commission']['migrated'] +
        results['223_fz']['commission_migration']['unclear_migrated'] +
        results['223_fz']['commission_migration']['awarded_migrated']
    )
    
    logger.info("=" * 60)
    logger.info("Ежедневная миграция завершена")
    logger.info(f"44-ФЗ: всего мигрировано {total_44} контрактов")
    logger.info(f"223-ФЗ: всего мигрировано {total_223} контрактов")
    logger.info("=" * 60)
    
    return results


if __name__ == '__main__':
    # Запуск миграции при прямом вызове скрипта
    results = run_daily_status_migration()
    print(f"\nРезультаты миграции: {results}")
