"""
Модуль для миграции завершенных контрактов из основных таблиц в таблицы завершенных контрактов.

Завершенными считаются контракты, у которых delivery_end_date < CURRENT_DATE.
Такие контракты переносятся в таблицы:
- reestr_contract_44_fz_completed
- reestr_contract_223_fz_completed
"""

from database_work.database_connection import DatabaseManager
from utils.logger_config import get_logger
from datetime import datetime
import time
import json
import os

logger = get_logger()

# Debug logging path
DEBUG_LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.cursor', 'debug.log')

def debug_log(hypothesis_id, location, message, data=None):
    """Write debug log in NDJSON format"""
    try:
        log_entry = {
            "sessionId": "debug-session",
            "runId": "migration-debug",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000)
        }
        with open(DEBUG_LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
    except Exception:
        pass  # Don't fail on debug logging errors


def migrate_unknown_and_bad_contracts():
    """
    Переносит контракты 44-ФЗ с неизвестным статусом и плохие закупки:
    - Неизвестный статус: delivery_end_date пустой, end_date < CURRENT_DATE - 180 дней
    - Плохие закупки: end_date пустой И delivery_end_date пустой
    
    :return: dict с результатами миграции
    """
    db = DatabaseManager()
    results = {
        "44_fz_unknown_migrated": 0,
        "44_fz_unknown_deleted": 0,
        "44_fz_bad_migrated": 0,
        "44_fz_bad_deleted": 0,
        "success": False,
        "error": None
    }
    
    try:
        logger.info("Начало миграции неизвестных и плохих контрактов 44-ФЗ...")
        
        # 1. Миграция контрактов с неизвестным статусом
        logger.info("Миграция контрактов 44-ФЗ с неизвестным статусом...")
        
        insert_query_unknown = """
            INSERT INTO reestr_contract_44_fz_unknown
            SELECT * FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NULL
            AND end_date IS NOT NULL
            AND end_date < (CURRENT_DATE - INTERVAL '180 days')
            AND id NOT IN (SELECT id FROM reestr_contract_44_fz_unknown);
        """
        
        db.cursor.execute(insert_query_unknown)
        results["44_fz_unknown_migrated"] = db.cursor.rowcount
        
        delete_query_unknown = """
            DELETE FROM reestr_contract_44_fz
            WHERE id IN (
                SELECT id FROM reestr_contract_44_fz_unknown
                WHERE delivery_end_date IS NULL
                AND end_date IS NOT NULL
                AND end_date < (CURRENT_DATE - INTERVAL '180 days')
            );
        """
        
        db.cursor.execute(delete_query_unknown)
        results["44_fz_unknown_deleted"] = db.cursor.rowcount
        
        db.connection.commit()
        logger.info(f"44-ФЗ неизвестные: перенесено {results['44_fz_unknown_migrated']}, удалено {results['44_fz_unknown_deleted']}")
        
        # 2. Миграция плохих закупок
        logger.info("Миграция плохих закупок 44-ФЗ...")
        
        insert_query_bad = """
            INSERT INTO reestr_contract_44_fz_bad
            SELECT * FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NULL
            AND end_date IS NULL
            AND id NOT IN (SELECT id FROM reestr_contract_44_fz_bad);
        """
        
        db.cursor.execute(insert_query_bad)
        results["44_fz_bad_migrated"] = db.cursor.rowcount
        
        delete_query_bad = """
            DELETE FROM reestr_contract_44_fz
            WHERE id IN (
                SELECT id FROM reestr_contract_44_fz_bad
                WHERE delivery_end_date IS NULL
                AND end_date IS NULL
            );
        """
        
        db.cursor.execute(delete_query_bad)
        results["44_fz_bad_deleted"] = db.cursor.rowcount
        
        db.connection.commit()
        logger.info(f"44-ФЗ плохие: перенесено {results['44_fz_bad_migrated']}, удалено {results['44_fz_bad_deleted']}")
        
        results["success"] = True
        
        total_migrated = results["44_fz_unknown_migrated"] + results["44_fz_bad_migrated"]
        total_deleted = results["44_fz_unknown_deleted"] + results["44_fz_bad_deleted"]
        
        if total_migrated > 0:
            logger.info(f"✅ Миграция неизвестных/плохих контрактов завершена. Всего перенесено: {total_migrated}, удалено: {total_deleted}")
            print(f"\n{'='*60}")
            print(f"✅ МИГРАЦИЯ НЕИЗВЕСТНЫХ И ПЛОХИХ КОНТРАКТОВ 44-ФЗ")
            print(f"{'='*60}")
            print(f"Неизвестные: перенесено {results['44_fz_unknown_migrated']}, удалено {results['44_fz_unknown_deleted']}")
            print(f"Плохие: перенесено {results['44_fz_bad_migrated']}, удалено {results['44_fz_bad_deleted']}")
            print(f"Всего перенесено: {total_migrated}, удалено: {total_deleted}")
            print(f"{'='*60}\n")
        else:
            logger.info("Миграция завершена: новых неизвестных/плохих контрактов не найдено")
            print(f"\n✅ Проверка завершена: новых неизвестных/плохих контрактов не найдено\n")
        
        return results
        
    except Exception as e:
        db.connection.rollback()
        error_msg = f"Ошибка при миграции неизвестных/плохих контрактов: {e}"
        logger.error(error_msg, exc_info=True)
        results["error"] = str(e)
        print(f"\n❌ Ошибка миграции: {e}\n")
        return results
        
    finally:
        db.close()


def migrate_completed_contracts():
    """
    Переносит завершенные контракты из основных таблиц в таблицы завершенных контрактов.
    
    Завершенными считаются контракты с delivery_end_date < CURRENT_DATE.
    
    :return: dict с результатами миграции
    """
    db = DatabaseManager()
    results = {
        "44_fz_migrated": 0,
        "44_fz_deleted": 0,
        "223_fz_migrated": 0,
        "223_fz_deleted": 0,
        "success": False,
        "error": None
    }
    
    try:
        logger.info("Начало миграции завершенных контрактов...")
        
        # Проверяем и завершаем зависшие запросы миграции
        try:
            db.cursor.execute("""
                SELECT pid, query, state, query_start
                FROM pg_stat_activity 
                WHERE (query LIKE '%INSERT INTO reestr_contract_44_fz_completed%' 
                    OR query LIKE '%DELETE FROM reestr_contract_44_fz%'
                    OR query LIKE '%INSERT INTO reestr_contract_223_fz_completed%'
                    OR query LIKE '%DELETE FROM reestr_contract_223_fz%')
                AND state = 'active'
                AND pid != pg_backend_pid()
                AND query_start < NOW() - INTERVAL '30 seconds'
            """)
            stuck_queries = db.cursor.fetchall()
            
            if stuck_queries:
                logger.warning(f"Найдено {len(stuck_queries)} зависших запросов миграции, завершаю их...")
                for pid, query, state, query_start in stuck_queries:
                    try:
                        db.cursor.execute("SELECT pg_terminate_backend(%s)", (pid,))
                        logger.warning(f"Завершен зависший процесс PID {pid}: {query[:100]}...")
                    except Exception as e:
                        logger.error(f"Ошибка завершения процесса {pid}: {e}")
                db.connection.commit()
                # Небольшая пауза после завершения процессов
                time.sleep(1)
        except Exception as e:
            logger.error(f"Ошибка при проверке зависших запросов: {e}")
        
        # 1. Миграция завершенных контрактов 44-ФЗ
        logger.info("Миграция завершенных контрактов 44-ФЗ...")
        debug_log("A", "contracts_migration.py:152", "Начало миграции 44-ФЗ", {})
        
        # Инициализируем список ID для удаления (нужно для всех случаев)
        inserted_ids = []
        
        # Сначала проверяем, сколько контрактов нужно мигрировать
        debug_log("A", "contracts_migration.py:155", "Перед COUNT запросом 44-ФЗ", {})
        count_start = time.time()
        db.cursor.execute("""
            SELECT COUNT(*) FROM reestr_contract_44_fz
            WHERE delivery_end_date IS NOT NULL 
            AND delivery_end_date < CURRENT_DATE
            AND id NOT IN (SELECT id FROM reestr_contract_44_fz_completed);
        """)
        count_to_migrate_44 = db.cursor.fetchone()[0]
        count_duration = time.time() - count_start
        logger.info(f"Найдено завершенных контрактов 44-ФЗ для миграции: {count_to_migrate_44:,}")
        debug_log("A", "contracts_migration.py:165", "После COUNT запроса 44-ФЗ", {
            "count": count_to_migrate_44,
            "duration_seconds": round(count_duration, 2)
        })
        
        if count_to_migrate_44 > 0:
            logger.info(f"Начинаю вставку {count_to_migrate_44:,} завершенных контрактов 44-ФЗ...")
            debug_log("B", "contracts_migration.py:170", "Перед INSERT запросом 44-ФЗ", {
                "rows_to_insert": count_to_migrate_44
            })
            
            # Проверяем индексы перед вставкой
            try:
                db.cursor.execute("""
                    SELECT indexname FROM pg_indexes 
                    WHERE tablename = 'reestr_contract_44_fz_completed' 
                    AND indexdef LIKE '%id%';
                """)
                indexes = [row[0] for row in db.cursor.fetchall()]
                debug_log("B", "contracts_migration.py:178", "Индексы на completed таблице", {
                    "indexes": indexes
                })
            except Exception as e:
                debug_log("B", "contracts_migration.py:182", "Ошибка проверки индексов", {
                    "error": str(e)
                })
            
            insert_start = time.time()
            batch_size = 50  # Увеличиваем размер батча для скорости
            total_inserted = 0
            # inserted_ids уже инициализирован выше
            
            # Получаем ID для миграции
            db.cursor.execute("""
                SELECT r.id FROM reestr_contract_44_fz r
                LEFT JOIN reestr_contract_44_fz_completed c ON r.id = c.id
                WHERE r.delivery_end_date IS NOT NULL 
                AND r.delivery_end_date < CURRENT_DATE
                AND c.id IS NULL
                LIMIT 1000;
            """)
            ids_to_migrate = [row[0] for row in db.cursor.fetchall()]
            debug_log("B", "contracts_migration.py:200", "Получены ID для миграции", {
                "ids_count": len(ids_to_migrate)
            })
            
            # Вставляем батчами
            for i in range(0, len(ids_to_migrate), batch_size):
                batch_ids = ids_to_migrate[i:i+batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(ids_to_migrate) + batch_size - 1) // batch_size
                
                debug_log("B", "contracts_migration.py:210", "Начало обработки батча", {
                    "batch_num": batch_num,
                    "total_batches": total_batches,
                    "batch_size": len(batch_ids),
                    "first_id": batch_ids[0] if batch_ids else None
                })
                
                batch_start = time.time()
                
                # Проверяем блокировки перед вставкой
                try:
                    db.cursor.execute("""
                        SELECT count(*) FROM pg_locks 
                        WHERE relation = 'reestr_contract_44_fz_completed'::regclass;
                    """)
                    lock_count = db.cursor.fetchone()[0]
                    debug_log("B", "contracts_migration.py:225", "Блокировки перед INSERT", {
                        "batch_num": batch_num,
                        "lock_count": lock_count
                    })
                    
                    # Если слишком много блокировок, пропускаем миграцию
                    if lock_count > 50:
                        logger.warning(f"Слишком много блокировок ({lock_count}), пропускаем миграцию")
                        debug_log("B", "contracts_migration.py:232", "Пропуск миграции из-за блокировок", {
                            "lock_count": lock_count
                        })
                        break
                except Exception as e:
                    debug_log("B", "contracts_migration.py:238", "Ошибка проверки блокировок", {
                        "batch_num": batch_num,
                        "error": str(e)
                    })
                
                # Простая вставка - используем INSERT ... SELECT напрямую
                # Не проверяем существование - если контракт уже есть, будет ошибка unique, пропустим
                for idx, contract_id in enumerate(batch_ids, 1):
                    try:
                        debug_log("B", "contracts_migration.py:245", "Вставка контракта", {
                            "batch_num": batch_num,
                            "contract_num": idx,
                            "contract_id": contract_id
                        })
                        
                        single_insert_start = time.time()
                        
                        # Простой INSERT ... SELECT - самый быстрый способ
                        insert_query = """
                            INSERT INTO reestr_contract_44_fz_completed
                            SELECT * FROM reestr_contract_44_fz
                            WHERE id = %s
                            AND delivery_end_date IS NOT NULL 
                            AND delivery_end_date < CURRENT_DATE
                        """
                        
                        db.cursor.execute(insert_query, (contract_id,))
                        single_duration = time.time() - single_insert_start
                        
                        if db.cursor.rowcount > 0:
                            total_inserted += db.cursor.rowcount
                            inserted_ids.append(contract_id)  # Сохраняем ID для удаления
                            db.connection.commit()
                            debug_log("B", "contracts_migration.py:265", "Контракт вставлен", {
                                "contract_id": contract_id,
                                "duration_seconds": round(single_duration, 3)
                            })
                        else:
                            # Контракт не подходит под условия или уже вставлен
                            db.connection.commit()
                            debug_log("B", "contracts_migration.py:273", "Контракт пропущен", {
                                "contract_id": contract_id
                            })
                            
                    except Exception as e:
                        error_msg = str(e)
                        db.connection.rollback()
                        
                            # Если это ошибка уникальности - контракт уже существует, это нормально
                        if "unique" in error_msg.lower() or "duplicate" in error_msg.lower() or "violates unique constraint" in error_msg.lower():
                            debug_log("B", "contracts_migration.py:283", "Контракт уже существует", {
                                "contract_id": contract_id
                            })
                            # Если контракт уже существует, добавляем в список для удаления
                            inserted_ids.append(contract_id)
                            continue
                        else:
                            # Другие ошибки - логируем и пропускаем
                            debug_log("B", "contracts_migration.py:291", "Ошибка вставки", {
                                "contract_id": contract_id,
                                "error": error_msg[:200]
                            })
                            logger.error(f"Ошибка вставки контракта {contract_id}: {error_msg}")
                            continue
                
                batch_duration = time.time() - batch_start
                batch_inserted_count = len([id for id in inserted_ids if id in batch_ids])
                debug_log("B", "contracts_migration.py:336", "Батч завершен", {
                    "batch_num": batch_num,
                    "batch_size": len(batch_ids),
                    "inserted_in_batch": batch_inserted_count,
                    "total_inserted": total_inserted,
                    "duration_seconds": round(batch_duration, 2)
                })
            
            results["44_fz_migrated"] = total_inserted
            insert_duration = time.time() - insert_start
            logger.info(f"Вставлено завершенных контрактов 44-ФЗ: {results['44_fz_migrated']:,}")
            debug_log("B", "contracts_migration.py:347", "Вставка завершена", {
                "rows_inserted": results["44_fz_migrated"],
                "duration_seconds": round(insert_duration, 2)
            })
            
            insert_duration = time.time() - insert_start
            results["44_fz_migrated"] = total_inserted
            logger.info(f"Вставлено завершенных контрактов 44-ФЗ: {results['44_fz_migrated']:,}")
            debug_log("B", "contracts_migration.py:234", "После INSERT запроса 44-ФЗ", {
                "rows_inserted": results["44_fz_migrated"],
                "duration_seconds": round(insert_duration, 2)
            })
        else:
            logger.info("Нет новых завершенных контрактов 44-ФЗ для миграции")
            results["44_fz_migrated"] = 0
            debug_log("A", "contracts_migration.py:191", "Нет данных для миграции 44-ФЗ", {})
        
        # Удаляем завершенные контракты из основной таблицы
        # Удаляем только те, которые реально вставлены в этом запуске
        if inserted_ids:
            logger.info(f"Начинаю удаление {len(inserted_ids):,} завершенных контрактов 44-ФЗ из основной таблицы...")
            debug_log("C", "contracts_migration.py:310", "Перед DELETE запросом 44-ФЗ", {
                "rows_to_delete": len(inserted_ids)
            })
            delete_start = time.time()
            
            # Удаляем батчами по 50 для избежания блокировок
            delete_batch_size = 50
            total_deleted = 0
            
            # Временно отключаем проверку внешних ключей для удаления
            # Контракты уже перенесены в completed, ссылки остаются валидными
            db.cursor.execute("SET session_replication_role = 'replica'")
            logger.debug("Проверка внешних ключей отключена для удаления")
            
            try:
                for i in range(0, len(inserted_ids), delete_batch_size):
                    delete_batch = inserted_ids[i:i+delete_batch_size]
                    batch_num = i // delete_batch_size + 1
                    total_batches = (len(inserted_ids) + delete_batch_size - 1) // delete_batch_size
                    
                    debug_log("C", "contracts_migration.py:410", "Начало удаления батча", {
                        "batch_num": batch_num,
                        "total_batches": total_batches,
                        "batch_size": len(delete_batch),
                        "first_id": delete_batch[0] if delete_batch else None
                    })
                    
                    batch_delete_start = time.time()
                    ids_placeholder = ','.join(['%s'] * len(delete_batch))
                    delete_query_44 = f"""
                        DELETE FROM reestr_contract_44_fz
                        WHERE id IN ({ids_placeholder})
                    """
                    
                    debug_log("C", "contracts_migration.py:422", "Выполняю DELETE", {
                        "batch_num": batch_num,
                        "ids_count": len(delete_batch)
                    })
                    
                    try:
                        db.cursor.execute(delete_query_44, delete_batch)
                        batch_deleted = db.cursor.rowcount
                        total_deleted += batch_deleted
                        batch_delete_duration = time.time() - batch_delete_start
                        
                        db.connection.commit()
                    
                    debug_log("C", "contracts_migration.py:433", "Батч удален", {
                        "batch_num": batch_num,
                        "deleted": batch_deleted,
                        "total_deleted": total_deleted,
                        "duration_seconds": round(batch_delete_duration, 3)
                    })
                    except Exception as delete_error:
                        error_msg = str(delete_error)
                        db.connection.rollback()
                        debug_log("C", "contracts_migration.py:442", "Ошибка DELETE", {
                            "batch_num": batch_num,
                            "error": error_msg[:300]
                        })
                        logger.error(f"Ошибка удаления батча {batch_num}: {error_msg}")
                        # Продолжаем со следующим батчем
                        continue
            finally:
                # Восстанавливаем проверку внешних ключей
                db.cursor.execute("SET session_replication_role = 'origin'")
                logger.debug("Проверка внешних ключей восстановлена")
            
            results["44_fz_deleted"] = total_deleted
            delete_duration = time.time() - delete_start
            logger.info(f"Удалено завершенных контрактов 44-ФЗ из основной таблицы: {results['44_fz_deleted']:,}")
            debug_log("C", "contracts_migration.py:340", "После DELETE запроса 44-ФЗ", {
                "rows_deleted": results["44_fz_deleted"],
                "duration_seconds": round(delete_duration, 2)
            })
        else:
            results["44_fz_deleted"] = 0
            debug_log("C", "contracts_migration.py:346", "Пропуск DELETE 44-ФЗ (нет вставленных)", {})
        
        commit_start = time.time()
        db.connection.commit()
        commit_duration = time.time() - commit_start
        logger.info(f"44-ФЗ: перенесено {results['44_fz_migrated']}, удалено из основной таблицы {results['44_fz_deleted']}")
        debug_log("D", "contracts_migration.py:220", "После COMMIT 44-ФЗ", {
            "duration_seconds": round(commit_duration, 2)
        })
        
        # 2. Миграция завершенных контрактов 223-ФЗ
        logger.info("Миграция завершенных контрактов 223-ФЗ...")
        debug_log("E", "contracts_migration.py:224", "Начало миграции 223-ФЗ", {})
        
        # Вставляем завершенные контракты в таблицу завершенных
        # Используем LEFT JOIN вместо NOT IN для лучшей производительности
        insert_query_223 = """
            INSERT INTO reestr_contract_223_fz_completed
            SELECT r.* FROM reestr_contract_223_fz r
            LEFT JOIN reestr_contract_223_fz_completed c ON r.id = c.id
            WHERE r.delivery_end_date IS NOT NULL 
            AND r.delivery_end_date < CURRENT_DATE
            AND c.id IS NULL;
        """
        
        db.cursor.execute(insert_query_223)
        results["223_fz_migrated"] = db.cursor.rowcount
        
        # Проверяем сколько реально вставили
        db.cursor.execute("SELECT COUNT(*) FROM reestr_contract_223_fz_completed WHERE delivery_end_date < CURRENT_DATE;")
        actual_count_223 = db.cursor.fetchone()[0]
        results["223_fz_migrated"] = actual_count_223 if actual_count_223 > results["223_fz_migrated"] else results["223_fz_migrated"]
        
        # Удаляем завершенные контракты из основной таблицы
        delete_query_223 = """
            DELETE FROM reestr_contract_223_fz
            WHERE id IN (
                SELECT id FROM reestr_contract_223_fz_completed
                WHERE delivery_end_date < CURRENT_DATE
            );
        """
        
        db.cursor.execute(delete_query_223)
        results["223_fz_deleted"] = db.cursor.rowcount
        
        db.connection.commit()
        logger.info(f"223-ФЗ: перенесено {results['223_fz_migrated']}, удалено из основной таблицы {results['223_fz_deleted']}")
        
        results["success"] = True
        
        total_migrated = results["44_fz_migrated"] + results["223_fz_migrated"]
        total_deleted = results["44_fz_deleted"] + results["223_fz_deleted"]
        
        if total_migrated > 0:
            logger.info(f"✅ Миграция завершена успешно. Всего перенесено: {total_migrated}, удалено: {total_deleted}")
            print(f"\n{'='*60}")
            print(f"✅ МИГРАЦИЯ ЗАВЕРШЕННЫХ КОНТРАКТОВ")
            print(f"{'='*60}")
            print(f"44-ФЗ: перенесено {results['44_fz_migrated']}, удалено {results['44_fz_deleted']}")
            print(f"223-ФЗ: перенесено {results['223_fz_migrated']}, удалено {results['223_fz_deleted']}")
            print(f"Всего перенесено: {total_migrated}, удалено: {total_deleted}")
            print(f"{'='*60}\n")
        else:
            logger.info("Миграция завершена: новых завершенных контрактов не найдено")
            print(f"\n✅ Проверка завершена: новых завершенных контрактов не найдено\n")
        
        return results
        
    except Exception as e:
        db.connection.rollback()
        error_msg = f"Ошибка при миграции завершенных контрактов: {e}"
        logger.error(error_msg, exc_info=True)
        results["error"] = str(e)
        print(f"\n❌ Ошибка миграции: {e}\n")
        return results
        
    finally:
        db.close()


def check_tables_exist():
    """
    Проверяет существование таблиц для завершенных контрактов.
    Создает их, если они не существуют.
    
    :return: bool - True если таблицы существуют или созданы успешно
    """
    db = DatabaseManager()
    
    try:
        # Проверяем существование таблицы 44-ФЗ завершенных
        db.cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'reestr_contract_44_fz_completed'
            );
        """)
        exists_44 = db.cursor.fetchone()[0]
        
        # Проверяем существование таблицы 223-ФЗ завершенных
        db.cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'reestr_contract_223_fz_completed'
            );
        """)
        exists_223 = db.cursor.fetchone()[0]
        
        # Проверяем таблицы для неизвестных и плохих контрактов 44-ФЗ
        db.cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'reestr_contract_44_fz_unknown'
            );
        """)
        exists_44_unknown = db.cursor.fetchone()[0]
        
        db.cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'reestr_contract_44_fz_bad'
            );
        """)
        exists_44_bad = db.cursor.fetchone()[0]
        
        if not exists_44:
            logger.info("Создание таблицы reestr_contract_44_fz_completed...")
            db.cursor.execute("CREATE TABLE reestr_contract_44_fz_completed (LIKE reestr_contract_44_fz INCLUDING ALL);")
            db.connection.commit()
            logger.info("✅ Таблица reestr_contract_44_fz_completed создана")
        
        if not exists_223:
            logger.info("Создание таблицы reestr_contract_223_fz_completed...")
            db.cursor.execute("CREATE TABLE reestr_contract_223_fz_completed (LIKE reestr_contract_223_fz INCLUDING ALL);")
            db.connection.commit()
            logger.info("✅ Таблица reestr_contract_223_fz_completed создана")
        
        if not exists_44_unknown:
            logger.info("Создание таблицы reestr_contract_44_fz_unknown...")
            db.cursor.execute("CREATE TABLE reestr_contract_44_fz_unknown (LIKE reestr_contract_44_fz INCLUDING ALL);")
            db.connection.commit()
            logger.info("✅ Таблица reestr_contract_44_fz_unknown создана")
        
        if not exists_44_bad:
            logger.info("Создание таблицы reestr_contract_44_fz_bad...")
            db.cursor.execute("CREATE TABLE reestr_contract_44_fz_bad (LIKE reestr_contract_44_fz INCLUDING ALL);")
            db.connection.commit()
            logger.info("✅ Таблица reestr_contract_44_fz_bad создана")
        
        return True
        
    except Exception as e:
        db.connection.rollback()
        error_msg = f"Ошибка при проверке/создании таблиц завершенных контрактов: {e}"
        logger.error(error_msg, exc_info=True)
        return False
        
    finally:
        db.close()
