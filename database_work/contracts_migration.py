"""
Модуль для миграции завершенных контрактов из основных таблиц в таблицы завершенных контрактов.

Завершенными считаются контракты, у которых delivery_end_date < CURRENT_DATE.
Такие контракты переносятся в таблицы:
- reestr_contract_44_fz_completed
- reestr_contract_223_fz_completed

Вместе с контрактами архивируются ссылки из links_documentation_44_fz
в links_documentation_44_fz_archive.
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


def _archive_links_44_fz(db, contract_ids, reason="completed"):
    """
    Архивирует ссылки из links_documentation_44_fz в links_documentation_44_fz_archive
    для указанных contract_id, затем удаляет их из основной таблицы.

    Матчинг производится по contract_id И по contract_number (для строк,
    у которых contract_id IS NULL — исторически вставлялись без FK).

    Инварианты:
    - ON CONFLICT (id) DO NOTHING гарантирует идемпотентность
    - Ошибки в батче не блокируют следующий батч
    - Не архивирует строки активных контрактов

    Возвращает количество заархивированных строк.
    """
    if not contract_ids:
        return 0
    archived_total = 0
    deleted_total = 0
    batch_size = 500
    for i in range(0, len(contract_ids), batch_size):
        batch = contract_ids[i:i + batch_size]
        placeholder = ','.join(['%s'] * len(batch))
        try:
            # Archive by contract_id (for rows with FK set)
            # AND by contract_number for rows where contract_id IS NULL
            # (historically inserted without FK — this was the bug).
            db.cursor.execute(f"""
                INSERT INTO links_documentation_44_fz_archive
                    (id, contract_id, document_links, file_name, contract_number, archived_reason)
                SELECT ld.id, ld.contract_id, ld.document_links, ld.file_name,
                       ld.contract_number, %s
                FROM links_documentation_44_fz ld
                WHERE
                    ld.contract_id IN ({placeholder})
                    OR (
                        ld.contract_id IS NULL
                        AND ld.contract_number IN (
                            SELECT contract_number FROM reestr_contract_44_fz_completed
                            WHERE id IN ({placeholder})
                        )
                    )
                ON CONFLICT (id) DO NOTHING
            """, (reason, *batch, *batch))
            archived_total += db.cursor.rowcount

            # Delete by contract_id AND by contract_number (mirror of archive above)
            db.cursor.execute(f"""
                DELETE FROM links_documentation_44_fz
                WHERE
                    contract_id IN ({placeholder})
                    OR (
                        contract_id IS NULL
                        AND contract_number IN (
                            SELECT contract_number FROM reestr_contract_44_fz_completed
                            WHERE id IN ({placeholder})
                        )
                    )
            """, (*batch, *batch))
            deleted_total += db.cursor.rowcount
            db.connection.commit()
        except Exception as e:
            db.connection.rollback()
            logger.error(f"Ошибка архивирования ссылок (пачка {i // batch_size + 1}): {e}")
    if archived_total:
        logger.info(f"_archive_links_44_fz: заархивировано {archived_total}, удалено {deleted_total}")
    return archived_total


def archive_links_for_historical_completed():
    """
    Одноразовый (идемпотентный) перенос ссылок для исторически завершённых контрактов.

    Причина: _archive_links_44_fz() ранее не обрабатывал строки с contract_id IS NULL.
    В результате ссылки для контрактов, перенесённых в reestr_contract_44_fz_completed
    до исправления, остались в основной таблице.

    Эта функция архивирует все ссылки, у которых contract_number совпадает
    с каким-либо уже завершённым контрактом и которые ещё не в archive таблице.

    Идемпотентна: повторный вызов не переносит ни одной строки.

    Возвращает dict с результатами: archived, deleted, batches, already_in_archive.
    """
    db = DatabaseManager()
    db.cursor = db.connection.cursor()
    results = {
        "archived": 0,
        "deleted": 0,
        "batches": 0,
        "already_in_archive": 0,
        "success": False,
        "error": None,
    }

    try:
        # Count how many are already archived (for reporting)
        db.cursor.execute("""
            SELECT count(*) FROM links_documentation_44_fz_archive;
        """)
        results["already_in_archive"] = db.cursor.fetchone()[0]

        # Process in batches to avoid lock contention.
        # Match on contract_number ∈ completed table, excluding rows already archived.
        BATCH = 2000
        batch_num = 0
        while True:
            # Fetch a batch of link IDs to archive
            db.cursor.execute(f"""
                SELECT ld.id, ld.contract_id, ld.document_links, ld.file_name,
                       ld.contract_number
                FROM links_documentation_44_fz ld
                WHERE ld.contract_number IN (
                    SELECT contract_number FROM reestr_contract_44_fz_completed
                )
                  AND NOT EXISTS (
                    SELECT 1 FROM links_documentation_44_fz_archive a WHERE a.id = ld.id
                )
                LIMIT {BATCH};
            """)
            rows = db.cursor.fetchall()
            if not rows:
                break

            batch_num += 1
            ids = [r[0] for r in rows]
            placeholder = ','.join(['%s'] * len(ids))

            # Archive
            db.cursor.execute(f"""
                INSERT INTO links_documentation_44_fz_archive
                    (id, contract_id, document_links, file_name, contract_number, archived_reason)
                SELECT id, contract_id, document_links, file_name, contract_number,
                       'historical_completed'
                FROM links_documentation_44_fz
                WHERE id IN ({placeholder})
                ON CONFLICT (id) DO NOTHING
            """, tuple(ids))
            archived_in_batch = db.cursor.rowcount
            results["archived"] += archived_in_batch

            # Delete from main table
            db.cursor.execute(f"""
                DELETE FROM links_documentation_44_fz
                WHERE id IN ({placeholder})
            """, tuple(ids))
            results["deleted"] += db.cursor.rowcount

            db.connection.commit()
            results["batches"] += 1
            logger.info(
                f"archive_links_for_historical_completed: батч {batch_num} — "
                f"заархивировано {archived_in_batch} / {len(rows)}"
            )

        results["success"] = True
        logger.info(
            f"archive_links_for_historical_completed завершена: "
            f"батчей={results['batches']}, заархивировано={results['archived']}, "
            f"удалено={results['deleted']}"
        )
        return results

    except Exception as e:
        db.connection.rollback()
        results["error"] = str(e)
        logger.error(f"Ошибка исторической архивации ссылок: {e}", exc_info=True)
        return results

    finally:
        db.close()


def migrate_unknown_and_bad_contracts():
    """
    Переносит контракты 44-ФЗ с неизвестным статусом и плохие закупки:
    - Неизвестный статус: delivery_end_date пустой, end_date < CURRENT_DATE - 180 дней
    - Плохие закупки: end_date пустой И delivery_end_date пустой

    :return: dict с результатами миграции
    """
    db = DatabaseManager()
    db.cursor = db.connection.cursor()
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
            logger.info(f"Миграция неизвестных/плохих контрактов завершена. Всего перенесено: {total_migrated}, удалено: {total_deleted}")
            print(f"\n{'='*60}")
            print(f"МИГРАЦИЯ НЕИЗВЕСТНЫХ И ПЛОХИХ КОНТРАКТОВ 44-ФЗ")
            print(f"{'='*60}")
            print(f"Неизвестные: перенесено {results['44_fz_unknown_migrated']}, удалено {results['44_fz_unknown_deleted']}")
            print(f"Плохие: перенесено {results['44_fz_bad_migrated']}, удалено {results['44_fz_bad_deleted']}")
            print(f"Всего перенесено: {total_migrated}, удалено: {total_deleted}")
            print(f"{'='*60}\n")
        else:
            logger.info("Миграция завершена: новых неизвестных/плохих контрактов не найдено")
            print(f"\nПроверка завершена: новых неизвестных/плохих контрактов не найдено\n")

        return results

    except Exception as e:
        db.connection.rollback()
        error_msg = f"Ошибка при миграции неизвестных/плохих контрактов: {e}"
        logger.error(error_msg, exc_info=True)
        results["error"] = str(e)
        print(f"\nОшибка миграции: {e}\n")
        return results

    finally:
        db.close()


def migrate_completed_contracts():
    """
    Переносит завершенные контракты из основных таблиц в таблицы завершенных контрактов.

    Завершенными считаются контракты с delivery_end_date < CURRENT_DATE.
    Источники:
    - reestr_contract_44_fz (открытые)
    - reestr_contract_44_fz_awarded (разыгранные)
    - reestr_contract_223_fz

    Одновременно архивируются ссылки из links_documentation_44_fz в _archive.

    :return: dict с результатами миграции
    """
    db = DatabaseManager()
    db.cursor = db.connection.cursor()
    results = {
        "44_fz_migrated": 0,
        "44_fz_deleted": 0,
        "44_fz_links_archived": 0,
        "44_fz_awarded_migrated": 0,
        "44_fz_awarded_deleted": 0,
        "44_fz_awarded_links_archived": 0,
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
                time.sleep(1)
        except Exception as e:
            logger.error(f"Ошибка при проверке зависших запросов: {e}")

        # ── 1. Миграция завершенных контрактов 44-ФЗ (открытые) ──────────────────
        logger.info("Миграция завершенных контрактов 44-ФЗ (открытые)...")
        debug_log("A", "contracts_migration.py:152", "Начало миграции 44-ФЗ", {})

        inserted_ids = []

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
            batch_size = 50
            total_inserted = 0

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

                for idx, contract_id in enumerate(batch_ids, 1):
                    try:
                        debug_log("B", "contracts_migration.py:245", "Вставка контракта", {
                            "batch_num": batch_num,
                            "contract_num": idx,
                            "contract_id": contract_id
                        })

                        single_insert_start = time.time()

                        db.cursor.execute("""
                            INSERT INTO reestr_contract_44_fz_completed
                            SELECT * FROM reestr_contract_44_fz
                            WHERE id = %s
                            AND delivery_end_date IS NOT NULL
                            AND delivery_end_date < CURRENT_DATE
                        """, (contract_id,))
                        single_duration = time.time() - single_insert_start

                        if db.cursor.rowcount > 0:
                            total_inserted += db.cursor.rowcount
                            inserted_ids.append(contract_id)
                            db.connection.commit()
                            debug_log("B", "contracts_migration.py:265", "Контракт вставлен", {
                                "contract_id": contract_id,
                                "duration_seconds": round(single_duration, 3)
                            })
                        else:
                            db.connection.commit()
                            debug_log("B", "contracts_migration.py:273", "Контракт пропущен", {
                                "contract_id": contract_id
                            })

                    except Exception as e:
                        error_msg = str(e)
                        db.connection.rollback()

                        if "unique" in error_msg.lower() or "duplicate" in error_msg.lower() or "violates unique constraint" in error_msg.lower():
                            debug_log("B", "contracts_migration.py:283", "Контракт уже существует", {
                                "contract_id": contract_id
                            })
                            inserted_ids.append(contract_id)
                            continue
                        else:
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
        else:
            logger.info("Нет новых завершенных контрактов 44-ФЗ для миграции")
            results["44_fz_migrated"] = 0
            debug_log("A", "contracts_migration.py:191", "Нет данных для миграции 44-ФЗ", {})

        # Архивируем ссылки перед удалением контрактов из открытой таблицы
        if inserted_ids:
            logger.info(f"Архивирование ссылок для {len(inserted_ids)} контрактов 44-ФЗ...")
            links_archived = _archive_links_44_fz(db, inserted_ids, "completed")
            results["44_fz_links_archived"] = links_archived
            logger.info(f"Заархивировано ссылок 44-ФЗ: {links_archived}")

        # Удаляем завершенные контракты из основной (открытой) таблицы
        if inserted_ids:
            logger.info(f"Начинаю удаление {len(inserted_ids):,} завершенных контрактов 44-ФЗ из основной таблицы...")
            debug_log("C", "contracts_migration.py:310", "Перед DELETE запросом 44-ФЗ", {
                "rows_to_delete": len(inserted_ids)
            })
            delete_start = time.time()

            delete_batch_size = 50
            total_deleted = 0

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
                        continue
            finally:
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

        db.connection.commit()
        logger.info(f"44-ФЗ (открытые): перенесено {results['44_fz_migrated']}, удалено {results['44_fz_deleted']}, ссылок архивировано {results['44_fz_links_archived']}")

        # ── 2. Миграция завершенных контрактов 44-ФЗ (разыгранные / awarded) ──────
        logger.info("Миграция завершенных контрактов 44-ФЗ (разыгранные)...")

        db.cursor.execute("""
            SELECT COUNT(*) FROM reestr_contract_44_fz_awarded
            WHERE delivery_end_date IS NOT NULL
            AND delivery_end_date < CURRENT_DATE
            AND id NOT IN (SELECT id FROM reestr_contract_44_fz_completed);
        """)
        count_to_migrate_awarded = db.cursor.fetchone()[0]
        logger.info(f"Найдено завершенных контрактов 44-ФЗ (awarded) для миграции: {count_to_migrate_awarded:,}")

        awarded_inserted_ids = []

        if count_to_migrate_awarded > 0:
            db.cursor.execute("""
                SELECT r.id FROM reestr_contract_44_fz_awarded r
                LEFT JOIN reestr_contract_44_fz_completed c ON r.id = c.id
                WHERE r.delivery_end_date IS NOT NULL
                AND r.delivery_end_date < CURRENT_DATE
                AND c.id IS NULL
                LIMIT 1000;
            """)
            awarded_ids_to_migrate = [row[0] for row in db.cursor.fetchall()]

            awarded_insert_start = time.time()
            for contract_id in awarded_ids_to_migrate:
                try:
                    db.cursor.execute("""
                        INSERT INTO reestr_contract_44_fz_completed
                        SELECT * FROM reestr_contract_44_fz_awarded
                        WHERE id = %s
                        AND delivery_end_date IS NOT NULL
                        AND delivery_end_date < CURRENT_DATE
                    """, (contract_id,))
                    if db.cursor.rowcount > 0:
                        awarded_inserted_ids.append(contract_id)
                    db.connection.commit()
                except Exception as e:
                    db.connection.rollback()
                    if "unique" in str(e).lower() or "duplicate" in str(e).lower() or "violates unique constraint" in str(e).lower():
                        awarded_inserted_ids.append(contract_id)
                    else:
                        logger.error(f"Ошибка вставки awarded контракта {contract_id}: {e}")

            results["44_fz_awarded_migrated"] = len(awarded_inserted_ids)
            logger.info(f"Вставлено завершенных контрактов из awarded 44-ФЗ: {results['44_fz_awarded_migrated']:,} за {time.time()-awarded_insert_start:.1f}с")
        else:
            logger.info("Нет новых завершенных контрактов 44-ФЗ (awarded) для миграции")

        # Архивируем ссылки awarded контрактов
        if awarded_inserted_ids:
            logger.info(f"Архивирование ссылок для {len(awarded_inserted_ids)} awarded контрактов...")
            links_archived_awarded = _archive_links_44_fz(db, awarded_inserted_ids, "completed_awarded")
            results["44_fz_awarded_links_archived"] = links_archived_awarded
            logger.info(f"Заархивировано ссылок awarded 44-ФЗ: {links_archived_awarded}")

            # Удаляем awarded контракты из их таблицы
            logger.info(f"Удаление {len(awarded_inserted_ids)} контрактов из awarded таблицы...")
            awarded_del_batch = 50
            total_del_awarded = 0
            db.cursor.execute("SET session_replication_role = 'replica'")
            try:
                for i in range(0, len(awarded_inserted_ids), awarded_del_batch):
                    batch = awarded_inserted_ids[i:i + awarded_del_batch]
                    placeholder = ','.join(['%s'] * len(batch))
                    try:
                        db.cursor.execute(
                            f"DELETE FROM reestr_contract_44_fz_awarded WHERE id IN ({placeholder})",
                            tuple(batch)
                        )
                        total_del_awarded += db.cursor.rowcount
                        db.connection.commit()
                    except Exception as e:
                        db.connection.rollback()
                        logger.error(f"Ошибка удаления awarded батча: {e}")
            finally:
                db.cursor.execute("SET session_replication_role = 'origin'")

            results["44_fz_awarded_deleted"] = total_del_awarded
            logger.info(f"Удалено из awarded 44-ФЗ: {results['44_fz_awarded_deleted']:,}")

        logger.info(
            f"44-ФЗ (awarded): перенесено {results['44_fz_awarded_migrated']}, "
            f"удалено {results['44_fz_awarded_deleted']}, "
            f"ссылок архивировано {results['44_fz_awarded_links_archived']}"
        )

        # ── 3. Миграция завершенных контрактов 223-ФЗ ───────────────────────────
        logger.info("Миграция завершенных контрактов 223-ФЗ...")
        debug_log("E", "contracts_migration.py:224", "Начало миграции 223-ФЗ", {})

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

        total_migrated = results["44_fz_migrated"] + results["44_fz_awarded_migrated"] + results["223_fz_migrated"]
        total_deleted = results["44_fz_deleted"] + results["44_fz_awarded_deleted"] + results["223_fz_deleted"]
        total_links = results["44_fz_links_archived"] + results["44_fz_awarded_links_archived"]

        if total_migrated > 0:
            logger.info(f"Миграция завершена успешно. Всего перенесено: {total_migrated}, удалено: {total_deleted}, ссылок архивировано: {total_links}")
            print(f"\n{'='*60}")
            print(f"МИГРАЦИЯ ЗАВЕРШЕННЫХ КОНТРАКТОВ")
            print(f"{'='*60}")
            print(f"44-ФЗ (открытые): перенесено {results['44_fz_migrated']}, удалено {results['44_fz_deleted']}, ссылок {results['44_fz_links_archived']}")
            print(f"44-ФЗ (awarded):  перенесено {results['44_fz_awarded_migrated']}, удалено {results['44_fz_awarded_deleted']}, ссылок {results['44_fz_awarded_links_archived']}")
            print(f"223-ФЗ:           перенесено {results['223_fz_migrated']}, удалено {results['223_fz_deleted']}")
            print(f"Всего перенесено: {total_migrated}, удалено: {total_deleted}, ссылок архивировано: {total_links}")
            print(f"{'='*60}\n")
        else:
            logger.info("Миграция завершена: новых завершенных контрактов не найдено")
            print(f"\nПроверка завершена: новых завершенных контрактов не найдено\n")

        return results

    except Exception as e:
        db.connection.rollback()
        error_msg = f"Ошибка при миграции завершенных контрактов: {e}"
        logger.error(error_msg, exc_info=True)
        results["error"] = str(e)
        print(f"\nОшибка миграции: {e}\n")
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
    db.cursor = db.connection.cursor()

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
            logger.info("Таблица reestr_contract_44_fz_completed создана")

        if not exists_223:
            logger.info("Создание таблицы reestr_contract_223_fz_completed...")
            db.cursor.execute("CREATE TABLE reestr_contract_223_fz_completed (LIKE reestr_contract_223_fz INCLUDING ALL);")
            db.connection.commit()
            logger.info("Таблица reestr_contract_223_fz_completed создана")

        if not exists_44_unknown:
            logger.info("Создание таблицы reestr_contract_44_fz_unknown...")
            db.cursor.execute("CREATE TABLE reestr_contract_44_fz_unknown (LIKE reestr_contract_44_fz INCLUDING ALL);")
            db.connection.commit()
            logger.info("Таблица reestr_contract_44_fz_unknown создана")

        if not exists_44_bad:
            logger.info("Создание таблицы reestr_contract_44_fz_bad...")
            db.cursor.execute("CREATE TABLE reestr_contract_44_fz_bad (LIKE reestr_contract_44_fz INCLUDING ALL);")
            db.connection.commit()
            logger.info("Таблица reestr_contract_44_fz_bad создана")

        return True

    except Exception as e:
        db.connection.rollback()
        error_msg = f"Ошибка при проверке/создании таблиц завершенных контрактов: {e}"
        logger.error(error_msg, exc_info=True)
        return False

    finally:
        db.close()
