"""
MODULE: services.archive_runner.processed_tenders_repository
RESPONSIBILITY: Repository for tracking processed tenders and files validity.
ALLOWED: TenderDatabaseManager, logging.
FORBIDDEN: Complex business logic (keep it CRUD-like).
ERRORS: None.

Репозиторий для отслеживания обработанных торгов и файлов.
"""

from typing import List, Dict, Any, Optional, Tuple
from loguru import logger
from psycopg2.extras import RealDictCursor

from core.tender_database import TenderDatabaseManager


class ProcessedTendersRepository:
    """Репозиторий для работы с обработанными торгами и файлами."""

    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager

    def is_tender_processed(
        self,
        tender_id: int,
        registry_type: str,
        folder_name: str,
        worker_id: Optional[str] = None,
    ) -> bool:
        """
        Проверяет, была ли обработана торг.
        Использует данные из tender_document_matches.
        
        Args:
            tender_id: ID торга
            registry_type: Тип реестра ('44fz', '223fz')
            folder_name: Имя папки торга (используется для совместимости, но не для фильтрации)
            
        Returns:
            True если торг уже обработана
        """
        # Нормализация типа реестра для запросов к БД
        reg_type_for_query = registry_type.split("_")[0] if registry_type else registry_type

        query = """
            SELECT error_reason, worker_id
            FROM tender_document_matches
            WHERE tender_id = %s AND registry_type = %s
        """

        try:
            result = self.db_manager.execute_query(query, (tender_id, reg_type_for_query))
            if not result:
                return False
            row = result[0]
            if row.get("error_reason") == "PROCESSING":
                existing_worker_id = row.get("worker_id")
                if worker_id and existing_worker_id == worker_id:
                    # Если обрабатывается текущим воркером - считаем НЕ обработанным,
                    # чтобы разрешить продолжение работы
                    return False
                # Если обрабатывается другим воркером - считаем обработанным (занятым)
                return True
            
            # Если есть запись и это не PROCESSING - значит уже обработан (COMPLETED, FAILED и т.д.)
            # НО: если это FAILED, мы можем захотеть повторить обработку?
            # Пока считаем True (обработан), если нужно переобрабатывать ошибки, нужно менять логику.
            return True
        except Exception as e:
            logger.warning(f"Ошибка проверки обработки торга {tender_id}: {e}")
            return False

    def mark_tender_processed(self, tender_id: int, registry_type: str, folder_name: str,
                             user_id: int, machine_id: Optional[str] = None,
                             error_message: Optional[str] = None) -> None:
        """
        Отмечает торг как обработанную.
        Использует tender_document_matches - просто логируем, данные уже там.

        Args:
            tender_id: ID торга
            registry_type: Тип реестра
            folder_name: Имя папки
            user_id: ID пользователя
            machine_id: ID машины (опционально)
            error_message: Сообщение об ошибке если обработка не удалась
        """
        status = 'failed' if error_message else 'completed'
        logger.debug(f"Торг {tender_id} ({registry_type}) отмечена как {status} (данные в tender_document_matches)")

        # Если есть ошибка, можно обновить error_reason в tender_document_matches
        if error_message:
            try:
                query = """
                    UPDATE tender_document_matches
                    SET error_reason = %s, folder_name = %s
                    WHERE tender_id = %s AND registry_type = %s
                """
                self.db_manager.execute_update(query, (f"processing_error: {error_message}", folder_name, tender_id, registry_type))
            except Exception as e:
                logger.warning(f"Ошибка обновления error_reason для торга {tender_id}: {e}")
        else:
            # Обновим folder_name для корректности
            try:
                query = """
                    UPDATE tender_document_matches
                    SET folder_name = %s
                    WHERE tender_id = %s AND registry_type = %s
                """
                self.db_manager.execute_update(query, (folder_name, tender_id, registry_type))
            except Exception as e:
                logger.debug(f"Не удалось обновить folder_name для торга {tender_id}: {e}")

    def mark_file_processed(self, tender_id: int, registry_type: str, file_path: str,
                           file_name: str, file_size: Optional[int], user_id: int,
                           machine_id: Optional[str] = None, error_message: Optional[str] = None) -> None:
        """
        Отмечает файл как обработанный.
        В текущей реализации файлы отслеживаются через tender_document_matches,
        поэтому этот метод просто логирует.
        """
        status = 'failed' if error_message else 'completed'
        logger.debug(f"Файл {file_name} отмечен как {status} (отслеживается через tender_document_matches)")

    def get_processing_stats(self, user_id: Optional[int] = None, days: int = 7) -> Dict[str, Any]:
        """
        Получает статистику обработки за последние дни из tender_document_matches.

        Args:
            user_id: ID пользователя (None для всех)
            days: Количество дней для статистики

        Returns:
            Словарь со статистикой
        """
        user_filter = "AND tdm.user_id = %s" if user_id else ""
        params = [days]
        if user_id:
            params.append(user_id)

        query = f"""
            SELECT
                COUNT(CASE WHEN tdm.is_interesting IS NOT NULL THEN 1 END) as completed_tenders,
                COUNT(CASE WHEN tdm.error_reason IS NOT NULL THEN 1 END) as failed_tenders,
                COUNT(*) as total_tenders,
                COUNT(DISTINCT tdm.folder_name) as unique_folders
            FROM tender_document_matches tdm
            WHERE tdm.processed_at >= CURRENT_TIMESTAMP - INTERVAL '%s days'
            {user_filter}
        """

        try:
            result = self.db_manager.execute_query(query, tuple(params))
            if result:
                row = result[0]
                return {
                    'completed_tenders': row['completed_tenders'] or 0,
                    'failed_tenders': row['failed_tenders'] or 0,
                    'total_tenders': row['total_tenders'] or 0,
                    'unique_folders': row['unique_folders'] or 0,
                    'period_days': days
                }
        except Exception as e:
            logger.error(f"Ошибка получения статистики обработки: {e}")

        return {
            'completed_tenders': 0,
            'failed_tenders': 0,
            'total_tenders': 0,
            'unique_folders': 0,
            'period_days': days
        }

    def cleanup_old_records(self, days_to_keep: int = 90) -> int:
        """
        Очищает старые записи обработки.
        В текущей реализации использует tender_document_matches,
        поэтому очистка не производится (данные нужны для истории).

        Args:
            days_to_keep: Количество дней для хранения записей

        Returns:
            Всегда возвращает 0 (очистка не производится)
        """
        logger.info("Очистка старых записей отключена - используются данные из tender_document_matches")
        return 0

    def insert_stop_word_match(self, tender_id: int, registry_type: str, user_id: int) -> None:
        """
        Создает запись в tender_document_matches для торга, отфильтрованного по стоп-словам.
        Это предотвращает повторную выборку этого торга.
        """
        query = """
            INSERT INTO tender_document_matches (
                tender_id, registry_type, user_id, 
                processed_at, created_at, 
                is_interesting, has_error, error_reason
            )
            VALUES (
                %s, %s, %s,
                NOW(), NOW(),
                FALSE, FALSE, 'stop_word_filter'
            )
            ON CONFLICT (tender_id, registry_type) DO NOTHING
        """
        try:
            self.db_manager.execute_update(query, (tender_id, registry_type, user_id))
            logger.debug(f"Торг {tender_id} ({registry_type}) помечен как игнорируемый (стоп-слово)")
        except Exception as e:
            logger.error(f"Ошибка при создании записи о стоп-слове для {tender_id}: {e}")

    def get_processed_tenders_batch(
        self,
        tender_ids_by_registry: Dict[str, List[int]],
        worker_id: Optional[str] = None,
    ) -> Dict[Tuple[int, str], bool]:
        """
        Батч-проверка обработанных торгов.
        
        Args:
            tender_ids_by_registry: Словарь {registry_type: [id1, id2, ...]}
            
        Returns:
            Словарь {(tender_id, registry_type): is_processed}
        """
        result = {}
        if not tender_ids_by_registry:
            return result
            
        for registry_type, tender_ids in tender_ids_by_registry.items():
            if not tender_ids:
                continue
                
            query = """
                SELECT tender_id, error_reason, worker_id
                FROM tender_document_matches
                WHERE registry_type = %s 
                  AND tender_id = ANY(%s)
                  AND COALESCE(has_error, FALSE) = FALSE
            """
            
            try:
                rows = self.db_manager.execute_query(query, (registry_type, tender_ids))
                processing_map = {}
                for row in rows:
                    processing_map[row["tender_id"]] = {
                        "error_reason": row.get("error_reason"),
                        "worker_id": row.get("worker_id"),
                    }
                for tender_id in tender_ids:
                    row = processing_map.get(tender_id)
                    if not row:
                        result[(tender_id, registry_type)] = False
                        continue
                    if row.get("error_reason") == "PROCESSING":
                        existing_worker_id = row.get("worker_id")
                        if worker_id and existing_worker_id == worker_id:
                            result[(tender_id, registry_type)] = False
                        else:
                            result[(tender_id, registry_type)] = True
                    else:
                        result[(tender_id, registry_type)] = True
                    
            except Exception as e:
                logger.error(f"Ошибка при батч-проверке обработанных торгов ({registry_type}): {e}")
                # В случае ошибки считаем, что не обработаны (безопасный фоллбэк)
                for tender_id in tender_ids:
                    result[(tender_id, registry_type)] = False
                    
        return result
