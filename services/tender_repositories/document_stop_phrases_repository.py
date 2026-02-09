"""
MODULE: services.tender_repositories.document_stop_phrases_repository
RESPONSIBILITY: Manage document analysis stop phrases in DB.
ALLOWED: typing, loguru, psycopg2.extras, core.tender_database.
FORBIDDEN: Business logic outside DB operations.
ERRORS: Database exceptions.

Репозиторий для работы со стоп-фразами анализа документации.
"""

from typing import List, Dict, Any, Optional

from loguru import logger
from psycopg2.extras import RealDictCursor

from core.tender_database import TenderDatabaseManager


class DocumentStopPhrasesRepository:
    """Репозиторий для работы со стоп-фразами анализа документации."""

    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager

    def get_document_stop_phrases(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Получение стоп-фраз пользователя для анализа документации.
        """
        try:
            query = """
                SELECT
                    id,
                    user_id,
                    phrase,
                    setting_id
                FROM document_stop_phrases
                WHERE user_id = %s
                ORDER BY phrase
            """
            results = self.db_manager.execute_query(
                query,
                (user_id,),
                RealDictCursor,
            )
            return [dict(row) for row in results] if results else []
        except Exception as error:
            logger.error(f"Ошибка при получении стоп-фраз пользователя: {error}")
            return []

    def add_document_stop_phrases(
        self,
        user_id: int,
        phrases: List[str],
        setting_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Добавление стоп-фраз для пользователя.

        Returns:
            Dict с полями added, skipped, errors.
        """
        result = {"added": 0, "skipped": 0, "errors": []}

        for phrase in phrases:
            text = phrase.strip()
            if not text:
                continue

            try:
                check_query = """
                    SELECT id
                    FROM document_stop_phrases
                    WHERE user_id = %s AND LOWER(phrase) = LOWER(%s)
                """
                existing = self.db_manager.execute_query(
                    check_query,
                    (user_id, text),
                )

                if existing:
                    result["skipped"] += 1
                    logger.debug(
                        "Стоп-фраза '%s' уже добавлена для пользователя %s",
                        text,
                        user_id,
                    )
                    continue

                insert_query = """
                    INSERT INTO document_stop_phrases (user_id, phrase, setting_id)
                    VALUES (%s, %s, %s)
                """
                self.db_manager.execute_update(
                    insert_query,
                    (user_id, text, setting_id),
                )
                result["added"] += 1
                logger.info(
                    "Добавлена стоп-фраза '%s' для пользователя %s",
                    text,
                    user_id,
                )
            except Exception as error:
                error_msg = f"Ошибка при добавлении стоп-фразы '{text}': {error}"
                logger.error(error_msg)
                result["errors"].append(error_msg)

        return result

    def remove_document_stop_phrase(self, user_id: int, phrase_id: int) -> bool:
        """
        Удаление стоп-фразы пользователя.
        """
        try:
            query = """
                DELETE FROM document_stop_phrases
                WHERE id = %s AND user_id = %s
            """
            self.db_manager.execute_update(query, (phrase_id, user_id))
            logger.info(
                "Удалена стоп-фраза (id=%s) для пользователя %s",
                phrase_id,
                user_id,
            )
            return True
        except Exception as error:
            logger.error(f"Ошибка при удалении стоп-фразы: {error}")
            return False


