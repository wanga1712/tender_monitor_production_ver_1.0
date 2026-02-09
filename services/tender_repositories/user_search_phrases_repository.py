"""
MODULE: services.tender_repositories.user_search_phrases_repository
RESPONSIBILITY: Manage user search phrases.
ALLOWED: typing, loguru, psycopg2.extras, core.tender_database.
FORBIDDEN: Business logic outside DB operations.
ERRORS: Database exceptions.

Репозиторий для работы с пользовательскими фразами для поиска по документации.
"""

from typing import List, Dict, Any, Optional

from loguru import logger
from psycopg2.extras import RealDictCursor

from core.tender_database import TenderDatabaseManager


class UserSearchPhrasesRepository:
    """Репозиторий для работы с пользовательскими фразами для поиска по документации."""

    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager

    def get_user_search_phrases(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Получение пользовательских фраз для поиска по документации.
        """
        try:
            query = """
                SELECT
                    id,
                    user_id,
                    phrase,
                    setting_id
                FROM user_search_phrases
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
            logger.error(f"Ошибка при получении пользовательских фраз поиска: {error}")
            return []

    def add_user_search_phrases(
        self,
        user_id: int,
        phrases: List[str],
        setting_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Добавление пользовательских фраз для поиска.

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
                    FROM user_search_phrases
                    WHERE user_id = %s AND LOWER(phrase) = LOWER(%s)
                """
                existing = self.db_manager.execute_query(
                    check_query,
                    (user_id, text),
                )

                if existing:
                    result["skipped"] += 1
                    logger.debug(
                        "Фраза поиска '%s' уже добавлена для пользователя %s",
                        text,
                        user_id,
                    )
                    continue

                insert_query = """
                    INSERT INTO user_search_phrases (user_id, phrase, setting_id)
                    VALUES (%s, %s, %s)
                """
                self.db_manager.execute_update(
                    insert_query,
                    (user_id, text, setting_id),
                )
                result["added"] += 1
                logger.info(
                    "Добавлена фраза поиска '%s' для пользователя %s",
                    text,
                    user_id,
                )
            except Exception as error:
                error_msg = f"Ошибка при добавлении фразы поиска '{text}': {error}"
                logger.error(error_msg)
                result["errors"].append(error_msg)

        return result

    def remove_user_search_phrase(self, user_id: int, phrase_id: int) -> bool:
        """
        Удаление пользовательской фразы поиска.
        """
        try:
            query = """
                DELETE FROM user_search_phrases
                WHERE id = %s AND user_id = %s
            """
            self.db_manager.execute_update(query, (phrase_id, user_id))
            logger.info(
                "Удалена фраза поиска (id=%s) для пользователя %s",
                phrase_id,
                user_id,
            )
            return True
        except Exception as error:
            logger.error(f"Ошибка при удалении фразы поиска: {error}")
            return False

