"""
MODULE: services.tender_repositories.stop_words_repository
RESPONSIBILITY: Manage user stop words in DB.
ALLOWED: typing, loguru, core.tender_database, psycopg2.extras.
FORBIDDEN: Business logic outside DB operations.
ERRORS: Database exceptions.

Репозиторий для работы со стоп-словами пользователя.
"""

from typing import List, Dict, Any, Optional
from loguru import logger
from core.tender_database import TenderDatabaseManager
from psycopg2.extras import RealDictCursor


class StopWordsRepository:
    """Репозиторий для работы со стоп-словами пользователя"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
    
    def get_user_stop_words(self, user_id: int) -> List[Dict[str, Any]]:
        """Получение стоп-слов пользователя"""
        try:
            query = """
                SELECT 
                    id,
                    user_id,
                    stop_word,
                    setting_id
                FROM stop_words_names
                WHERE user_id = %s
                ORDER BY stop_word
            """
            results = self.db_manager.execute_query(
                query,
                (user_id,),
                RealDictCursor
            )
            return [dict(row) for row in results] if results else []
            
        except Exception as e:
            logger.error(f"Ошибка при получении стоп-слов пользователя: {e}")
            return []
    
    def add_user_stop_words(
        self,
        user_id: int,
        stop_words: List[str],
        setting_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Добавление стоп-слов для пользователя"""
        result = {'added': 0, 'skipped': 0, 'errors': []}
        
        for stop_word in stop_words:
            stop_word = stop_word.strip()
            if not stop_word:
                continue
            
            try:
                check_query = """
                    SELECT id FROM stop_words_names 
                    WHERE user_id = %s AND LOWER(stop_word) = LOWER(%s)
                """
                existing = self.db_manager.execute_query(
                    check_query,
                    (user_id, stop_word)
                )
                
                if existing:
                    result['skipped'] += 1
                    logger.debug(f"Стоп-слово '{stop_word}' уже добавлено для пользователя {user_id}")
                    continue
                
                insert_query = """
                    INSERT INTO stop_words_names (user_id, stop_word, setting_id)
                    VALUES (%s, %s, %s)
                """
                self.db_manager.execute_update(
                    insert_query,
                    (user_id, stop_word, setting_id)
                )
                result['added'] += 1
                logger.info(f"Добавлено стоп-слово '{stop_word}' для пользователя {user_id}")
                
            except Exception as e:
                error_msg = f"Ошибка при добавлении стоп-слова '{stop_word}': {e}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
        
        return result
    
    def remove_user_stop_word(self, user_id: int, stop_word_id: int) -> bool:
        """Удаление стоп-слова пользователя"""
        try:
            query = """
                DELETE FROM stop_words_names 
                WHERE id = %s AND user_id = %s
            """
            self.db_manager.execute_update(query, (stop_word_id, user_id))
            logger.info(f"Удалено стоп-слово (id={stop_word_id}) для пользователя {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при удалении стоп-слова: {e}")
            return False

