import json
import re
import psycopg2
from typing import List, Dict, Set
from pathlib import Path
from database_work.database_connection import DatabaseManager
from utils.logger_config import get_logger

logger = get_logger()

REMOTE_DB_CONFIG = {
    "host": "localhost",
    "database": "product_catalog_2",
    "user": "postgres",
    "password": "0IFz3_",
    "port": "5432"
}

class KeywordMatcher:
    def __init__(self, user_keywords_file: str = "user_keywords.json"):
        self.db_manager = DatabaseManager()
        self.user_keywords_file = Path(user_keywords_file)
        self.keywords: Set[str] = set()
        self.load_keywords()

    def load_keywords(self):
        """Loads keywords from Remote DB and JSON file."""
        self.keywords.clear()
        
        # 1. Load from Remote DB (product_catalog_2 -> products -> name)
        try:
            logger.info("Connecting to remote product catalog database...")
            with psycopg2.connect(**REMOTE_DB_CONFIG) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT name FROM products")
                    db_keywords = {row[0].lower().strip() for row in cursor.fetchall() if row[0]}
                    self.keywords.update(db_keywords)
                    logger.info(f"Loaded {len(db_keywords)} keywords from Remote DB products table")
        except Exception as e:
            logger.error(f"Error loading keywords from Remote DB: {e}")

        # 2. Load from JSON
        if self.user_keywords_file.exists():
            try:
                with open(self.user_keywords_file, 'r', encoding='utf-8') as f:
                    user_data = json.load(f)
                    # Assuming simple list of strings or dict with 'keywords'
                    if isinstance(user_data, list):
                        json_keywords = {k.lower().strip() for k in user_data}
                    elif isinstance(user_data, dict) and 'keywords' in user_data:
                        json_keywords = {k.lower().strip() for k in user_data['keywords']}
                    else:
                        json_keywords = set()
                    
                    self.keywords.update(json_keywords)
                    logger.info(f"Loaded {len(json_keywords)} keywords from JSON")
            except Exception as e:
                logger.error(f"Error loading keywords from JSON: {e}")
        else:
            logger.warning(f"User keywords file not found: {self.user_keywords_file}")

    def find_matches(self, text: str) -> List[Dict]:
        """
        Finds occurrences of keywords in text.
        Returns list of matches with context.
        """
        matches = []
        text_lower = text.lower()
        
        for keyword in self.keywords:
            if not keyword:
                continue
                
            # Simple string search first (faster)
            start_idx = text_lower.find(keyword)
            if start_idx != -1:
                # Found a match
                # Extract context (e.g. 50 chars before and after)
                context_start = max(0, start_idx - 50)
                context_end = min(len(text), start_idx + len(keyword) + 50)
                context = text[context_start:context_end].replace('\n', ' ').strip()
                
                matches.append({
                    "keyword": keyword,
                    "context": context
                })
        
        return matches

    def save_matches(self, queue_id: int, file_name: str, matches: List[Dict]):
        """Saves matches to match_repository."""
        if not matches:
            return

        try:
            with self.db_manager.connection.cursor() as cursor:
                query = """
                    INSERT INTO match_repository (queue_id, file_name, keyword_found, context)
                    VALUES (%s, %s, %s, %s)
                """
                data = [(queue_id, file_name, m['keyword'], m['context']) for m in matches]
                cursor.executemany(query, data)
                self.db_manager.connection.commit()
                logger.info(f"Saved {len(matches)} matches for queue_id {queue_id}")
        except Exception as e:
            self.db_manager.connection.rollback()
            logger.error(f"Error saving matches: {e}")

    def close(self):
        self.db_manager.close()
