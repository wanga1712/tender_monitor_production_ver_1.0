"""
Сервис для работы с деталями совпадений в документах.

Ответственность: Сохранение и получение детальной информации о совпадениях
в документах тендеров.
"""

from typing import Dict, Any, List, Sequence, Optional
from loguru import logger
from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseQueryError


class MatchDetailService:
    """Сервис для работы с деталями совпадений в документах."""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        """Инициализация сервиса деталей совпадений."""
        self.db_manager = db_manager
    
    def save_match_details(self, match_id: int, details: Sequence[Dict[str, Any]]) -> None:
        """
        Сохраняет детальные совпадения для записи tender_document_matches.
        
        Args:
            match_id: ID записи в tender_document_matches
            details: Список словарей с деталями совпадений
        """
        try:
            # Проверяем существование таблицы
            if not self._table_exists("tender_document_match_details"):
                logger.warning("Таблица tender_document_match_details не существует. Пропускаем сохранение деталей.")
                logger.info("Для создания таблицы выполните SQL скрипт: scripts/create_tender_document_match_details_table.sql")
                return
            
            # Если нет деталей для сохранения, не удаляем существующие
            if not details:
                logger.debug(f"Нет деталей для сохранения (match_id={match_id}), пропускаем обновление")
                return
            
            # Удаляем старые детали только если есть новые для вставки
            delete_query = """
                DELETE FROM tender_document_match_details
                WHERE match_id = %s
            """
            
            try:
                self.db_manager.execute_update(delete_query, (match_id,))
            except Exception as delete_error:
                logger.error(f"Ошибка при удалении старых деталей: {delete_error}")
                raise

            insert_query = """
                INSERT INTO tender_document_match_details (
                    match_id,
                    product_name,
                    score,
                    sheet_name,
                    row_index,
                    column_index,
                    file_name,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """
            
            # Подготавливаем данные для вставки
            insert_data = []
            for detail in details:
                if detail.get("score", 0) >= 56.0:  # Фильтруем только значимые совпадения
                    insert_data.append((
                        match_id,
                        detail.get("product_name", ""),
                        detail.get("score", 0.0),
                        detail.get("sheet_name", ""),
                        detail.get("row_index", 0),
                        detail.get("column_index", 0),
                        detail.get("file_name", "")
                    ))
            
            # Выполняем пакетную вставку
            if insert_data:
                self.db_manager.execute_many(insert_query, insert_data)
                logger.info(f"Сохранено {len(insert_data)} деталей совпадений для match_id={match_id}")
            else:
                logger.debug(f"Нет деталей для сохранения после фильтрации (match_id={match_id})")
                
        except Exception as e:
            logger.error(f"Ошибка при сохранении деталей совпадений: {e}")
            raise
    
    def get_match_details(self, match_id: int) -> List[Dict[str, Any]]:
        """
        Получает детали совпадений для указанного match_id.
        
        Args:
            match_id: ID записи в tender_document_matches
            
        Returns:
            Список словарей с деталями совпадений
        """
        pass
    
    def _table_exists(self, table_name: str) -> bool:
        """Проверяет существование таблицы в базе данных."""
        pass