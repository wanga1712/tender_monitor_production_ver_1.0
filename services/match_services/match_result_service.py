"""
MODULE: services.match_services.match_result_service
RESPONSIBILITY: Сервис сохранения результатов поиска совпадений
ALLOWED: TenderDatabaseManager, SQL запросы, валидация данных
FORBIDDEN: Бизнес-логика интереса, прямые проверки БД вне своего слоя
ERRORS: Должен пробрасывать DatabaseQueryError, DatabaseConnectionError

Сервис для сохранения результатов поиска совпадений.
Отвечает за CRUD операции с результатами совпадений.
"""

from typing import Optional, Dict, Any, List, Sequence
from datetime import datetime
from loguru import logger

from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseQueryError, DatabaseConnectionError
from psycopg2.extras import RealDictCursor


class MatchResultService:
    """Сервис сохранения результатов поиска совпадений"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        """Инициализация сервиса"""
        self.db_manager = db_manager
    
    def save_match_result(
        self,
        tender_id: int,
        registry_type: str,
        match_count: int,
        match_percentage: float,
        processing_time_seconds: Optional[float] = None,
        total_files_processed: int = 0,
        total_size_bytes: int = 0,
        error_reason: Optional[str] = None,
        folder_name: Optional[str] = None,
        has_error: bool = False,
        is_interesting: Optional[bool] = None
    ) -> Optional[int]:
        """
        Сохранение или обновление результата поиска совпадений
        
        Returns:
            ID сохраненной записи или None при ошибке
        """
        try:
            # Проверяем существование записи
            existing_id = self._get_existing_match_id(tender_id, registry_type)
            
            if existing_id:
                return self._update_match_result(
                    existing_id, match_count, match_percentage, processing_time_seconds,
                    total_files_processed, total_size_bytes, error_reason, folder_name,
                    has_error, is_interesting
                )
            else:
                return self._insert_match_result(
                    tender_id, registry_type, match_count, match_percentage, processing_time_seconds,
                    total_files_processed, total_size_bytes, error_reason, folder_name,
                    has_error, is_interesting
                )
                
        except Exception as e:
            logger.error(f"Ошибка сохранения результата совпадения: {e}")
            return None
    
    def _get_existing_match_id(self, tender_id: int, registry_type: str) -> Optional[int]:
        """Получение ID существующей записи"""
        query = """
            SELECT id FROM tender_document_matches 
            WHERE tender_id = %s AND registry_type = %s
        """
        
        try:
            result = self.db_manager.execute_query(query, (tender_id, registry_type))
            return result[0]['id'] if result else None
        except Exception as e:
            logger.error(f"Ошибка получения ID записи: {e}")
            return None
    
    def _insert_match_result(
        self,
        tender_id: int,
        registry_type: str,
        match_count: int,
        match_percentage: float,
        processing_time_seconds: Optional[float],
        total_files_processed: int,
        total_size_bytes: int,
        error_reason: Optional[str],
        folder_name: Optional[str],
        has_error: bool,
        is_interesting: Optional[bool]
    ) -> Optional[int]:
        """Вставка новой записи"""
        query = """
            INSERT INTO tender_document_matches (
                tender_id, registry_type, match_count, match_percentage,
                processing_time_seconds, total_files_processed, total_size_bytes,
                error_reason, folder_name, has_error, is_interesting, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING id
        """
        
        try:
            result = self.db_manager.execute_query(
                query, (
                    tender_id, registry_type, match_count, match_percentage,
                    processing_time_seconds, total_files_processed, total_size_bytes,
                    error_reason, folder_name, has_error, is_interesting
                )
            )
            return result[0]['id'] if result else None
        except Exception as e:
            logger.error(f"Ошибка вставки записи: {e}")
            return None
    
    def _update_match_result(
        self,
        match_id: int,
        match_count: int,
        match_percentage: float,
        processing_time_seconds: Optional[float],
        total_files_processed: int,
        total_size_bytes: int,
        error_reason: Optional[str],
        folder_name: Optional[str],
        has_error: bool,
        is_interesting: Optional[bool]
    ) -> Optional[int]:
        """Обновление существующей записи"""
        query = """
            UPDATE tender_document_matches 
            SET match_count = %s, match_percentage = %s,
                processing_time_seconds = %s, total_files_processed = %s,
                total_size_bytes = %s, error_reason = %s, folder_name = %s,
                has_error = %s, is_interesting = %s, updated_at = NOW()
            WHERE id = %s
            RETURNING id
        """
        
        try:
            result = self.db_manager.execute_query(
                query, (
                    match_count, match_percentage, processing_time_seconds,
                    total_files_processed, total_size_bytes, error_reason,
                    folder_name, has_error, is_interesting, match_id
                )
            )
            return result[0]['id'] if result else None
        except Exception as e:
            logger.error(f"Ошибка обновления записи: {e}")
            return None
    
    def save_match_details(self, match_id: int, details: Sequence[Dict[str, Any]]) -> None:
        """
        Сохранение деталей совпадений в таблицу tender_document_match_details
        
        Args:
            match_id: ID записи в tender_match_results
            details: Список деталей совпадений
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
                insert_data.append((
                    match_id,
                    detail.get("product_name"),
                    detail.get("score"),
                    detail.get("sheet_name"),
                    detail.get("row_index"),
                    detail.get("column_index"),
                    detail.get("file_name")
                ))
            
            # Выполняем массовую вставку
            if insert_data:
                self.db_manager.execute_many(insert_query, insert_data)
                logger.debug(f"Сохранено {len(insert_data)} деталей совпадений для match_id={match_id}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения деталей совпадений: {e}")
            raise
    
    def save_file_errors(self, match_id: int, file_errors: Sequence[Dict[str, Any]]) -> None:
        """
        Сохранение ошибок файлов в таблицу tender_document_file_errors
        
        Args:
            match_id: ID записи в tender_match_results
            file_errors: Список ошибок файлов
        """
        try:
            # Проверяем существование таблицы
            if not self._table_exists("tender_document_file_errors"):
                logger.warning("Таблица tender_document_file_errors не существует. Пропускаем сохранение ошибок.")
                logger.info("Для создания таблицы выполните SQL скрипт: scripts/create_tender_document_file_errors_table.sql")
                return
            
            # Если нет ошибок для сохранения, не удаляем существующие
            if not file_errors:
                logger.debug(f"Нет ошибок файлов для сохранения (match_id={match_id}), пропускаем обновление")
                return
            
            # Удаляем старые ошибки только если есть новые для вставки
            delete_query = """
                DELETE FROM tender_document_file_errors
                WHERE match_id = %s
            """
            
            try:
                self.db_manager.execute_update(delete_query, (match_id,))
            except Exception as delete_error:
                logger.error(f"Ошибка при удалении старых ошибок: {delete_error}")
                raise

            insert_query = """
                INSERT INTO tender_document_file_errors (
                    match_id,
                    file_name,
                    error_type,
                    error_message,
                    file_size,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, NOW())
            """
            
            # Подготавливаем данные для вставки
            insert_data = []
            for error in file_errors:
                insert_data.append((
                    match_id,
                    error.get("file_name"),
                    error.get("error_type"),
                    error.get("error_message"),
                    error.get("file_size")
                ))
            
            # Выполняем массовую вставку
            if insert_data:
                self.db_manager.execute_many(insert_query, insert_data)
                logger.debug(f"Сохранено {len(insert_data)} ошибок файлов для match_id={match_id}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения ошибок файлов: {e}")
            raise
    
    def _table_exists(self, table_name: str) -> bool:
        """
        Проверяет существование таблицы в базе данных.
        
        Args:
            table_name: Имя таблицы для проверки
            
        Returns:
            True если таблица существует, False в противном случае
        """
        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """
            result = self.db_manager.execute_query(query, (table_name,))
            return result[0].get('exists', False) if result else False
        except Exception as error:
            logger.debug(f"Ошибка при проверке существования таблицы {table_name}: {error}")
            return False
