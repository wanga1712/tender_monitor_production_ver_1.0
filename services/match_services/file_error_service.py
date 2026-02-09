"""
Сервис для работы с ошибками обработки файлов.

Ответственность: Сохранение и управление ошибками, возникающими
при обработке файлов в тендерах.
"""

from typing import Dict, Any, List, Optional
from loguru import logger
from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseQueryError


class FileErrorService:
    """Сервис для работы с ошибками обработки файлов."""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        """Инициализация сервиса ошибок файлов."""
        self.db_manager = db_manager
    
    def save_file_errors(
        self,
        match_id: int,
        failed_files: List[Dict[str, Any]],
    ) -> None:
        """
        Сохраняет информацию об ошибках обработки файлов.
        
        Args:
            match_id: ID записи результата обработки из tender_document_matches
            failed_files: Список проблемных файлов, каждый элемент:
                {"path": str, "error": str, "file_size_mb": float}
        """
        if not failed_files:
            return
        
        # Проверяем существование таблицы
        if not self._table_exists("tender_document_file_errors"):
            logger.warning("Таблица tender_document_file_errors не существует, пропускаем сохранение ошибок файлов")
            return
        
        try:
            # Удаляем старые записи об ошибках для этого match_id
            delete_query = """
                DELETE FROM tender_document_file_errors
                WHERE match_id = %s
            """
            self.db_manager.execute_update(delete_query, (match_id,))
            
            # Вставляем новые записи об ошибках
            for failed_file in failed_files:
                file_path = failed_file.get("path", "")
                file_name = Path(file_path).name if file_path else "unknown"
                error_message = failed_file.get("error", "Неизвестная ошибка")
                file_size_mb = failed_file.get("file_size_mb", 0)
                file_size_bytes = int(file_size_mb * 1024 * 1024) if file_size_mb else None
                error_type = self.get_error_type(error_message)
                
                insert_query = """
                    INSERT INTO tender_document_file_errors (
                        match_id, file_name, file_path, error_message,
                        error_type, file_size_bytes, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """
                
                self.db_manager.execute_update(
                    insert_query,
                    (match_id, file_name, file_path, error_message, error_type, file_size_bytes)
                )
                
            logger.info(f"Сохранено {len(failed_files)} ошибок файлов для match_id={match_id}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении ошибок файлов: {e}")
    
    def get_error_type(self, error_msg: str) -> str:
        """
        Определяет тип ошибки на основе сообщения об ошибке.
        
        Args:
            error_msg: Сообщение об ошибке
            
        Returns:
            Тип ошибки ('cuda_error', 'timeout', 'open_error', 'parse_error', 'unknown_error')
        """
        error_lower = error_msg.lower()
        if "cuda" in error_lower or "gpu" in error_lower:
            return "cuda_error"
        elif "timeout" in error_lower or "таймаут" in error_lower:
            return "timeout"
        elif "open" in error_lower or "открыть" in error_lower:
            return "open_error"
        elif "parse" in error_lower or "парсинг" in error_lower:
            return "parse_error"
        else:
            return "unknown_error"
    
    def _table_exists(self, table_name: str) -> bool:
        """Проверяет существование таблицы в базе данных."""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """
        try:
            result = self.db_manager.execute_query(query, (table_name,))
            return result[0]['exists'] if result else False
        except Exception as e:
            logger.error(f"Ошибка проверки существования таблицы {table_name}: {e}")
            return False