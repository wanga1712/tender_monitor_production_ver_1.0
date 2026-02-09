"""
MODULE: services.match_services.tender_lock_service
RESPONSIBILITY: Сервис управления блокировками тендеров
ALLOWED: TenderDatabaseManager, SQL запросы, управление блокировками
FORBIDDEN: Бизнес-логика вне блокировок, прямые операции с файлами
ERRORS: Должен пробрасывать DatabaseQueryError, DatabaseConnectionError

Сервис для управления блокировками тендеров.
Отвечает за приобретение и освобождение блокировок тендеров.
"""

from typing import Optional, Dict, Any, List
from loguru import logger
from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseQueryError, DatabaseConnectionError


class TenderLockService:
    """Сервис управления блокировками тендеров"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        """Инициализация сервиса"""
        self.db_manager = db_manager
    
    def acquire_tender_lock(self, tender_id: int, registry_type: str, tender_type: str = 'new') -> bool:
        """
        Приобретение блокировки тендера
        
        Args:
            tender_id: ID закупки
            registry_type: Тип реестра ('44fz' или '223fz')
            tender_type: Тип торгов ('new' или 'won')
            
        Returns:
            True если блокировка приобретена, False при ошибке
        """
        query = """
            INSERT INTO tender_locks (tender_id, registry_type, tender_type, locked_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (tender_id, registry_type, tender_type) 
            DO UPDATE SET locked_at = NOW()
            WHERE tender_locks.locked_at < NOW() - INTERVAL '30 minutes'
            RETURNING id
        """
        
        try:
            result = self.db_manager.execute_query(
                query, (tender_id, registry_type, tender_type)
            )
            
            if result:
                logger.info(f"Блокировка приобретена: tender_id={tender_id}, "
                          f"registry_type={registry_type}, tender_type={tender_type}")
                return True
            else:
                logger.warning(f"Блокировка уже активна или не удалось приобрести: "
                             f"tender_id={tender_id}, registry_type={registry_type}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка приобретения блокировки: {e}")
            return False
    
    def release_tender_lock(self, tender_id: int, registry_type: str, tender_type: str = 'new') -> bool:
        """
        Освобождение блокировки тендера
        
        Args:
            tender_id: ID закупки
            registry_type: Тип реестра ('44fz' или '223fz')
            tender_type: Тип торгов ('new' или 'won')
            
        Returns:
            True если блокировка освобождена, False при ошибке
        """
        query = """
            DELETE FROM tender_locks 
            WHERE tender_id = %s AND registry_type = %s AND tender_type = %s
            RETURNING id
        """
        
        try:
            result = self.db_manager.execute_query(
                query, (tender_id, registry_type, tender_type)
            )
            
            if result:
                logger.info(f"Блокировка освобождена: tender_id={tender_id}, "
                          f"registry_type={registry_type}, tender_type={tender_type}")
                return True
            else:
                logger.warning(f"Блокировка не найдена для освобождения: "
                             f"tender_id={tender_id}, registry_type={registry_type}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка освобождения блокировки: {e}")
            return False
    
    def check_tender_lock(self, tender_id: int, registry_type: str, tender_type: str = 'new') -> bool:
        """
        Проверка активной блокировки тендера
        
        Returns:
            True если блокировка активна, False если нет блокировки или она устарела
        """
        query = """
            SELECT id FROM tender_locks 
            WHERE tender_id = %s AND registry_type = %s AND tender_type = %s
            AND locked_at > NOW() - INTERVAL '30 minutes'
        """
        
        try:
            result = self.db_manager.execute_query(
                query, (tender_id, registry_type, tender_type)
            )
            return bool(result)
                
        except Exception as e:
            logger.error(f"Ошибка проверки блокировки: {e}")
            return False
    
    def get_lock_info(self, tender_id: int, registry_type: str, tender_type: str = 'new') -> Optional[Dict[str, Any]]:
        """
        Получение информации о блокировке
        
        Returns:
            Информация о блокировке или None если блокировки нет
        """
        query = """
            SELECT id, tender_id, registry_type, tender_type, locked_at
            FROM tender_locks 
            WHERE tender_id = %s AND registry_type = %s AND tender_type = %s
            AND locked_at > NOW() - INTERVAL '30 minutes'
        """
        
        try:
            result = self.db_manager.execute_query(
                query, (tender_id, registry_type, tender_type)
            )
            return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Ошибка получения информации о блокировке: {e}")
            return None
    
    def cleanup_expired_locks(self) -> int:
        """
        Очистка устаревших блокировок
        
        Returns:
            Количество удаленных устаревших блокировок
        """
        query = """
            DELETE FROM tender_locks 
            WHERE locked_at <= NOW() - INTERVAL '30 minutes'
            RETURNING id
        """
        
        try:
            result = self.db_manager.execute_query(query)
            count = len(result) if result else 0
            
            if count > 0:
                logger.info(f"Очищено {count} устаревших блокировок")
            
            return count
                
        except Exception as e:
            logger.error(f"Ошибка очистки устаревших блокировок: {e}")
            return 0
    
    def bulk_release_locks(self, tender_locks: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Массовое освобождение блокировок
        
        Args:
            tender_locks: Список блокировок для освобождения
                [{'tender_id': 123, 'registry_type': '44fz', 'tender_type': 'new'}, ...]
                
        Returns:
            Статистика освобождения: {'successful': X, 'failed': Y}
        """
        if not tender_locks:
            return {'successful': 0, 'failed': 0}
        
        successful = 0
        failed = 0
        
        for lock_info in tender_locks:
            tender_id = lock_info.get('tender_id')
            registry_type = lock_info.get('registry_type')
            tender_type = lock_info.get('tender_type', 'new')
            
            if tender_id is not None and registry_type:
                if self.release_tender_lock(tender_id, registry_type, tender_type):
                    successful += 1
                else:
                    failed += 1
            else:
                failed += 1
                logger.warning(f"Невалидные данные для массового освобождения: {lock_info}")
        
        logger.info(f"Массовое освобождение блокировок: успешно {successful}, неудачно {failed}")
        return {'successful': successful, 'failed': failed}