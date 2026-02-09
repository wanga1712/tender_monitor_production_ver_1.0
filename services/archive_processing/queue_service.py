"""
Сервис управления очередями обработки тендеров.
Отвечает за управление очередями и приоритизацию задач.
"""

from typing import List, Dict, Any, Optional
import logging

from services.archive_runner.tender_queue_manager import TenderQueueManager

logger = logging.getLogger(__name__)


class TenderQueueService:
    """Сервис управления очередями обработки тендеров."""
    
    def __init__(self):
        """Инициализация сервиса управления очередями."""
        self.queue_manager = TenderQueueManager()
    
    def add_to_queue(self, tender_data: Dict[str, Any], priority: int = 1) -> bool:
        """
        Добавление тендера в очередь обработки.
        
        Args:
            tender_data: Данные тендера
            priority: Приоритет обработки (1-высокий, 5-низкий)
            
        Returns:
            True если добавление успешно
        """
        try:
            return self.queue_manager.add_tender(tender_data, priority)
        except Exception as e:
            logger.error(f"Ошибка добавления тендера в очередь: {e}")
            return False
    
    def get_next_tender(self) -> Optional[Dict[str, Any]]:
        """
        Получение следующего тендера для обработки.
        
        Returns:
            Данные следующего тендера или None если очередь пуста
        """
        try:
            return self.queue_manager.get_next_tender()
        except Exception as e:
            logger.error(f"Ошибка получения следующего тендера: {e}")
            return None
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Получение статистики по очередям обработки.
        
        Returns:
            Статистика очередей
        """
        try:
            return self.queue_manager.get_stats()
        except Exception as e:
            logger.error(f"Ошибка получения статистики очередей: {e}")
            return {'total': 0, 'processing': 0, 'pending': 0}
    
    def clear_queue(self) -> bool:
        """
        Очистка всех очередей обработки.
        
        Returns:
            True если очистка успешна
        """
        try:
            return self.queue_manager.clear_all()
        except Exception as e:
            logger.error(f"Ошибка очистки очередей: {e}")
            return False
    
    def prioritize_tenders(self, tender_ids: List[int], priority: int = 1) -> bool:
        """
        Установка приоритета для списка тендеров.
        
        Args:
            tender_ids: Список ID тендеров
            priority: Новый приоритет
            
        Returns:
            True если обновление успешно
        """
        try:
            return self.queue_manager.set_priority_for_tenders(tender_ids, priority)
        except Exception as e:
            logger.error(f"Ошибка установки приоритета для тендеров: {e}")
            return False
    
    def remove_from_queue(self, tender_id: int) -> bool:
        """
        Удаление тендера из очереди обработки.
        
        Args:
            tender_id: ID тендера для удаления
            
        Returns:
            True если удаление успешно
        """
        try:
            return self.queue_manager.remove_tender(tender_id)
        except Exception as e:
            logger.error(f"Ошибка удаления тендера из очереди: {e}")
            return False