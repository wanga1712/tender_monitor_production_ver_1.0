"""
MODULE: services.match_services.match_status_service
RESPONSIBILITY: Сервис управления статусами интереса результатов поиска совпадений
ALLOWED: TenderDatabaseManager, SQL запросы, бизнес-логика статусов
FORBIDDEN: Прямые операции с файлами, сложная бизнес-логика вне статусов
ERRORS: Должен пробрасывать DatabaseQueryError, DatabaseConnectionError

Сервис для управления статусами интереса результатов поиска совпадений.
Отвечает за установку, проверку и фильтрацию статусов интереса.
"""

from typing import List, Dict, Any, Optional
from loguru import logger

from core.tender_database import TenderDatabaseManager
from core.exceptions import DatabaseQueryError, DatabaseConnectionError


class MatchStatusService:
    """Сервис управления статусами интереса результатов поиска совпадений"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        """Инициализация сервиса"""
        self.db_manager = db_manager
    
    def set_interesting_status(self, tender_id: int, registry_type: str, is_interesting: bool) -> bool:
        """
        Установка статуса интереса для результата совпадения
        
        Args:
            tender_id: ID закупки
            registry_type: Тип реестра ('44fz' или '223fz')
            is_interesting: True - интересно, False - не интересно
            
        Returns:
            True если успешно, False при ошибке
        """
        query = """
            UPDATE tender_document_matches 
            SET is_interesting = %s, updated_at = NOW()
            WHERE tender_id = %s AND registry_type = %s
        """
        
        try:
            result = self.db_manager.execute_query(
                query, (is_interesting, tender_id, registry_type)
            )
            
            if result:
                logger.info(f"Статус интереса установлен: tender_id={tender_id}, "
                          f"registry_type={registry_type}, is_interesting={is_interesting}")
                return True
            else:
                logger.warning(f"Запись не найдена для установки статуса: "
                             f"tender_id={tender_id}, registry_type={registry_type}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка установки статуса интереса: {e}")
            return False
    
    def get_interesting_status(self, tender_id: int, registry_type: str) -> Optional[bool]:
        """
        Получение текущего статуса интереса
        
        Returns:
            True - интересно, False - не интересно, None - запись не найдена
        """
        query = """
            SELECT is_interesting FROM tender_document_matches 
            WHERE tender_id = %s AND registry_type = %s
        """
        
        try:
            result = self.db_manager.execute_query(query, (tender_id, registry_type))
            
            if result:
                return result[0]['is_interesting']
            else:
                return None
                
        except Exception as e:
            logger.error(f"Ошибка получения статуса интереса: {e}")
            return None
    
    def filter_uninteresting_tenders(self, tenders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Фильтрация неинтересных тендеров из списка
        
        Args:
            tenders: Список тендеров для фильтрации
            
        Returns:
            Отфильтрованный список только с интересными тендерами
        """
        if not tenders:
            return []
        
        # Собираем ID тендеров для проверки
        tender_ids_44fz = []
        tender_ids_223fz = []
        
        for tender in tenders:
            tender_id = tender.get('id')
            registry_type = tender.get('registry_type')
            
            if tender_id and registry_type:
                if registry_type == '44fz':
                    tender_ids_44fz.append(tender_id)
                elif registry_type == '223fz':
                    tender_ids_223fz.append(tender_id)
        
        # Получаем неинтересные тендеры
        uninteresting_tenders = self._get_uninteresting_tenders_batch(tender_ids_44fz, tender_ids_223fz)
        
        # Фильтруем исходный список
        filtered_tenders = []
        
        for tender in tenders:
            tender_id = tender.get('id')
            registry_type = tender.get('registry_type')
            
            if not tender_id or not registry_type:
                # Если нет ID или типа реестра, оставляем как есть
                filtered_tenders.append(tender)
            else:
                # Проверяем, не является ли тендер неинтересным
                is_uninteresting = any(
                    t['tender_id'] == tender_id and t['registry_type'] == registry_type
                    for t in uninteresting_tenders
                )
                
                if not is_uninteresting:
                    filtered_tenders.append(tender)
        
        logger.info(f"Фильтрация неинтересных тендеров: было {len(tenders)}, стало {len(filtered_tenders)}")
        return filtered_tenders
    
    def _get_uninteresting_tenders_batch(
        self, 
        tender_ids_44fz: List[int], 
        tender_ids_223fz: List[int]
    ) -> List[Dict[str, Any]]:
        """Получение неинтересных тендеров партией"""
        uninteresting_tenders = []
        
        # Проверяем 44-ФЗ тендеры
        if tender_ids_44fz:
            query_44fz = """
                SELECT tender_id, registry_type FROM tender_document_matches 
                WHERE tender_id = ANY(%s) AND registry_type = '44fz' AND is_interesting = FALSE
            """
            
            try:
                result = self.db_manager.execute_query(query_44fz, (tender_ids_44fz,))
                if result:
                    uninteresting_tenders.extend(result)
            except Exception as e:
                logger.error(f"Ошибка получения неинтересных тендеров 44fz: {e}")
        
        # Проверяем 223-ФЗ тендеры
        if tender_ids_223fz:
            query_223fz = """
                SELECT tender_id, registry_type FROM tender_document_matches 
                WHERE tender_id = ANY(%s) AND registry_type = '223fz' AND is_interesting = FALSE
            """
            
            try:
                result = self.db_manager.execute_query(query_223fz, (tender_ids_223fz,))
                if result:
                    uninteresting_tenders.extend(result)
            except Exception as e:
                logger.error(f"Ошибка получения неинтересных тендеров 223fz: {e}")
        
        return uninteresting_tenders
    
    def bulk_update_interesting_status(
        self, 
        tender_updates: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Массовое обновление статусов интереса
        
        Args:
            tender_updates: Список словарей с обновлениями
                [{'tender_id': 123, 'registry_type': '44fz', 'is_interesting': True}, ...]
                
        Returns:
            Статистика обновлений: {'successful': X, 'failed': Y}
        """
        if not tender_updates:
            return {'successful': 0, 'failed': 0}
        
        successful = 0
        failed = 0
        
        for update in tender_updates:
            tender_id = update.get('tender_id')
            registry_type = update.get('registry_type')
            is_interesting = update.get('is_interesting')
            
            if tender_id is not None and registry_type and is_interesting is not None:
                if self.set_interesting_status(tender_id, registry_type, is_interesting):
                    successful += 1
                else:
                    failed += 1
            else:
                failed += 1
                logger.warning(f"Невалидные данные для массового обновления: {update}")
        
        logger.info(f"Массовое обновление статусов: успешно {successful}, неудачно {failed}")
        return {'successful': successful, 'failed': failed}
