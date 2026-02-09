"""
Фасад для обратной совместимости с ArchiveBackgroundRunner.
Предоставляет тот же интерфейс, но использует новую архитектуру сервисов.
"""

from typing import Dict, List, Optional, Any
from pathlib import Path

from loguru import logger

from core.tender_database import TenderDatabaseManager
from config.settings import Config
from services.archive_processing import ArchiveProcessingCoordinator


class ArchiveBackgroundRunnerFacade:
    """
    Фасад для обратной совместимости с ArchiveBackgroundRunner.
    
    Предоставляет тот же публичный интерфейс, но использует под капотом
    новую архитектуру с разделенными сервисами.
    """

    def __init__(
        self,
        tender_db_manager: TenderDatabaseManager,
        product_db_manager: TenderDatabaseManager,
        user_id: int = 1,
        max_workers: int = 2,
        batch_size: int = 5,
        batch_delay: float = 10.0,
    ):
        """
        Инициализация фасада с совместимым интерфейсом.
        
        Args:
            tender_db_manager: Менеджер БД для данных тендеров
            product_db_manager: Менеджер БД для продуктов
            user_id: ID пользователя (сохраняется для совместимости)
            max_workers: Максимальное количество рабочих процессов
            batch_size: Размер батча (сохраняется для совместимости)
            batch_delay: Задержка между батчами обработки
        """
        self.tender_db_manager = tender_db_manager
        self.product_db_manager = product_db_manager
        self.user_id = user_id
        self.max_workers = max(1, max_workers)
        self.batch_size = max(1, batch_size)
        self.batch_delay = max(0.0, batch_delay)
        
        # Получаем конфигурацию
        self.config = Config()
        
        # Определяем директорию для скачивания
        download_dir = Path(self.config.document_download_dir) \
            if self.config.document_download_dir \
            else Path.home() / "Downloads" / "ЕИС_Документация"
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Инициализируем новый координатор
        self._coordinator = self._create_coordinator()
        
        logger.info(f"Фасад ArchiveBackgroundRunner инициализирован. "
                   f"Workers: {self.max_workers}, Batch: {self.batch_size}")
    
    def _create_coordinator(self) -> ArchiveProcessingCoordinator:
        """Создание координатора обработки архивов."""
        return ArchiveProcessingCoordinator(
            tender_db_manager=self.tender_db_manager,
            product_db_manager=self.product_db_manager,
            download_dir=self.download_dir,
            config=self.config,
            max_workers=self.max_workers,
            batch_delay=self.batch_delay
        )
    
    def run(self, specific_tender_ids: Optional[List[Dict[str, Any]]] = None, 
            registry_type: Optional[str] = None, tender_type: str = 'new') -> Dict[str, Any]:
        """
        Запуск полного цикла обработки архивов.
        
        Args:
            specific_tender_ids: Список конкретных тендеров для обработки
            registry_type: Тип реестра для фильтрации ('44fz' или '223fz'). Если None, обрабатываются оба.
            tender_type: Тип торгов ('new' для новых, 'won' для разыгранных). По умолчанию 'new'.
            
        Returns:
            Результаты обработки в совместимом формате
        """
        logger.info(f"Запуск обработки через фасад. Тип: {tender_type}, Реестр: {registry_type}")
        
        # Конвертируем specific_tender_ids в формат, понятный новому координатору
        processed_tender_ids = None
        if specific_tender_ids:
            processed_tender_ids = []
            for tender_info in specific_tender_ids:
                if isinstance(tender_info, dict):
                    processed_tender_ids.append(tender_info)
                elif isinstance(tender_info, int):
                    # Поддержка старого формата с простыми ID
                    processed_tender_ids.append({'id': tender_info, 'registry_type': registry_type or '44fz'})
        
        # Вызываем новый координатор
        results = self._coordinator.run(
            specific_tender_ids=processed_tender_ids,
            registry_type=registry_type,
            tender_type=tender_type
        )
        
        # Конвертируем результаты в совместимый формат
        return self._convert_results_to_legacy_format(results)
    
    def _process_existing_folders(self, registry_type: Optional[str] = None, 
                                tender_type: str = 'new') -> int:
        """
        Обработка существующих папок с документами.
        
        Args:
            registry_type: Тип реестра для фильтрации ('44fz' или '223fz')
            tender_type: Тип торгов ('new' для новых, 'won' для разыгранных)
            
        Returns:
            Количество обработанных папок
        """
        logger.info(f"Обработка существующих папок через фасад. Реестр: {registry_type}")
        
        # Для существующих папок tender_type не используется в новой архитектуре
        # но сохраняем параметр для совместимости
        return self._coordinator.process_existing_folders(registry_type)
    
    def _convert_results_to_legacy_format(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Конвертация результатов новой архитектуры в старый формат.
        
        Args:
            results: Результаты из нового координатора
            
        Returns:
            Результаты в старом формате
        """
        # Базовый формат результатов
        legacy_results = {
            'processed': results.get('processed', 0),
            'successful': results.get('successful', 0),
            'failed': results.get('failed', 0),
            'errors': results.get('errors', []),
            'details': []
        }
        
        # Добавляем дополнительную информацию для совместимости
        if 'processed' in results:
            legacy_results['total_processed'] = results['processed']
        
        return legacy_results
    
    def get_processing_stats(self) -> Dict[str, float]:
        """
        Получение статистики обработки.
        
        Returns:
            Статистика обработки
        """
        return self._coordinator.get_processing_stats()
    
    def cleanup(self):
        """Очистка ресурсов и временных файлов."""
        self._coordinator.cleanup()
    
    # Методы для совместимости со старым кодом
    
    @property
    def tender_repo(self):
        """Свойство для совместимости с старым кодом."""
        # Возвращаем заглушку или реальный репозиторий при необходимости
        return self._coordinator.tender_repo
    
    @property  
    def tender_match_repo(self):
        """Свойство для совместимости с старым кодом."""
        return self._coordinator.tender_match_repo
    
    @property
    def processed_tenders_repo(self):
        """Свойство для совместимости с старым кодом."""
        return self._coordinator.processed_tenders_repo
    
    def __getattr__(self, name):
        """
        Перехват обращений к атрибутам для полной совместимости.
        
        Позволяет обращаться к методам и свойствам оригинального runner.py
        даже если они не реализованы в фасаде.
        """
        # Логируем попытку доступа к нереализованному методу
        logger.warning(f"Попытка доступа к нереализованному атрибуту: {name}")
        
        # Возвращаем заглушку для методов
        if name.startswith('_') and not name.startswith('__'):
            def method_stub(*args, **kwargs):
                logger.warning(f"Вызов нереализованного метода {name} с args: {args}, kwargs: {kwargs}")
                return None
            return method_stub
        
        # Для свойств возвращаем None
        return None


# Алиас для обратной совместимости
ArchiveBackgroundRunner = ArchiveBackgroundRunnerFacade