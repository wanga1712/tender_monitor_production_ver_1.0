"""
Обновленный координатор обработки торгов с использованием новых сервисов.

MODULE: services.archive_processing.tender_coordinator
RESPONSIBILITY:
- Координация процесса обработки торгов с использованием новых сервисов
- Управление потоком выполнения основных операций
- Оркестрация работы специализированных сервисов обработки архивов
ALLOWED:
- Вызов методов сервисов из archive_processing
- Управление многопоточностью и очередями
- Логирование через loguru
FORBIDDEN:
- Прямые файловые операции
- Прямые запросы к базе данных
- Прямая работа с облачными сервисами
ERRORS:
- Должен пробрасывать CoordinationError, ProcessingError
"""

import time
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from loguru import logger

from services.archive_processing.download_service import TenderDownloadService
from services.archive_processing.processing_service import DocumentProcessingService
from services.archive_processing.match_service import MatchDetectionService
from services.archive_processing.cleanup_service import FileCleanupService
from services.archive_processing.cloud_service import CloudUploadService
from services.archive_processing.error_service import ErrorHandlingService
from services.archive_processing.queue_service import TenderQueueService


class TenderProcessingCoordinator:
    """Координатор обработки торгов с использованием новых сервисов."""

    def __init__(self, 
                 download_service: TenderDownloadService,
                 processing_service: DocumentProcessingService,
                 match_service: MatchDetectionService,
                 cleanup_service: FileCleanupService,
                 cloud_service: CloudUploadService,
                 error_service: ErrorHandlingService,
                 queue_service: TenderQueueService,
                 max_workers: int = 2):
        
        self.download_service = download_service
        self.processing_service = processing_service
        self.match_service = match_service
        self.cleanup_service = cleanup_service
        self.cloud_service = cloud_service
        self.error_service = error_service
        self.queue_service = queue_service
        self.max_workers = max_workers

    def process_new_tenders(self, prefetcher: Any, tender_type: str = 'new') -> Dict[str, Any]:
        """
        Обработка новых торгов из префетчера.
        
        Args:
            prefetcher: Префетчер для получения торгов
            tender_type: Тип торгов ('new' или 'won')
            
        Returns:
            Результаты обработки
        """
        results = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
        try:
            # Получаем торги из префетчера
            tenders = prefetcher.get_tenders()
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_tender = {
                    executor.submit(self._process_single_tender, tender, tender_type): tender
                    for tender in tenders
                }
                
                for future in as_completed(future_to_tender):
                    tender = future_to_tender[future]
                    try:
                        success = future.result()
                        if success:
                            results['successful'] += 1
                        else:
                            results['failed'] += 1
                        results['processed'] += 1
                    except Exception as e:
                        results['failed'] += 1
                        results['errors'].append({
                            'tender_id': tender.get('id', 'unknown'),
                            'error': str(e)
                        })
                        logger.error(f"Ошибка обработки тендера {tender.get('id')}: {e}")
            
        except Exception as e:
            logger.error(f"Критическая ошибка в process_new_tenders: {e}")
            results['errors'].append({'error': f"Critical error: {e}"})
        
        return results

    def _process_single_tender(self, tender: Dict[str, Any], tender_type: str) -> bool:
        """
        Обработка одного тендера.
        
        Args:
            tender: Данные тендера
            tender_type: Тип торгов
            
        Returns:
            True если обработка успешна
        """
        tender_id = tender.get('id')
        registry_type = tender.get('registry_type')
        
        if not tender_id or not registry_type:
            logger.warning(f"Пропуск тендера без ID или типа реестра: {tender}")
            return False
        
        try:
            # 1. Скачивание документов
            documents = self.error_service.safe_call(
                self.download_service.download_tender_documents,
                tender_id, registry_type
            )
            
            if not documents:
                logger.warning(f"Нет документов для тендера {tender_id}")
                return False
            
            # 2. Обработка документов
            processed_docs = []
            for doc in documents:
                doc_path = Path(doc.get('local_path', ''))
                if doc_path.exists():
                    processed_doc = self.error_service.safe_call(
                        self.processing_service.parse_document,
                        doc_path
                    )
                    if processed_doc:
                        processed_docs.append(processed_doc)
            
            # 3. Поиск совпадений
            if processed_docs:
                match_results = self.error_service.safe_call(
                    self.match_service.find_matches_in_documents,
                    processed_docs
                )
                
                # TODO: Сохранение результатов в БД
                
            # 4. Загрузка в облако (если подключено)
            if self.cloud_service.check_cloud_connection():
                tender_folder = self.download_service.download_dir / f"{registry_type}_{tender_id}"
                if tender_folder.exists():
                    self.error_service.safe_call(
                        self.cloud_service.upload_to_cloud,
                        tender_folder, tender_id, registry_type
                    )
            
            # 5. Очистка временных файлов
            self.error_service.safe_call(
                self.cleanup_service.remove_processed_files,
                tender_id, registry_type
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка обработки тендера {tender_id}: {e}")
            return False

    def process_existing_folders(self, registry_type: Optional[str] = None) -> int:
        """
        Обработка существующих папок с документами.
        
        Args:
            registry_type: Тип реестра для фильтрации
            
        Returns:
            Количество обработанных папок
        """
        try:
            # Получаем список ожидающих обработки тендеров
            pending_tenders = self.cleanup_service.list_pending_tenders()
            
            if not pending_tenders:
                logger.info("Нет папок с существующими файлами для обработки")
                return 0
            
            # Фильтруем по registry_type если указан
            if registry_type:
                pending_tenders = [
                    t for t in pending_tenders 
                    if t.get('registry_type') == registry_type
                ]
            
            processed_count = 0
            for tender_info in pending_tenders:
                success = self._process_single_tender(tender_info, tender_info.get('tender_type', 'new'))
                if success:
                    processed_count += 1
            
            logger.info(f"Обработано существующих папок: {processed_count}/{len(pending_tenders)}")
            return processed_count
            
        except Exception as e:
            logger.error(f"Ошибка обработки существующих папок: {e}")
            return 0

    def get_processing_stats(self) -> Dict[str, float]:
        """
        Получение статистики обработки.
        
        Returns:
            Статистика обработки
        """
        # TODO: Реализовать сбор статистики из сервисов
        return {
            'average_file_time': 0.0,
            'average_tender_time': 0.0,
            'total_processed': 0,
            'success_rate': 0.0
        }

    def process(self, specific_tender_ids: Optional[List[Dict[str, Any]]] = None,
               registry_type: Optional[str] = None, tender_type: str = 'new') -> Dict[str, Any]:
        """
        Основной метод обработки торгов.
        
        Args:
            specific_tender_ids: Конкретные тендеры для обработки
            registry_type: Тип реестра
            tender_type: Тип торгов
            
        Returns:
            Результаты обработки
        """
        results = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
        try:
            # TODO: Реализовать обработку конкретных тендеров
            # Пока используем существующие папки
            if specific_tender_ids:
                logger.warning("Обработка конкретных тендеров еще не реализована")
            
            # Обрабатываем существующие папки
            processed = self.process_existing_folders(registry_type)
            results['processed'] = processed
            results['successful'] = processed
            
        except Exception as e:
            logger.error(f"Ошибка в основном процессе обработки: {e}")
            results['errors'].append({'error': str(e)})
        
        return results