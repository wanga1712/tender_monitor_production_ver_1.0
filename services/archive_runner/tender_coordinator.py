"""
MODULE: services.archive_runner.tender_coordinator
RESPONSIBILITY:
- Координация процесса обработки торгов
- Управление потоком выполнения основных операций
- Оркестрация работы специализированных компонентов
ALLOWED:
- Вызов методов других компонентов (FolderProcessor, CloudUploader, ErrorHandler)
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
from typing import Any, Dict, List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger

from services.archive_runner.folder_processor import FolderProcessor
from services.archive_runner.cloud_uploader import CloudUploader
from services.archive_runner.error_handler import ErrorHandler
from services.archive_runner.tender_queue_manager import TenderQueueManager
from services.archive_runner.tender_prefetcher import TenderPrefetcher
from services.archive_runner.tender_processor import TenderProcessor
from services.archive_runner.tender_provider import TenderProvider


class TenderCoordinator:
    """Координатор обработки торгов"""

    def __init__(self, 
                 folder_processor: FolderProcessor,
                 cloud_uploader: CloudUploader,
                 error_handler: ErrorHandler,
                 queue_manager: TenderQueueManager,
                 max_workers: int = 2):
        
        self.folder_processor = folder_processor
        self.cloud_uploader = cloud_uploader
        self.error_handler = error_handler
        self.queue_manager = queue_manager
        self.max_workers = max_workers

    def _process_single_tender(
        self,
        tender: Dict[str, Any],
        tender_type: str,
        tender_processor: TenderProcessor,
        tender_provider: TenderProvider,
    ) -> bool:
        tender_id = tender.get('id')
        registry_type = tender.get('registry_type', '44fz')
        try:
            documents = tender_provider.get_tender_documents(tender_id, registry_type)
            result = tender_processor.process_tender(
                tender=tender,
                documents=documents,
                existing_records=None,
                prefetched_data=None,
                processed_tenders_cache=None,
                tender_type=tender_type,
                get_tender_documents_func=tender_provider.get_tender_documents,
            )
            if result is None:
                return False
            return result.get('success', False)
        except Exception as e:
            logger.error(f"Ошибка обработки tender_id={tender_id}: {e}")
            self.error_handler.handle_failed_tender(tender, e, self.queue_manager)
            return False

    def process_existing_folders_parallel(self, registry_type: Optional[str] = None) -> int:
        """Параллельная обработка существующих папок"""
        try:
            # Используем FolderProcessor для обработки папок
            processed_count = self.folder_processor.process_existing_folders(registry_type)
            logger.info(f"Параллельно обработано {processed_count} папок")
            return processed_count
            
        except Exception as e:
            logger.error(f"Ошибка параллельной обработки папок: {e}")
            return 0

    def get_processing_stats(self) -> Dict[str, float]:
        """Получить статистику обработки"""
        return {
            'average_time_per_file': self._get_average_processing_time_per_file(),
            'average_time_per_tender': self._get_average_processing_time_per_tender(),
            'active_workers': self.max_workers
        }

    def _get_average_processing_time_per_file(self) -> float:
        """Среднее время обработки файла"""
        # Заглушка для реальной реализации
        return 2.5

    def _get_average_processing_time_per_tender(self) -> float:
        """Среднее время обработки торгов"""
        # Заглушка для реальной реализации
        return 30.0

    def process(self, specific_tender_ids: Optional[List[Dict[str, Any]]] = None,
               registry_type: Optional[str] = None, tender_type: str = 'new',
               tender_processor: Optional[TenderProcessor] = None,
               tender_provider: Optional[TenderProvider] = None,
               batch_size: int = 10,
               stop_checker: Optional[Callable[[], bool]] = None,
    ) -> Dict[str, Any]:
        """
        Основной метод обработки, координирующий весь процесс.
        
        Args:
            specific_tender_ids: Конкретные тендеры для обработки
            registry_type: Тип реестра
            tender_type: Тип торгов
            tender_processor: Процессор тендеров
            tender_provider: Провайдер тендеров
            
        Returns:
            Результаты обработки
        """
        results = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'existing_folders_processed': 0,
            'new_tenders_processed': 0,
            'errors': []
        }

        try:
            # Обработка существующих папок
            if tender_type == 'existing':
                results['existing_folders_processed'] = self.process_existing_folders(
                    registry_type=registry_type,
                    tender_type=tender_type,
                    tender_processor=tender_processor
                )
            else:
                # Обработка новых тендеров
                new_tenders_result = self.process_new_tenders(
                    registry_type=registry_type,
                    tender_type=tender_type,
                    tender_processor=tender_processor,
                    tender_provider=tender_provider,
                    batch_size=batch_size,
                    stop_checker=stop_checker,
                )
                results.update(new_tenders_result)
                results['new_tenders_processed'] = results.get('processed', 0)

            results['total_processed'] = results['existing_folders_processed'] + results['new_tenders_processed']
            return results

        except Exception as e:
            logger.error(f"Критическая ошибка в координаторе: {e}")
            results['errors'].append(str(e))
            return results

    def process_existing_folders(self, registry_type: Optional[str] = None,
                               tender_type: str = 'new',
                               tender_processor: Optional[TenderProcessor] = None,
                               stop_checker: Optional[Callable[[], bool]] = None) -> int:
        """Обработка существующих папок с документами."""
        try:
            if stop_checker and stop_checker():
                return 0
            return self.process_existing_folders_parallel(registry_type)
        except Exception as e:
            logger.error(f"Ошибка обработки существующих папок: {e}")
            return 0

    def process_new_tenders(
        self,
        registry_type: Optional[str] = None,
        tender_type: str = 'new',
        tender_processor: Optional[TenderProcessor] = None,
        tender_provider: Optional[TenderProvider] = None,
        batch_size: int = 10,
        stop_checker: Optional[Callable[[], bool]] = None,
    ) -> Dict[str, Any]:
        results = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'errors': [],
        }
        if tender_processor is None or tender_provider is None:
            logger.error("tender_processor или tender_provider не инициализированы")
            return results
        try:
            if stop_checker and stop_checker():
                return results
            batch_size = max(10, batch_size)
            tenders = tender_provider.get_target_tenders(
                region_id=None,
                limit=batch_size,
                specific_tender_ids=None,
                registry_type=registry_type,
                tender_type=tender_type,
            )
            if not tenders:
                logger.info("Нет торгов для обработки")
                return results
            
            logger.info(f"Начинаем обработку {len(tenders)} торгов...")
            for batch_start in range(0, len(tenders), batch_size):
                if stop_checker and stop_checker():
                    return results
                batch = tenders[batch_start:batch_start + batch_size]
                logger.info(f"🔒 Блокировка батча: {len(batch)} торгов")
                locked_batch: List[Dict[str, Any]] = []
                for tender in batch:
                    if stop_checker and stop_checker():
                        return results
                    tender_id = tender.get("id")
                    registry = tender.get("registry_type", "44fz")
                    
                    # Проверяем, не была ли закупка уже обработана или заблокирована другим процессом
                    # Если мы получили её из базы, то скорее всего нет, но лучше проверить дважды
                    # Исключаем PROCESSING только если он не принадлежит текущему воркеру
                    # НО: tender_provider уже отфильтровал tdm.id IS NULL, так что сюда попадают только те,
                    # которых нет в tdm.
                    
                    if tender_processor and tender_processor.result_saver:
                        # Сначала проверяем, не нужно ли снять блокировку, если папка была удалена
                        # (Это решает проблему "фантомных" блокировок при удалении папок вручную)
                        # НО: мы не знаем путь к папке здесь. Это делает tender_processor.
                        
                        locked = tender_processor.result_saver.mark_as_processing(
                            tender_id,
                            registry,
                            worker_id=tender_processor.worker_id,
                        )
                        if locked:
                            locked_batch.append(tender)
                        else:
                            # Если не удалось заблокировать, но это НАША блокировка (например, от прошлого упавшего запуска)
                            # Мы должны попытаться её перехватить.
                            # mark_as_processing уже пытается обновить timestamp, если worker_id совпадает.
                            # Если вернул False - значит занято ДРУГИМ воркером или завершено.
                            
                            logger.warning(f"⚠️ Не удалось заблокировать тендер {tender_id} ({registry}) - возможно, уже обрабатывается")
                            results['failed'] += 1
                
                if not locked_batch:
                    continue

                for tender in locked_batch:
                    if stop_checker and stop_checker():
                        return results
                    
                    try:
                        success = self._process_single_tender(
                            tender,
                            tender_type,
                            tender_processor,
                            tender_provider,
                        )
                        if success:
                            results['successful'] += 1
                        else:
                            results['failed'] += 1
                            # Если обработка не удалась, нужно понять почему.
                            # Если это ошибка блокировки папки (WinError 5), то блокировка БД уже снята в tender_processor.
                            # Если другая ошибка - блокировка может остаться висеть.
                            # В tender_processor.process_tender мы уже добавили снятие блокировки при ошибке подготовки.
                            pass
                            
                        results['processed'] += 1
                    except Exception as e:
                        logger.error(f"Ошибка обработки tender {tender.get('id')}: {e}")
                        results['failed'] += 1
                        results['errors'].append(str(e))
                        
                        # Аварийное снятие блокировки при крахе
                        try:
                            if tender_processor and tender_processor.result_saver:
                                tender_processor.result_saver.unlock_tender(
                                    tender.get("id"), 
                                    tender.get("registry_type", "44fz")
                                )
                        except Exception:
                            pass
            return results
        except Exception as e:
            logger.error(f"Критическая ошибка в process_new_tenders: {e}")
            results['errors'].append(str(e))
            return results
