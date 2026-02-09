"""
MODULE: services.archive_runner.runner
RESPONSIBILITY: Main orchestrator for background tender document processing.
ALLOWED: TenderDatabaseManager, DocumentSearchService, TenderProcessor, TenderFolderManager, logging.
FORBIDDEN: Direct SQL queries (use repositories/managers).
ERRORS: None.

Основной модуль запуска автоматической обработки документов торгов.
"""

from __future__ import annotations

import os
import time
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger

from config.settings import config
from core.database import DatabaseManager
from core.tender_database import TenderDatabaseManager
from core.exceptions import DocumentSearchError, DatabaseConnectionError
from services.document_search_service import DocumentSearchService
from services.document_search.document_selector import DocumentSelector
from services.document_search.document_downloader import DocumentDownloader
from services.document_search.download_timeout_calculator import create_timeout_calculator
from services.document_search.archive_extractor import ArchiveExtractor
from services.document_search.match_finder import MatchFinder
from services.tender_services.tender_repository_facade import TenderRepositoryFacade
from services.match_services.tender_match_repository_facade import TenderMatchRepositoryFacade
from services.archive_runner.file_cleaner import FileCleaner
from services.archive_runner.existing_files_processor import ExistingFilesProcessor
from services.archive_runner.tender_provider import TenderProvider
from services.archive_runner.tender_folder_manager import TenderFolderManager
from services.archive_runner.tender_processor import TenderProcessor
from services.archive_runner.tender_prefetcher import TenderPrefetcher, PrefetchedTenderData
from services.archive_runner.tender_queue_manager import TenderQueueManager
from services.archive_runner.processed_tenders_repository import ProcessedTendersRepository
# from services.storage.yandex_disk import YandexDiskClient  # Временное отключение

# Новые компоненты для декомпозиции
from services.archive_runner.folder_processor import FolderProcessor
from services.archive_runner.cloud_uploader import CloudUploader
from services.archive_runner.error_handler import ErrorHandler
from services.archive_runner.tender_coordinator import TenderCoordinator


class ArchiveBackgroundRunner:
    """
    Координатор фоновой обработки документов:
    1. Обрабатывает уже скачанные файлы
    2. Скачивает новые документы
    3. Находит совпадения и сохраняет результаты
    """


    def __init__(
        self,
        tender_db_manager: TenderDatabaseManager,
        product_db_manager: DatabaseManager,
        user_id: int = 1,
        max_workers: int = 2,
        batch_size: int = 5,
        batch_delay: float = 10.0,
    ):
        self.tender_db_manager = tender_db_manager
        self.product_db_manager = product_db_manager
        self.user_id = user_id
        self.max_workers = max(1, max_workers)  # Минимум 1 поток
        self.batch_size = max(1, batch_size)  # Минимум 1 торг в батче
        self.batch_delay = max(0.0, batch_delay)  # Минимум 0 секунд задержки

        self.tender_repo = TenderRepositoryFacade(tender_db_manager)
        self.tender_match_repo = TenderMatchRepositoryFacade(tender_db_manager)
        self.processed_tenders_repo = ProcessedTendersRepository(tender_db_manager)
        self.tender_provider = TenderProvider(self.tender_repo, user_id)

        download_dir = Path(config.document_download_dir) if config.document_download_dir else Path.home() / "Downloads" / "ЕИС_Документация"
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Инициализация Яндекс Диска (если включен) - временно отключено
        self.yandex_disk = None
        # if config.yandex_disk.enabled and config.yandex_disk.token:
        #     try:
        #         self.yandex_disk = YandexDiskClient(
        #             token=config.yandex_disk.token,
        #             base_path=config.yandex_disk.base_path
        #         )
        #         if self.yandex_disk.check_connection():
        #             logger.info("✅ Подключение к Яндекс Диску установлено")
        #         else:
        #             logger.warning("⚠️  Не удалось подключиться к Яндекс Диску, продолжаем без него")
        #             self.yandex_disk = None
        #     except Exception as e:
        #         logger.warning(f"⚠️  Ошибка инициализации Яндекс Диска: {e}, продолжаем без него")
        #         self.yandex_disk = None

        self.document_search_service = DocumentSearchService(
            product_db_manager,
            download_dir,
            unrar_path=config.unrar_tool,
            winrar_path=config.winrar_path,
        )
        self.document_search_service.ensure_products_loaded()

        self.selector = DocumentSelector()
        # Создаем калькулятор таймаута на основе статистики БД
        timeout_calculator = create_timeout_calculator(tender_db_manager)
        self.downloader = DocumentDownloader(
            download_dir,
            progress_callback=None,
            timeout_calculator=timeout_calculator
        )
        self.extractor = ArchiveExtractor(
            unrar_path=config.unrar_tool,
            winrar_path=config.winrar_path,
        )
        # Стоп-фразы для анализа документации (можно хранить на стороне tender_monitor)
        try:
            document_stop_phrases_rows = getattr(self.tender_repo, "get_document_stop_phrases", lambda _uid: [])(user_id)
            document_stop_phrases = [
                row.get("phrase", "").strip()
                for row in document_stop_phrases_rows
                if row.get("phrase")
            ]
        except Exception:
            document_stop_phrases = []

        # Пользовательские фразы для поиска по документации
        # Фразы загружаются из additional_phrases.py (инъектирование, усиление и т.д.)
        # Они объединяются с дополнительными фразами внутри MatchFinder
        user_search_phrases = []  # Пока не используем БД, фразы берутся из additional_phrases.py

        self.match_finder = MatchFinder(
            self.document_search_service._product_names,
            stop_phrases=document_stop_phrases,
            user_search_phrases=user_search_phrases,
        )
        self.file_cleaner = FileCleaner()
        self.existing_processor = ExistingFilesProcessor(download_dir)
        
        # Инициализируем менеджер папок и процессор тендеров
        self.folder_manager = TenderFolderManager(download_dir)
        
        # Инициализация новых компонентов для декомпозиции
        self.folder_processor = FolderProcessor(self.folder_manager)
        self.cloud_uploader = CloudUploader(self.yandex_disk)
        self.error_handler = ErrorHandler(max_retries=3, retry_delay=2.0)
        
        # Инициализация координатора
        self.tender_coordinator = TenderCoordinator(
            folder_processor=self.folder_processor,
            cloud_uploader=self.cloud_uploader,
            error_handler=self.error_handler,
            queue_manager=TenderQueueManager(),  # TODO: Заменить на реальный менеджер очередей
            max_workers=self.max_workers
        )
        
        self.tender_processor = TenderProcessor(
            tender_match_repo=self.tender_match_repo,
            folder_manager=self.folder_manager,
            document_search_service=self.document_search_service,
            selector=self.selector,
            downloader=self.downloader,
            extractor=self.extractor,
            match_finder=self.match_finder,
            file_cleaner=self.file_cleaner,
            processed_tenders_repo=self.processed_tenders_repo,
            max_workers=self.max_workers,
            safe_call_func=self.error_handler.safe_call,  # Используем ErrorHandler напрямую
            get_avg_time_func=self._get_average_processing_time_per_file,
            batch_delay=min(self.batch_delay, 5.0),  # Используем меньшую задержку для файлов
        )

        self._processed_tenders: Set[Tuple[int, str]] = set()
        self._reconnect_delay = 60

    def run(self, specific_tender_ids: Optional[List[Dict[str, Any]]] = None, registry_type: Optional[str] = None, tender_type: str = 'all') -> Dict[str, Any]:
        """
        Запуск полного цикла обработки для всех типов торгов или конкретного типа.
        Если tender_type='all', последовательно обрабатывает 'new', 'won', 'commission'.
        """
        if specific_tender_ids:
            # Если указаны конкретные ID, запускаем один проход
            pass_type = tender_type if tender_type != 'all' else 'new'
            return self._run_single_pass(specific_tender_ids, registry_type, pass_type)

        if tender_type == 'all':
            types_to_process = ['new', 'won']
        else:
            types_to_process = [tender_type]

        aggregated_results = {
            "existing_folders": 0,
            "total_tenders": 0,
            "processed": 0,
            "errors": 0,
            "total_matches": 0,
            "total_time": 0.0,
        }
        
        start_time = time.time()
        
        for t_type in types_to_process:
            try:
                logger.info(f"🔄 --- Начало цикла обработки для типа: {t_type} ---")
                results = self._run_single_pass(specific_tender_ids, registry_type, t_type)
                
                aggregated_results["existing_folders"] += results.get("existing_folders", 0)
                aggregated_results["total_tenders"] += results.get("total_tenders", 0)
                aggregated_results["processed"] += results.get("processed", 0)
                aggregated_results["errors"] += results.get("errors", 0)
                aggregated_results["total_matches"] += results.get("total_matches", 0)
            except Exception as e:
                logger.error(f"Ошибка в цикле обработки для типа {t_type}: {e}")
                aggregated_results["errors"] += 1
                
        aggregated_results["total_time"] = time.time() - start_time
        
        logger.info(f"\n{'='*80}")
        logger.info("🏁 ОБЩИЙ ИТОГ (ВСЕ ТИПЫ)")
        logger.info(f"{'='*80}")
        logger.info(f"✅ Всего обработано: {aggregated_results['processed']}")
        logger.info(f"❌ Всего ошибок: {aggregated_results['errors']}")
        logger.info(f"⏱️  Общее время: {aggregated_results['total_time']:.2f} сек")
        
        return aggregated_results

    def _run_single_pass(self, specific_tender_ids: Optional[List[Dict[str, Any]]] = None, registry_type: Optional[str] = None, tender_type: str = 'new') -> Dict[str, Any]:
        """
        Запуск полного цикла обработки:
        - сначала существующие файлы
        - затем новые торги из БД или конкретные закупки

        Args:
            specific_tender_ids: Список словарей с ключами 'id' и 'registry_type' для конкретных закупок
            registry_type: Тип реестра для фильтрации ('44fz' или '223fz'). Если None, обрабатываются оба.
            tender_type: Тип торгов ('new' для новых, 'won' для разыгранных). По умолчанию 'new'.
        """
        # #region agent log - ДО logger.info!
        import json
        import time
        import os
        from pathlib import Path
        # Используем относительный путь
        project_root = Path(__file__).parent.parent.parent
        log_path = project_root / ".cursor" / "debug.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(str(log_path), "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "main",
                    "hypothesisId": "MAIN",
                    "location": "runner.py:run:entry",
                    "message": "Запуск ArchiveBackgroundRunner.run",
                    "data": {
                        "specific_tender_ids_count": len(specific_tender_ids) if specific_tender_ids else 0,
                        "registry_type": registry_type,
                        "tender_type": tender_type
                    },
                    "timestamp": int(time.time() * 1000)
}, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        except Exception as e:
            # Запишем в stderr если лог не пишется
            print(f"Failed to write log: {e}", file=__import__('sys').stderr)
        # #endregion

        logger.info("🚀 Запуск автоматической обработки документов торгов")
        logger.info("=" * 80)
        logger.info(f"Параметры запуска: specific_tenders={specific_tender_ids is not None}, registry_type={registry_type}, tender_type={tender_type}")
        try:
            with open(str(log_path), "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "subprocess",
                    "hypothesisId": "PROCESS",
                    "location": "runner.py:run:entry",
                    "message": "ArchiveBackgroundRunner.run запущен",
                    "data": {
                        "specific_tender_ids_count": len(specific_tender_ids) if specific_tender_ids else 0,
                        "registry_type": registry_type,
                        "tender_type": tender_type
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            pass
        # #endregion

        overall_start = time.time()

        # #region agent log
        try:
            with open(str(log_path), "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "main",
                    "hypothesisId": "MAIN",
                    "location": "runner.py:run:after_init",
                    "message": "Инициализация завершена, начинаем обработку",
                    "data": {
                        "overall_start": overall_start
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            pass
        # #endregion

        # #region agent log
        try:
            with open(str(log_path), "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "main",
                    "hypothesisId": "MAIN",
                    "location": "runner.py:run:before_process_existing",
                    "message": "Перед вызовом _process_existing_folders",
                    "data": {
                        "registry_type": registry_type,
                        "tender_type": tender_type
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            pass
        # #endregion

        # #region agent log - CHECK FOR REPROCESSING
        try:
            with open(str(log_path), "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "reprocessing_check",
                    "hypothesisId": "REPROCESSING",
                    "location": "runner.py:run:before_existing_folders",
                    "message": "Проверка на повторную обработку",
                    "data": {
                        "specific_tenders_count": len(specific_tender_ids) if specific_tender_ids else 0,
                        "registry_type": registry_type,
                        "tender_type": tender_type
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            pass
        # #endregion

        # Собираем существующие папки для параллельной обработки
        existing_folders: List[Dict[str, Any]] = []
        try:
            logger.info("Сбор существующих папок для обработки...")
            existing_entries = self.existing_processor.list_pending_tenders()
            logger.info(f"Найдено директорий с существующими файлами: {len(existing_entries)}")
            
            # Батч-проверка обработанных торгов для существующих файлов
            tenders_by_registry: Dict[str, List[int]] = {}
            for entry in existing_entries:
                if registry_type and entry.get("registry_type") != registry_type:
                    continue
                entry_tender_type = entry.get("tender_type", "new")
                if entry_tender_type != tender_type:
                    continue
                reg = entry.get("registry_type", "44fz")
                tender_id = entry.get("tender_id")
                if tender_id:
                    if reg not in tenders_by_registry:
                        tenders_by_registry[reg] = []
                    tenders_by_registry[reg].append(tender_id)
            
            processed_tenders_cache_existing: Dict[Tuple[int, str], Dict[str, Any]] = {}
            for reg, tender_ids in tenders_by_registry.items():
                batch_results = self._safe_tender_call(
                    self.tender_match_repo.get_match_results_batch,
                    tender_ids,
                    reg,
                )
                for tender_id, match_result in batch_results.items():
                    processed_tenders_cache_existing[(tender_id, reg)] = match_result
            
            filtered_entries = [e for e in existing_entries if (not registry_type or e.get("registry_type") == registry_type) and e.get("tender_type", "new") == tender_type]
            logger.info(f"Найдено {len(filtered_entries)} существующих папок для обработки (фильтр: registry_type={registry_type}, tender_type={tender_type})")
            
            # Собираем существующие папки с размерами и сортируем по размеру (самая маленькая первая)
            for entry in filtered_entries:
                key = (entry["tender_id"], entry["registry_type"])
                folder_path = entry.get("folder_path")
                folder_name = folder_path.name if folder_path else None
                
                # Пропускаем уже обработанные
                if key in processed_tenders_cache_existing:
                    match_result = processed_tenders_cache_existing[key]
                    if folder_name and match_result.get("folder_name") == folder_name:
                        continue
                    elif match_result:
                        continue
                
                # Дополнительная проверка по folder_name
                if folder_name:
                    folder_match_result = self._safe_tender_call(
                        self.tender_match_repo.get_match_result_by_folder_name,
                        folder_name,
                    )
                    if folder_match_result:
                        continue
                
                # Вычисляем размер папки
                folder_size = 0
                if folder_path and folder_path.exists():
                    folder_size = self.folder_manager.get_folder_size(folder_path)
                
                existing_folders.append({
                    "id": entry["tender_id"],
                    "registry_type": entry["registry_type"],
                    "folder_path": folder_path,
                    "tender_type": entry.get("tender_type", "new"),
                    "folder_size": folder_size,
                })
            
            # Сортируем по размеру (самая маленькая первая)
            existing_folders.sort(key=lambda x: x["folder_size"])
            logger.info(f"Подготовлено {len(existing_folders)} существующих папок для обработки (отсортировано по размеру)")
        except Exception as e:
            logger.error(f"Ошибка при сборе существующих файлов: {e}")
            logger.exception("Детали ошибки:")
            existing_folders = []

        logger.info(f"Подготовлено {len(existing_folders)} существующих папок для параллельной обработки")
        
        # #region agent log
        try:
            with open(str(log_path), "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "subprocess",
                    "hypothesisId": "PROCESS",
                    "location": "runner.py:run:before_get_tenders",
                    "message": "Перед получением списка торгов",
                    "data": {
                        "specific_tender_ids_count": len(specific_tender_ids) if specific_tender_ids else 0,
                        "registry_type": registry_type,
                        "tender_type": tender_type
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            pass
        # #endregion

        # Если указаны конкретные закупки, используем их, иначе получаем по настройкам
        logger.info("Получение списка торгов из БД...")
        if specific_tender_ids:
            logger.info(f"Запрошены конкретные закупки: {len(specific_tender_ids)} шт.")
            tenders = self._safe_tender_call(
                self.tender_provider.get_target_tenders,
                specific_tender_ids=specific_tender_ids,
                registry_type=registry_type,
                tender_type=tender_type
            )
        else:
            logger.info("Получение всех торгов по настройкам пользователя...")
            tenders = self._safe_tender_call(
                self.tender_provider.get_target_tenders,
                registry_type=registry_type,
                tender_type=tender_type
            )
        logger.info(f"Получено торгов из БД: {len(tenders) if tenders else 0}")

        # #region agent log
        try:
            with open(str(log_path), "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "subprocess",
                    "hypothesisId": "PROCESS",
                    "location": "runner.py:run:after_get_tenders",
                    "message": "После получения списка торгов",
                    "data": {
                        "tenders_count": len(tenders) if tenders else 0,
                        "tenders_is_none": tenders is None,
                        "specific_tender_ids_count": len(specific_tender_ids) if specific_tender_ids else 0,
                        "registry_type": registry_type,
                        "tender_type": tender_type
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            pass
        # #endregion
        
        if not tenders:
            logger.warning("Нет торгов для обработки")
            # #region agent log
            try:
                with open(str(log_path), "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "subprocess",
                        "hypothesisId": "PROCESS",
                        "location": "runner.py:run:no_tenders",
                        "message": "Нет торгов для обработки",
                        "data": {
                            "tenders_is_none": tenders is None,
                            "tenders_is_empty": tenders == [] if tenders is not None else None
                        },
                        "timestamp": int(time.time() * 1000)
                    }, ensure_ascii=False) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except Exception:
                pass
            # #endregion

        # #region agent log
        try:
            with open(str(log_path), "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "subprocess",
                    "hypothesisId": "PROCESS",
                    "location": "runner.py:run:before_batch_check",
                    "message": "Перед батч-проверкой обработанных торгов",
                    "data": {
                        "tenders_count": len(tenders) if tenders else 0
                        },
                        "timestamp": int(time.time() * 1000)
                    }, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception:
                pass
            # #endregion

        # Батч-проверка обработанных торгов для новых торгов
        processed_tenders_cache: Dict[Tuple[int, str], Dict[str, Any]] = {}
        if tenders:
            tenders_by_registry: Dict[str, List[int]] = {}
            for tender in tenders:
                registry = tender.get("registry_type", "44fz")
                tender_id = tender.get("id")
                if tender_id:
                    if registry not in tenders_by_registry:
                        tenders_by_registry[registry] = []
                    tenders_by_registry[registry].append(tender_id)
            
            for registry, tender_ids in tenders_by_registry.items():
                batch_results = self._safe_tender_call(
                    self.tender_match_repo.get_match_results_batch,
                    tender_ids,
                    registry,
                )
                for tender_id, match_result in batch_results.items():
                    processed_tenders_cache[(tender_id, registry)] = match_result
            
            logger.info(f"Батч-проверка новых торгов: из {len(tenders)} уже обработано: {len(processed_tenders_cache)}")

            # #region agent log
            try:
                with open(str(log_path), "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "subprocess",
                        "hypothesisId": "PROCESS",
                        "location": "runner.py:run:after_batch_check",
                        "message": "После батч-проверки обработанных торгов",
                        "data": {
                            "processed_cache_size": len(processed_tenders_cache),
                            "tenders_count": len(tenders) if tenders else 0
                        },
                        "timestamp": int(time.time() * 1000)
                    }, ensure_ascii=False) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except Exception:
                pass
            # #endregion

        # Если запрошены конкретные закупки — форсируем повторную обработку (не доверяем кэшу)
        if specific_tender_ids:
            logger.info(
                f"Форсируем обработку конкретных закупок: сбрасываем кэш обработанных ({len(processed_tenders_cache)})"
            )
            processed_tenders_cache = {}

            # #region agent log
            try:
                with open(str(log_path), "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "subprocess",
                        "hypothesisId": "PROCESS",
                        "location": "runner.py:run:force_reprocess_specific",
                        "message": "FORCE_REPROCESS_SPECIFIC",
                        "data": {
                            "specific_tender_ids_count": len(specific_tender_ids),
                            "processed_cache_size_before": len(processed_tenders_cache)
                        },
                        "timestamp": int(time.time() * 1000)
                    }, ensure_ascii=False) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except Exception:
                pass
            # #endregion

        # Получаем среднее время обработки одного торга из БД для расчета оставшегося времени
        avg_time_per_tender = self._get_average_processing_time_per_tender()

        # #region agent log
        try:
            with open(str(log_path), "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "subprocess",
                    "hypothesisId": "PROCESS",
                    "location": "runner.py:run:before_parallel_processing",
                    "message": "Перед запуском параллельной обработки",
                    "data": {
                        "avg_time_per_tender": avg_time_per_tender,
                        "tenders_count": len(tenders) if tenders else 0
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            pass
        # #endregion

        # Запускаем параллельную обработку существующих папок и скачивание новых торгов
        logger.info("🚀 Запуск параллельной обработки существующих папок и скачивания новых торгов")
        
        # Создаём prefetcher для новых торгов
        prefetcher: Optional[TenderPrefetcher] = None
        if tenders:
            # #region agent log
            try:
                with open(str(log_path), "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "subprocess",
                        "hypothesisId": "PROCESS",
                        "location": "runner.py:run:before_create_prefetcher",
                        "message": "Перед созданием prefetcher",
                        "data": {
                            "tender_type": tender_type
                        },
                        "timestamp": int(time.time() * 1000)
                    }, ensure_ascii=False) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except Exception:
                pass
            # #endregion

            prefetcher = self._create_prefetcher(tender_type)

            # #region agent log
            try:
                with open(str(log_path), "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "subprocess",
                        "hypothesisId": "PROCESS",
                        "location": "runner.py:run:after_create_prefetcher",
                        "message": "После создания prefetcher",
                        "data": {
                            "prefetcher_created": prefetcher is not None
                        },
                        "timestamp": int(time.time() * 1000)
                    }, ensure_ascii=False) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except Exception:
                pass
            # #endregion

            prefetcher.schedule(tenders, self._get_tender_documents_safe)

            # #region agent log
            try:
                with open(str(log_path), "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "subprocess",
                        "hypothesisId": "PROCESS",
                        "location": "runner.py:run:after_schedule_prefetcher",
                        "message": "После schedule prefetcher",
                        "data": {
                            "tenders_count": len(tenders) if tenders else 0
                        },
                        "timestamp": int(time.time() * 1000)
                    }, ensure_ascii=False) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except Exception:
                pass
            # #endregion
        
        # Счетчики статистики
        processed = 0
        errors = 0
        skipped_no_docs = 0
        total_matches = 0
        
        # Запускаем параллельную обработку существующих папок
        if existing_folders:
            logger.info(f"Начинаем параллельную обработку {len(existing_folders)} существующих папок")
            existing_stats = self._process_existing_folders_parallel(
                existing_folders,
                processed_tenders_cache,
                tender_type,
                prefetcher,
                tenders if tenders else []
            )
            processed += existing_stats.get("processed", 0)
            errors += existing_stats.get("errors", 0)
            total_matches += existing_stats.get("total_matches", 0)
        
        # Обрабатываем новые торги из очереди скачивания
        if tenders:
            logger.info(f"Начинаем обработку {len(tenders)} новых торгов")
            new_stats = self._process_new_tenders(
                tenders,
                processed_tenders_cache,
                tender_type,
                prefetcher,
            )
            processed += new_stats.get("processed", 0)
            errors += new_stats.get("errors", 0)
            skipped_no_docs += new_stats.get("skipped_no_docs", 0)
            total_matches += new_stats.get("total_matches", 0)

        if prefetcher:
            try:
                prefetcher.shutdown()
            except Exception as e:
                logger.warning(f"Ошибка при закрытии prefetcher: {e}", exc_info=True)

        overall_time = time.time() - overall_start

        logger.info(f"\n{'='*80}")
        logger.info("📊 ИТОГОВАЯ СТАТИСТИКА")
        logger.info(f"{'='*80}")
        logger.info(f"📁 Обработано существующих директорий: {len(existing_folders)}")
        logger.info(f"📦 Новых торгов: {len(tenders) if tenders else 0}")
        logger.info(f"✅ Успешно обработано: {processed}")
        logger.info(f"⏭️ Пропущено (нет документов): {skipped_no_docs}")
        logger.info(f"❌ Ошибок: {errors}")
        logger.info(f"🔍 Всего найдено совпадений: {total_matches}")
        logger.info(f"⏱️  Общее время: {overall_time:.2f} сек")

        return {
            "existing_folders": len(existing_folders),
            "total_tenders": len(tenders) if tenders else 0,
            "processed": processed,
            "errors": errors,
            "total_matches": total_matches,
            "total_time": overall_time,
        }

    def _process_existing_folders(self, registry_type: Optional[str] = None, tender_type: str = 'new') -> int:
        """Обработка существующих папок с документами"""
        # #region agent log
        import json
        import time
        from pathlib import Path
        log_path = Path(__file__).parent.parent.parent / ".cursor" / "debug.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(str(log_path), "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "main",
                    "hypothesisId": "MAIN",
                    "location": "runner.py:_process_existing_folders:entry",
                    "message": "Начинаем обработку существующих папок",
                    "data": {
                        "registry_type": registry_type,
                        "tender_type": tender_type
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception:
            pass
        # #endregion
        """
        Обрабатывает уже скачанные файлы в директориях.
        
        Args:
            registry_type: Тип реестра для фильтрации ('44fz' или '223fz')
            tender_type: Тип торгов ('new' для новых, 'won' для разыгранных)
        """
        entries = self.existing_processor.list_pending_tenders()
        logger.info(f"Найдено директорий с существующими файлами: {len(entries)}")
        
        if not entries:
            return 0
        
        # Батч-проверка обработанных торгов для существующих файлов
        tenders_by_registry: Dict[str, List[int]] = {}
        for entry in entries:
            # Фильтруем по registry_type
            if registry_type and entry.get("registry_type") != registry_type:
                continue
            # Фильтруем по tender_type - обрабатываем только соответствующий тип
            entry_tender_type = entry.get("tender_type", "new")
            if entry_tender_type != tender_type:
                continue
            reg = entry.get("registry_type", "44fz")
            tender_id = entry.get("tender_id")
            if tender_id:
                if reg not in tenders_by_registry:
                    tenders_by_registry[reg] = []
                tenders_by_registry[reg].append(tender_id)
        
        processed_tenders_cache: Dict[Tuple[int, str], Dict[str, Any]] = {}
        for reg, tender_ids in tenders_by_registry.items():
            batch_results = self._safe_tender_call(
                self.tender_match_repo.get_match_results_batch,
                tender_ids,
                reg,
            )
            for tender_id, match_result in batch_results.items():
                processed_tenders_cache[(tender_id, reg)] = match_result
        
        filtered_count = len([e for e in entries if (not registry_type or e.get("registry_type") == registry_type) and e.get("tender_type", "new") == tender_type])
        logger.info(
            f"Батч-проверка существующих файлов ({tender_type}): из {filtered_count} торгов уже обработано: {len(processed_tenders_cache)}"
        )
        
        processed = 0
        filtered_entries = [e for e in entries if (not registry_type or e.get("registry_type") == registry_type) and e.get("tender_type", "new") == tender_type]
        logger.info(f"Начинаем обработку {len(filtered_entries)} существующих папок (фильтр: registry_type={registry_type}, tender_type={tender_type})")
        
        # Ограничиваем обработку существующих файлов для ускорения
        # Обрабатываем только первые 100, остальные пропускаем (они уже обработаны или будут обработаны как новые)
        max_existing_to_process = 100
        entries_to_process = filtered_entries[:max_existing_to_process]
        logger.info(f"Обрабатываем первые {len(entries_to_process)} существующих папок (для ускорения)")
        
        for idx, entry in enumerate(entries_to_process):
            if idx % 50 == 0 and idx > 0:
                logger.info(f"Обработка существующих файлов: {idx}/{len(entries_to_process)}")
            tender = {
                "id": entry["tender_id"],
                "registry_type": entry["registry_type"],
                "folder_path": entry["folder_path"],
            }
            
            # Проверяем кэш перед обработкой
            key = (tender["id"], tender["registry_type"])
            folder_path = entry.get("folder_path")
            folder_name = folder_path.name if folder_path else None
            
            # Проверяем по tender_id в кэше
            if key in processed_tenders_cache:
                match_result = processed_tenders_cache[key]
                # Дополнительно проверяем по folder_name, если он указан
                if folder_name and match_result.get("folder_name") == folder_name:
                    logger.debug(f"Папка {folder_name} уже обработана (найдено в БД по folder_name)")
                    self._processed_tenders.add(key)
                    continue
                elif match_result:
                    # Если есть запись в БД, но folder_name не совпадает, все равно пропускаем
                    logger.debug(f"Торг {tender['id']} ({tender['registry_type']}) уже обработан, но folder_name не совпадает. Пропускаем.")
                    self._processed_tenders.add(key)
                    continue
            
            # Дополнительная проверка по folder_name в БД (если не нашли в кэше)
            if folder_name:
                folder_match_result = self._safe_tender_call(
                    self.tender_match_repo.get_match_result_by_folder_name,
                    folder_name,
                )
                if folder_match_result:
                    logger.debug(f"Папка {folder_name} уже обработана (найдено в БД по folder_name)")
                    processed_tenders_cache[key] = folder_match_result
                    self._processed_tenders.add(key)
                    continue
            
            documents = self._safe_tender_call(
                self.tender_provider.get_tender_documents,
                tender["id"],
                tender["registry_type"],
            )
            # Определяем tender_type из папки
            tender_type_from_folder = entry.get("tender_type", "new")
            try:
                folder_path = self.folder_manager.prepare_tender_folder(tender["id"], tender["registry_type"], tender_type_from_folder)
                existing_records = self.existing_processor.build_records(folder_path)
                if not existing_records:
                    continue
                tender["folder_path"] = folder_path
            except Exception as e:
                logger.error(f"Ошибка при подготовке папки для торга {tender['id']}: {e}")
                continue
            
            try:
                result = self._process_tender(
                    tender,
                    documents=documents,
                    existing_records=existing_records,
                    processed_tenders_cache=processed_tenders_cache,
                    tender_type=tender_type_from_folder,
                )
                if result:
                    processed += 1
                    self._processed_tenders.add((tender["id"], tender["registry_type"]))
            except KeyboardInterrupt:
                # Пользователь прервал выполнение
                raise
            except SystemExit:
                # Системный выход
                raise
            except Exception as e:
                logger.error(f"Ошибка при обработке существующего файла для торга {tender['id']}: {e}", exc_info=True)
                # Продолжаем обработку следующих
                continue
        
        logger.info(f"Обработка существующих файлов завершена. Обработано: {processed} из {len(entries_to_process)} (всего найдено: {len(filtered_entries)})")
        return processed

    def _process_tender(
        self,
        tender: Dict[str, Any],
        documents: Optional[List[Dict[str, Any]]] = None,
        existing_records: Optional[List[Dict[str, Any]]] = None,
        prefetched_data: Optional[PrefetchedTenderData] = None,
        processed_tenders_cache: Optional[Dict[Tuple[int, str], Dict[str, Any]]] = None,
        tender_type: str = 'new',
    ) -> Optional[Dict[str, Any]]:
        """Обработка одного тендера (делегируется TenderProcessor)"""
        tender_id = tender.get("id")
        registry_type = tender.get("registry_type", "44fz")
        folder_path = prefetched_data.folder_path if prefetched_data else self.folder_manager.prepare_tender_folder(tender_id, registry_type, tender_type)
        tender["folder_path"] = folder_path

        return self.tender_processor.process_tender(
            tender=tender,
            documents=documents,
            existing_records=existing_records,
            prefetched_data=prefetched_data,
            processed_tenders_cache=processed_tenders_cache,
            tender_type=tender_type,
            get_tender_documents_func=lambda tid, rt: self._safe_tender_call(
                self.tender_provider.get_tender_documents,
                tid,
                rt,
            ),
        )

    def _get_tender_documents_safe(self, tender_id: int, registry_type: str) -> List[Dict[str, Any]]:
        """Обертка для безопасного получения документов торга."""
        return self._safe_tender_call(
                self.tender_provider.get_tender_documents,
                tender_id,
                registry_type,
            )

    def _prepare_tenders_with_sizes(
        self,
        tenders: List[Dict[str, Any]],
        prefetcher: Optional[TenderPrefetcher],
        tender_type: str = 'new',
    ) -> Tuple[List[Tuple[Dict[str, Any], int]], Dict[int, int]]:
        """
        Подготавливает список тендеров с размерами папок и сортирует по размеру (от меньшего к большему).
        
        Args:
            tenders: Список тендеров для обработки
            prefetcher: Префетчер для скачивания документов (если есть)
            tender_type: Тип торгов ('new' или 'won')
            
        Returns:
            Кортеж из:
            - Список кортежей (tender, folder_size), отсортированный по размеру папки
            - Словарь маппинга id(tender) -> original_index для получения prefetched_data
        """
        if not tenders:
            return []
        
        logger.info("📦 Определение размеров папок для приоритизации обработки...")
        
        # Дожидаемся скачивания документов для всех тендеров через prefetcher
        if prefetcher:
            logger.info(f"Ожидание завершения скачивания документов для {len(tenders)} тендеров...")
            # Получаем prefetched_data для всех тендеров, чтобы дождаться скачивания
            for idx, tender in enumerate(tenders):
                try:
                    prefetcher.get_prefetched_data(idx, tender)
                except Exception as e:
                    logger.debug(f"Ошибка при получении prefetched_data для торга {tender.get('id')}: {e}")
        
        # Определяем размеры папок для всех тендеров и сохраняем маппинг оригинальных индексов
        tenders_with_sizes: List[Tuple[Dict[str, Any], int]] = []
        original_index_map: Dict[int, int] = {}  # id(tender) -> original_index
        
        for original_index, tender in enumerate(tenders):
            tender_id = tender.get("id")
            registry_type = tender.get("registry_type", "44fz")
            folder_path = self.folder_manager.prepare_tender_folder(tender_id, registry_type, tender_type)
            folder_size = self.folder_manager.get_folder_size(folder_path)
            tenders_with_sizes.append((tender, folder_size))
            
            # Сохраняем маппинг для получения prefetched_data
            original_index_map[id(tender)] = original_index
            
            # Логируем размер для отладки
            size_mb = folder_size / (1024 * 1024)
            logger.debug(f"Торг {tender_id} ({registry_type}): размер папки {size_mb:.2f} МБ")
        
        # Сортируем по размеру папки (от меньшего к большему)
        tenders_with_sizes.sort(key=lambda x: x[1])
        
        # Логируем статистику
        if tenders_with_sizes:
            min_size_mb = tenders_with_sizes[0][1] / (1024 * 1024)
            max_size_mb = tenders_with_sizes[-1][1] / (1024 * 1024)
            avg_size_mb = sum(size for _, size in tenders_with_sizes) / len(tenders_with_sizes) / (1024 * 1024)
            logger.info(
                f"✅ Приоритизация завершена: {len(tenders_with_sizes)} тендеров, "
                f"размеры от {min_size_mb:.2f} МБ до {max_size_mb:.2f} МБ (средний: {avg_size_mb:.2f} МБ)"
            )
            logger.info("📋 Обработка будет выполняться от меньших папок к большим для быстрого наполнения БД")
        
        return tenders_with_sizes, original_index_map

    def _create_prefetcher(self, tender_type: str = 'new') -> TenderPrefetcher:
        """Создает настроенный префетчер для фоновой загрузки документов."""
        prefetch_size = min(3, max(1, self.max_workers // 2))
        return TenderPrefetcher(
            folder_manager=self.folder_manager,
            selector=self.selector,
            downloader=self.downloader,
            max_prefetch=prefetch_size,
            tender_type=tender_type,
        )


    def _safe_tender_call(self, func, *args, **kwargs):
        """Безопасный вызов функции с обработкой ошибок через ErrorHandler"""
        return self.error_handler.safe_call(func, *args, **kwargs)

    def _ensure_tender_connection(self):
        if self.tender_db_manager.is_connected():
            return
        self._attempt_connect()

    def _attempt_connect(self):
        try:
            self.tender_db_manager.connect()
        except DatabaseConnectionError as error:
            self._handle_db_disconnect(error)

    def _get_average_processing_time_per_file(self) -> float:
        """
        Получает среднее время обработки одного файла из истории БД.
        
        Returns:
            Среднее время обработки одного файла в секундах, или 0.0 если данных нет
        """
        try:
            from psycopg2.extras import RealDictCursor
            query = """
                SELECT 
                    AVG(processing_time_seconds / NULLIF(total_files_processed, 0)) as avg_time_per_file
                FROM tender_document_matches
                WHERE processing_time_seconds IS NOT NULL 
                    AND total_files_processed > 0
                    AND processing_time_seconds > 0
            """
            results = self.tender_db_manager.execute_query(query, None, RealDictCursor)
            if results and results[0].get('avg_time_per_file'):
                avg_time = float(results[0]['avg_time_per_file'])
                logger.debug(f"Среднее время обработки одного файла из истории: {avg_time:.2f} сек")
                return avg_time
        except Exception as error:
            logger.debug(f"Не удалось получить среднее время обработки из БД: {error}")
        
        return 0.0
    
    def _get_average_processing_time_per_tender(self) -> float:
        """
        Получает среднее время обработки одного торга из истории БД.
        
        Returns:
            Среднее время обработки одного торга в секундах, или 0.0 если данных нет
        """
        try:
            from psycopg2.extras import RealDictCursor
            query = """
                SELECT 
                    AVG(processing_time_seconds) as avg_time_per_tender
                FROM tender_document_matches
                WHERE processing_time_seconds IS NOT NULL 
                    AND processing_time_seconds > 0
            """
            results = self.tender_db_manager.execute_query(query, None, RealDictCursor)
            if results and results[0].get('avg_time_per_tender'):
                avg_time = float(results[0]['avg_time_per_tender'])
                logger.debug(f"Среднее время обработки одного торга из истории: {avg_time:.2f} сек")
                return avg_time
        except Exception as error:
            logger.debug(f"Не удалось получить среднее время обработки торга из БД: {error}")
        
        return 0.0
    
    def _process_existing_folders_parallel(
        self,
        existing_folders: List[Dict[str, Any]],
        processed_tenders_cache: Dict[Tuple[int, str], Dict[str, Any]],
        tender_type: str,
        prefetcher: Optional[TenderPrefetcher],
        new_tenders: List[Dict[str, Any]]
    ) -> None:
        """
        Параллельная обработка существующих папок.
        Пока обрабатываются существующие папки, скачиваются новые торги.
        
        Логика:
        1. Берем самую маленькую папку
        2. Пробуем открыть/обработать
        3. Если не открылось - удаляем файлы, папку, скачиваем заново
        4. Пока скачиваем - проверяем другие папки
        5. Если открылась - обрабатываем, записываем в БД, удаляем папку
        """
        from threading import Lock
        
        processed_count = 0
        errors_count = 0
        lock = Lock()
        failed_tenders: List[Dict[str, Any]] = []  # Торги, которые нужно скачать заново
        
        def process_single_folder(folder_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            """Обрабатывает одну существующую папку"""
            nonlocal processed_count, errors_count
            
            tender_id = folder_data["id"]
            registry_type = folder_data["registry_type"]
            folder_path = folder_data["folder_path"]
            folder_size_mb = folder_data["folder_size"] / (1024 * 1024)
            
            key = (tender_id, registry_type)
            
            # Проверяем кэш
            if key in processed_tenders_cache:
                logger.debug(f"Торг {tender_id} ({registry_type}) уже обработан, пропускаем")
                return None
            
            logger.info(f"Обработка существующей папки {folder_path.name} (размер: {folder_size_mb:.2f} МБ)")
            
            try:
                # Пробуем собрать записи из папки
                existing_records = self.existing_processor.build_records(folder_path)
                if not existing_records:
                    logger.warning(f"Для торга {tender_id} ({registry_type}) не найдено файлов в папке {folder_path.name}")
                    # Удаляем папку и ставим на скачивание
                    self._delete_folder_and_schedule_download(tender_id, registry_type, folder_path, folder_data.get("tender_type", "new"))
                    with lock:
                        errors_count += 1
                    return {"error": "no_files"}
                
                # Создаем торг для обработки
                tender = {
                    "id": tender_id,
                    "registry_type": registry_type,
                    "folder_path": folder_path,
                    "tender_type": folder_data.get("tender_type", "new"),
                }
                
                # Обрабатываем торг
                result = self._process_tender(
                    tender,
                    existing_records=existing_records,
                    processed_tenders_cache=processed_tenders_cache,
                    tender_type=folder_data.get("tender_type", "new")
                )
                
                if result and not result.get("error"):
                    # Успешная обработка - удаляем папку
                    logger.info(f"✅ Торг {tender_id} ({registry_type}) успешно обработан, удаляем папку")
                    self._delete_folder_after_processing(folder_path, tender_id, registry_type)
                    with lock:
                        processed_count += 1
                    return result
                else:
                    # Ошибка обработки - удаляем папку и ставим на скачивание
                    logger.warning(f"❌ Ошибка обработки торга {tender_id} ({registry_type}), удаляем папку и ставим на скачивание")
                    self._delete_folder_and_schedule_download(tender_id, registry_type, folder_path, folder_data.get("tender_type", "new"))
                    with lock:
                        errors_count += 1
                    return {"error": "processing_failed"}
                    
            except Exception as e:
                logger.error(f"❌ Критическая ошибка при обработке существующей папки {folder_path.name}: {e}", exc_info=True)
                # Удаляем папку и ставим на скачивание
                self._delete_folder_and_schedule_download(tender_id, registry_type, folder_path, folder_data.get("tender_type", "new"))
                with lock:
                    errors_count += 1
                return {"error": str(e)}
        
        # Обрабатываем папки параллельно
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # #region agent log
            import json
            import os
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            log_path = os.path.join(project_root, ".cursor", "debug.log")
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "transaction-debug",
                        "hypothesisId": "THREAD_POOL_IDLE",
                        "location": "runner.py:process_existing_folders:thread_pool_start",
                        "message": "Запуск ThreadPoolExecutor для обработки папок",
                        "data": {"max_workers": self.max_workers, "folders_count": len(existing_folders)},
                        "timestamp": int(__import__('time').time() * 1000)
                    }) + "\n")
            except Exception:
                pass
            # #endregion

            futures = [executor.submit(process_single_folder, folder) for folder in existing_folders]
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result and result.get("error") == "processing_failed":
                        # Добавляем в список для скачивания
                        folder_data = existing_folders[futures.index(future)]
                        failed_tenders.append({
                            "id": folder_data["id"],
                            "registry_type": folder_data["registry_type"],
                            "tender_type": folder_data.get("tender_type", "new"),
                        })
                except Exception as e:
                    logger.error(f"Ошибка при получении результата обработки папки: {e}")
        
        logger.info(f"Обработка существующих папок завершена: обработано {processed_count}, ошибок {errors_count}")
        
        # Добавляем неудачные торги в очередь на скачивание
        if failed_tenders and prefetcher:
            logger.info(f"Добавляем {len(failed_tenders)} торгов в очередь на скачивание")
            prefetcher.schedule(failed_tenders, self._get_tender_documents_safe)
        
        return {
            "processed": processed_count,
            "errors": errors_count,
            "total_matches": 0,  # Будет обновлено из результатов обработки
        }
    
    def _process_new_tenders(
        self,
        tenders: List[Dict[str, Any]],
        processed_tenders_cache: Dict[Tuple[int, str], Dict[str, Any]],
        tender_type: str,
        prefetcher: Optional[TenderPrefetcher],
    ) -> Dict[str, Any]:
        """Обрабатывает новые торги из очереди скачивания"""
        queue_manager = TenderQueueManager(self.folder_manager, tender_type)
        queue_manager.add_tenders(tenders)
        
        original_index_map: Dict[int, int] = {}
        for idx, tender in enumerate(tenders):
            original_index_map[id(tender)] = idx
        
        processed = 0
        errors = 0
        index = 0
        
        while queue_manager.has_more():
            try:
                next_item = queue_manager.get_next_tender()
                if next_item is None:
                    break
                
                tender, folder_size = next_item
                tender_id = tender.get("id")
                registry_type = tender.get("registry_type", "44fz")
                key = (tender_id, registry_type)
                
                # Проверяем кэш
                if key in processed_tenders_cache:
                    queue_manager.mark_processed()
                    index += 1
                    continue
                
                # Получаем prefetched_data
                original_index = original_index_map.get(id(tender), index)
                prefetched_data = prefetcher.get_prefetched_data(original_index, tender) if prefetcher else None
                
                # Обрабатываем торг
                # #region agent log
                try:
                    from pathlib import Path
                    import time as _time
                    import json
                    log_path_local = Path(__file__).parent.parent.parent / ".cursor" / "debug.log"
                    with open(log_path_local, "a", encoding="utf-8") as f:
                        f.write(json.dumps({
                            "sessionId": "debug-session",
                            "runId": "subprocess",
                            "hypothesisId": "PROCESS",
                            "location": "runner.py:_process_new_tenders:before_process_tender",
                            "message": "PROCESS_NEW_TENDER_START",
                            "data": {
                                "tender_id": tender_id,
                                "registry_type": registry_type,
                                "tender_type": tender_type,
                                "folder_size_mb": round(folder_size / (1024 * 1024), 2),
                                "prefetched_has_data": prefetched_data is not None,
                                "queue_remaining": queue_manager._remaining
                            },
                            "timestamp": int(_time.time() * 1000)
                        }, ensure_ascii=False) + "\n")
                except Exception:
                    pass
                # #endregion

                result = self._process_tender(
                    tender,
                    prefetched_data=prefetched_data,
                    processed_tenders_cache=processed_tenders_cache,
                    tender_type=tender_type,
                )
                
                queue_manager.mark_processed()
                
                if result and not result.get("error"):
                    # Загружаем на Яндекс Диск и удаляем папку после успешной обработки
                    folder_path = self.folder_manager.get_tender_folder_path(tender_id, registry_type, tender_type)
                    if folder_path and folder_path.exists():
                        self._delete_folder_after_processing(folder_path, tender_id, registry_type)
                    processed += 1
                else:
                    errors += 1
                    # #region agent log
                    try:
                        from pathlib import Path
                        import time as _time
                        import json
                        log_path_local = Path(__file__).parent.parent.parent / ".cursor" / "debug.log"
                        with open(log_path_local, "a", encoding="utf-8") as f:
                            f.write(json.dumps({
                                "sessionId": "debug-session",
                                "runId": "subprocess",
                                "hypothesisId": "PROCESS",
                                "location": "runner.py:_process_new_tenders:process_tender_error",
                                "message": "PROCESS_NEW_TENDER_ERROR",
                                "data": {
                                    "tender_id": tender_id,
                                    "registry_type": registry_type,
                                    "tender_type": tender_type,
                                    "result": result
                                },
                                "timestamp": int(_time.time() * 1000)
                            }, ensure_ascii=False) + "\n")
                    except Exception:
                        pass
                    # #endregion
                
                index += 1
            except Exception as e:
                logger.error(f"Ошибка при обработке нового торга: {e}", exc_info=True)
                errors += 1
                queue_manager.mark_processed()
                index += 1
        
        logger.info(f"Обработка новых торгов завершена: обработано {processed}, ошибок {errors}")
        
        return {
            "processed": processed,
            "errors": errors,
            "skipped_no_docs": 0,
            "total_matches": 0,
        }
    
    def _upload_folder_to_yandex_disk(self, folder_path: Path, tender_id: int, registry_type: str) -> bool:
        """
        Загружает папку с документами на Яндекс Диск.
        Использует CloudUploader для выполнения операции.
        
        Args:
            folder_path: Путь к локальной папке
            tender_id: ID торга
            registry_type: Тип реестра (44fz или 223fz)
            
        Returns:
            True если загрузка успешна, False иначе
        """
        return self.cloud_uploader.upload_folder_to_yandex_disk(folder_path, tender_id, registry_type)
    
    def _delete_folder_after_processing(self, folder_path: Path, tender_id: int = None, registry_type: str = None) -> None:
        """
        Удаляет папку после успешной обработки.
        Перед удалением загружает на Яндекс Диск (если включено).
        """
        # Загружаем на Яндекс Диск перед удалением (если включено)
        if tender_id and registry_type:
            self._upload_folder_to_yandex_disk(folder_path, tender_id, registry_type)
        
        try:
            if folder_path.exists():
                self.folder_manager.clean_tender_folder_force(folder_path)
                folder_path.rmdir()
                logger.debug(f"Папка {folder_path.name} успешно удалена после обработки")
        except Exception as e:
            logger.warning(f"Не удалось удалить папку {folder_path.name} после обработки: {e}")
    
    def _delete_folder_and_schedule_download(
        self,
        tender_id: int,
        registry_type: str,
        folder_path: Path,
        tender_type: str
    ) -> None:
        """Удаляет папку и планирует скачивание заново"""
        try:
            if folder_path.exists():
                # Используем FolderProcessor для удаления папки
                self.folder_processor.delete_folder_after_processing(folder_path, tender_id, registry_type)
                logger.info(f"Папка {folder_path.name} удалена, торг {tender_id} будет скачан заново")
        except Exception as e:
            logger.warning(f"Не удалось удалить папку {folder_path.name}: {e}")
    
    def _handle_failed_existing_tender(self, tender: Dict[str, Any], queue_manager: TenderQueueManager) -> None:
        """
        Обрабатывает случай, когда обработка существующего файла не удалась.
        Удаляет папку и добавляет торг в очередь на скачивание.
        
        Args:
            tender: Словарь с данными торга
            queue_manager: Менеджер очереди для добавления торга на скачивание
        """
        tender_id = tender.get("id")
        registry_type = tender.get("registry_type", "44fz")
        folder_path = tender.get("folder_path")
        
        try:
            # Удаляем папку существующего файла
            if folder_path and folder_path.exists():
                logger.info(f"Удаление папки существующего файла для торга {tender_id} ({registry_type}): {folder_path}")
                # Используем FolderProcessor для удаления папки
                self.folder_processor.delete_folder_after_processing(folder_path, tender_id, registry_type)
                logger.info(f"Папка {folder_path.name} успешно удалена")
            
            # Создаём новый торг без пометки _is_existing для скачивания
            new_tender = {
                "id": tender_id,
                "registry_type": registry_type,
                "tender_type": tender.get("tender_type", "new"),
                # Убираем пометку _is_existing и folder_path
            }
            
            # Добавляем в очередь на скачивание
            queue_manager.add_tenders([new_tender])
            logger.info(f"Торг {tender_id} ({registry_type}) добавлен в очередь на скачивание")
        except Exception as e:
            logger.error(f"Ошибка при обработке неудачной обработки существующего файла для торга {tender_id} ({registry_type}): {e}")
    
    @staticmethod
    def _format_eta(seconds: float) -> str:
        """Форматирует время в читаемый формат."""
        if seconds < 60:
            return f"{int(seconds)} сек"
        if seconds < 3600:
            minutes = int(seconds / 60)
            sec = int(seconds % 60)
            return f"{minutes} мин {sec} сек"
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours} ч {minutes} мин"
    
    def _handle_db_disconnect(self, error: Exception):
        """Обрабатывает отключение базы данных с помощью ErrorHandler"""
        self.error_handler.handle_db_disconnect(error)

