"""
MODULE: services.archive_runner.tender_prefetcher
RESPONSIBILITY: Background downloading of tender documents to optimize throughput.
ALLOWED: TenderFolderManager, DocumentSelector, DocumentDownloader, ThreadPoolExecutor, logging.
FORBIDDEN: Heavy data processing (download only).
ERRORS: None.

Модуль для фоновой предзагрузки документов тендеров.
"""

from __future__ import annotations

from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, Future, as_completed, TimeoutError as FuturesTimeoutError
from pathlib import Path
from typing import Callable, Dict, List, Optional, Any

from loguru import logger

from services.archive_runner.tender_folder_manager import TenderFolderManager
from services.document_search.document_selector import DocumentSelector
from services.document_search.document_downloader import DocumentDownloader


@dataclass
class PrefetchedTenderData:
    """Результат фоновой загрузки документов для тендера."""

    tender_id: int
    registry_type: str
    folder_path: Path
    documents: List[Dict[str, Any]]
    download_records: List[Dict[str, Any]]
    cleaned: bool = False


class TenderPrefetcher:
    """
    Управляет полосой предварительной загрузки тендеров.

    Пока основной поток обрабатывает текущий торг, следующие закупки
    скачиваются в фоне.
    """

    def __init__(
        self,
        folder_manager: TenderFolderManager,
        selector: DocumentSelector,
        downloader: DocumentDownloader,
        max_prefetch: int = 2,
        tender_type: str = 'new',
    ):
        self.folder_manager = folder_manager
        self.selector = selector
        self.downloader = downloader
        self.max_prefetch = max(1, max_prefetch)
        self.executor = ThreadPoolExecutor(max_workers=self.max_prefetch)
        self.tender_type = tender_type

        self._tenders: List[Dict[str, Any]] = []
        self._get_documents: Optional[Callable[[int, str], List[Dict[str, Any]]]] = None
        self._futures: Dict[int, Future] = {}

    def schedule(
        self,
        tenders: List[Dict[str, Any]],
        get_documents_func: Callable[[int, str], List[Dict[str, Any]]],
    ) -> None:
        """Инициализирует очередь и запускает первые задания."""
        self._tenders = tenders
        self._get_documents = get_documents_func
        self._futures.clear()

        for idx in range(min(self.max_prefetch, len(tenders))):
            self._submit_prefetch(idx)

    def get_prefetched_data(
        self,
        index: int,
        tender: Dict[str, Any],
    ) -> Optional[PrefetchedTenderData]:
        """
        Возвращает результат предзагрузки для тендера.
        Если фоновой задачи нет или она упала, возвращается None.
        """
        future = self._futures.pop(index, None)
        if future is None:
            tender_id = tender.get("id")
            logger.debug(
                f"Предзагрузка для торга {tender_id} отсутствует, работаем синхронно"
            )
            return None

            tender_id = tender.get("id")
            registry_type = tender.get("registry_type", "44fz")
        
        try:
            # Таймаут 120 секунд на получение результата prefetch
            result = future.result(timeout=120)
        except FuturesTimeoutError:
            logger.warning(
                f"Таймаут предзагрузки для торга {tender_id} ({registry_type}), работаем синхронно"
            )
            # Не отменяем future - пусть завершится в фоне
            result = None
        except Exception as error:
            logger.warning(
                f"Фоновая загрузка для торга {tender_id} ({registry_type}) завершилась ошибкой: {error}"
            )
            result = None

        self._submit_prefetch(index + self.max_prefetch)
        return result

    def shutdown(self) -> None:
        """Завершает пул потоков с защитой от Access Violation."""
        try:
            # Отменяем все незавершенные задачи с защитой
            if hasattr(self, '_futures') and self._futures:
                for future in list(self._futures.values()):  # Создаем копию списка для безопасности
                    try:
                        if not future.done():
                            future.cancel()
                    except Exception as cancel_error:
                        logger.debug(f"Ошибка при отмене задачи: {cancel_error}")
            
            # Закрываем executor с ожиданием завершения текущих задач
            if hasattr(self, 'executor') and self.executor:
                try:
                    # Python 3.11/3.12: shutdown не принимает timeout
                    self.executor.shutdown(wait=True)
                except Exception as shutdown_error:
                    logger.warning(f"Ошибка при закрытии executor: {shutdown_error}")
        except Exception as e:
            logger.warning(f"Ошибка при закрытии префетчера: {e}", exc_info=True)

    def _submit_prefetch(self, index: int) -> None:
        if index >= len(self._tenders) or self._get_documents is None:
            return

        tender = self._tenders[index]
        self._futures[index] = self.executor.submit(self._prefetch_tender, tender)

    def _prefetch_tender(self, tender: Dict[str, Any]) -> PrefetchedTenderData:
        """
        Предзагрузка документов для тендера.
        
        ВАЖНО: Файлы скачиваются, но НЕ проверяются на валидность здесь.
        Проверка происходит в TenderProcessor._check_existing_files() перед парсингом.
        """
        tender_id = tender.get("id")
        registry_type = tender.get("registry_type", "44fz")
        folder_path = self._prepare_tender_folder(tender_id, registry_type)
        
        documents = self._safe_get_documents(tender_id, registry_type)
        if not documents:
            logger.info(f"Для торга {tender_id} ({registry_type}) нет документов для предзагрузки")
            return self._create_prefetched_data(tender_id, registry_type, folder_path, [], [])
        
        records = self._process_documents(documents, folder_path)
        logger.info(
            f"Фоновая загрузка завершена: торг {tender_id} ({registry_type}), файлов {len(records)}"
        )
        return self._create_prefetched_data(tender_id, registry_type, folder_path, documents, records)
    
    def _prepare_tender_folder(self, tender_id: int, registry_type: str) -> Path:
        """Подготовка папки для тендера"""
        folder_path = self.folder_manager.prepare_tender_folder(tender_id, registry_type, self.tender_type)
        self.folder_manager.clean_tender_folder_force(folder_path)
        return folder_path
    
    def _process_documents(self, documents: List[Dict[str, Any]], folder_path: Path) -> List[Dict[str, Any]]:
        """Обработка и скачивание документов"""
        selected = self.selector.choose_documents(documents)
        grouped = self.selector.group_documents_by_archive(selected, documents)
        return self._download_documents(grouped, documents, folder_path)
    
    def _create_prefetched_data(
        self,
        tender_id: int,
        registry_type: str,
        folder_path: Path,
        documents: List[Dict[str, Any]],
        download_records: List[Dict[str, Any]]
    ) -> PrefetchedTenderData:
        """Создание объекта PrefetchedTenderData"""
        return PrefetchedTenderData(
            tender_id=tender_id,
            registry_type=registry_type,
            folder_path=folder_path,
            documents=documents,
            download_records=download_records,
            cleaned=True,
        )

    def _safe_get_documents(self, tender_id: int, registry_type: str) -> List[Dict[str, Any]]:
        if not self._get_documents:
            return []
        try:
            return self._get_documents(tender_id, registry_type) or []
        except Exception as error:
            logger.warning(
                "Не удалось получить документы для торга %s (%s): %s",
                tender_id,
                registry_type,
                error,
            )
            return []

    def _download_documents(
        self,
        unique_docs: List[Dict[str, Any]],
        all_documents: List[Dict[str, Any]],
        tender_folder: Path,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        if not unique_docs:
            return records

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
                    "hypothesisId": "PREFETCHER_IDLE",
                    "location": "tender_prefetcher.py:_prefetch_documents:thread_pool_start",
                    "message": "Запуск ThreadPoolExecutor для скачивания документов",
                    "data": {"max_workers": min(self.max_prefetch, len(unique_docs)), "docs_count": len(unique_docs)},
                    "timestamp": int(__import__('time').time() * 1000)
                }) + "\n")
        except Exception:
            pass
        # #endregion

        with ThreadPoolExecutor(max_workers=min(self.max_prefetch, len(unique_docs))) as executor:
            future_to_doc = {
                executor.submit(
                    self.downloader.download_required_documents,
                    doc,
                    all_documents,
                    tender_folder,
                ): doc
                for doc in unique_docs
            }

            for future in as_completed(future_to_doc):
                doc = future_to_doc[future]
                try:
                    paths = future.result(timeout=600)
                    if paths:
                        records.append(
                            {
                                "doc": doc,
                                "paths": paths,
                                "source": "prefetch",
                                "retries": 0,
                            }
                        )
                except Exception as error:
                    file_name = doc.get("file_name", "неизвестный файл") if doc else "неизвестный файл"
                    logger.error(
                        f"Ошибка предзагрузки документа {file_name}: {error}",
                        exc_info=True
                    )
        return records

