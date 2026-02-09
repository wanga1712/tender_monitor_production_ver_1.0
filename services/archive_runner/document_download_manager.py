"""
MODULE: services.archive_runner.document_download_manager
RESPONSIBILITY: Manage parallel downloading of tender documents.
ALLOWED: DocumentDownloader, ThreadPoolExecutor, logging.
FORBIDDEN: Processing/Extraction logic (delegate).
ERRORS: None.

Менеджер скачивания документов для тендеров.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Any

from loguru import logger

from services.document_search.document_downloader import DocumentDownloader


class DocumentDownloadManager:
    """Отвечает за параллельное скачивание документов тендера."""

    def __init__(self, downloader: DocumentDownloader, max_workers: int = 2):
        self.downloader = downloader
        self.max_workers = max(1, max_workers)  # Минимум 1 поток

    def download_documents(
        self,
        unique_docs: List[Dict[str, Any]],
        all_documents: List[Dict[str, Any]],
        tender_folder: Path,
    ) -> List[Dict[str, Any]]:
        """
        Скачивает набор документов и возвращает информацию о путях.
        """
        records: List[Dict[str, Any]] = []
        if not unique_docs:
            return records

        worker_count = min(self.max_workers, len(unique_docs)) or 1

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
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
                doc_name = doc.get("file_name")
                try:
                    paths = future.result(timeout=600)
                    if paths:
                        records.append(
                            {
                                "doc": doc,
                                "paths": paths,
                                "source": "download",
                                "retries": 0,
                            }
                        )
                        logger.info(f"Скачан документ {doc_name} ({len(paths)} файлов)")
                except Exception as error:
                    logger.error(f"Ошибка скачивания документа {doc_name}: {error}")

        return records

