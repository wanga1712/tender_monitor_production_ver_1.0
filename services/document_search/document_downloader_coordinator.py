"""
MODULE: services.document_search.document_downloader_coordinator
RESPONSIBILITY: Coordinate high-level document downloading logic (retries, progress).
ALLOWED: DocumentDownloader, ThreadPoolExecutor, logging.
FORBIDDEN: Direct HTTP requests (use DocumentDownloader).
ERRORS: DocumentSearchError.

Модуль для координации скачивания документов.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger

from core.exceptions import DocumentSearchError
from services.document_search.document_downloader import DocumentDownloader


class DocumentDownloaderCoordinator:
    """Класс для координации скачивания документов"""
    
    def __init__(
        self,
        downloader: DocumentDownloader,
        progress_callback: Optional[callable] = None,
    ):
        """
        Инициализация координатора скачивания
        
        Args:
            downloader: Загрузчик документов
            progress_callback: Функция для обновления прогресса
        """
        self.downloader = downloader
        self.progress_callback = progress_callback
    
    def _update_progress(self, stage: str, progress: int, detail: Optional[str] = None):
        """Обновление прогресса через callback"""
        if self.progress_callback:
            try:
                self.progress_callback(stage, progress, detail)
            except Exception as error:
                logger.debug(f"Ошибка при обновлении прогресса: {error}")
    
    def download_documents(
        self,
        unique_docs_to_download: List[Dict[str, Any]],
        documents: List[Dict[str, Any]],
        tender_folder: Path,
    ) -> List[Path]:
        """Скачивание документов с повторными попытками"""
        total_to_download = len(unique_docs_to_download)
        max_attempts = 2
        attempt = 1
        last_error: Optional[Exception] = None

        while attempt <= max_attempts:
            all_downloaded_paths: List[Path] = []
            downloaded_count = 0
            self._update_progress(
                "Скачивание документов",
                0,
                f"Найдено документов для скачивания: {total_to_download}",
            )
            logger.info(
                "Начинаю параллельное скачивание %s документов/архивов (попытка %s/%s)",
                total_to_download,
                attempt,
                max_attempts,
            )

            try:
                with ThreadPoolExecutor(max_workers=min(8, total_to_download or 1)) as executor:
                    future_to_doc = {
                        executor.submit(
                            self.downloader.download_required_documents,
                            target_doc,
                            documents,
                            tender_folder,
                        ): target_doc
                        for target_doc in unique_docs_to_download
                    }

                    for future in as_completed(future_to_doc):
                        target_doc = future_to_doc[future]
                        doc_name = target_doc.get('file_name') or target_doc.get('document_links', 'unknown')
                        try:
                            downloaded_paths = future.result(timeout=600)
                            all_downloaded_paths.extend(downloaded_paths)
                            downloaded_count += 1
                            progress = int((downloaded_count / total_to_download) * 30) if total_to_download > 0 else 0
                            self._update_progress(
                                "Скачивание документов",
                                progress,
                                f"Скачано: {downloaded_count}/{total_to_download} - {doc_name}"
                            )
                            logger.info(f"Успешно скачан документ/архив: {doc_name}")
                        except Exception as error:
                            logger.error(f"Ошибка при скачивании документа {doc_name}: {error}")
                            raise

                if not all_downloaded_paths:
                    raise DocumentSearchError("Не удалось скачать ни один документ.")

                self._update_progress(
                    "Скачивание документов",
                    30,
                    f"Скачано документов: {downloaded_count}/{total_to_download}",
                )
                return all_downloaded_paths
            except DocumentSearchError as error:
                last_error = error
                logger.warning(
                    "Ошибка при обработке документов (попытка %s/%s): %s",
                    attempt,
                    max_attempts,
                    error,
                )
                attempt += 1
            except Exception as error:
                last_error = error
                logger.warning(
                    "Не удалось завершить попытку скачивания (попытка %s/%s): %s",
                    attempt,
                    max_attempts,
                    error,
                )
                attempt += 1

        raise DocumentSearchError(str(last_error)) if last_error else DocumentSearchError(
            "Не удалось скачать документы после всех попыток."
        )

