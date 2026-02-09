"""
MODULE: services.document_search.document_downloader
RESPONSIBILITY: Download documents from remote URLs.
ALLOWED: requests, ThreadPoolExecutor, logging, services.document_search.document_selector.
FORBIDDEN: Processing file content (except size check), database access.
ERRORS: DocumentSearchError.

Модуль для загрузки документов с сервера.

Класс DocumentDownloader отвечает за:
- Скачивание документов по URL
- Обработку многочастных архивов
- Параллельную загрузку файлов
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time

import requests
from loguru import logger

from core.exceptions import DocumentSearchError


class DocumentDownloader:
    """Класс для загрузки документов с сервера."""

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://zakupki.gov.ru/",
        "Connection": "keep-alive",
    }

    ARCHIVE_PATTERN = re.compile(
        r"^(?P<base>.+?)(?:[._ -]*(?:part)?(?P<part>\d+))?\.(rar|zip|7z)$",
        re.IGNORECASE,
    )

    def __init__(
        self,
        download_dir: Path,
        progress_callback: Optional[callable] = None,
        timeout_calculator: Optional[Callable[[Optional[int], Optional[str]], None]] = None,
    ):
        """
        Args:
            download_dir: Директория для сохранения файлов
            progress_callback: Функция для обновления прогресса (stage, progress, detail)
            timeout_calculator: Функция для расчета таймаута (file_size_bytes, file_name) -> None
            Таймауты полностью убраны - файлы могут скачиваться сколь угодно долго
        """
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.progress_callback = progress_callback
        self.timeout_calculator = timeout_calculator
        self.http_session = requests.Session()
        self.http_session.headers.update(self.DEFAULT_HEADERS)
        self._active_downloads: List[Path] = []

    def _update_progress(self, stage: str, progress: int, detail: Optional[str] = None):
        """Обновление прогресса через callback"""
        if self.progress_callback:
            try:
                self.progress_callback(stage, progress, detail)
            except Exception as error:
                logger.debug(f"Ошибка при обновлении прогресса: {error}")

    def _calculate_timeout(
        self,
        file_size_bytes: Optional[int] = None,
        file_name: Optional[str] = None,
    ) -> Optional[Tuple[int, Optional[int]]]:
        """
        Вычисляет таймаут на основе размера файла и типа.
        Таймауты полностью убраны - файлы могут скачиваться сколь угодно долго.
        
        Args:
            file_size_bytes: Размер файла в байтах (если известен)
            file_name: Имя файла (для определения типа и коэффициента запаса)
            
        Returns:
            None - без таймаутов вообще
        """
        if self.timeout_calculator:
            result = self.timeout_calculator(file_size_bytes, file_name)
            # Если калькулятор вернул таймауты, но мы хотим их убрать
            if result and result[1] is not None:
                return None
            return result
        
        # Полностью убираем таймауты
        return None

    def download_document(
        self,
        document: Dict[str, Any],
        target_dir: Optional[Path] = None,
    ) -> Path:
        """Скачивание документа по ссылке."""
        url = document.get("document_links")
        if not url:
            raise DocumentSearchError("У выбранного документа отсутствует ссылка для скачивания.")

        raw_name = document.get("file_name") or f"document_{document.get('id', 'unknown')}"
        file_name = self._sanitize_filename(raw_name)
        suffix = Path(file_name).suffix.lower()
        if not suffix:
            file_name = f"{file_name}.xlsx"

        destination_dir = target_dir or self.download_dir
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination = destination_dir / file_name
        logger.info(f"Начинаю скачивание документа '{file_name}' по ссылке {url}")

        try:
            # Пытаемся получить размер файла из document или через HEAD запрос
            file_size_bytes = document.get("file_size")
            if not file_size_bytes:
                # Пробуем HEAD запрос для получения размера (без таймаута)
                try:
                    head_response = self.http_session.head(url, timeout=None, allow_redirects=True)
                    content_length = head_response.headers.get('content-length')
                    if content_length:
                        file_size_bytes = int(content_length)
                except Exception:
                    pass  # Если HEAD не работает, продолжаем без размера
            
            # Вычисляем таймаут (теперь всегда None - без таймаутов)
            timeout_config = self._calculate_timeout(file_size_bytes, file_name)
            logger.debug(
                f"Скачивание файла {file_name} без таймаутов"
                + (f" (размер: {file_size_bytes / 1024 / 1024:.2f} MB)" if file_size_bytes else "")
            )
            
            response = self.http_session.get(
                url,
                timeout=timeout_config,  # None - без таймаутов
                stream=True,
                allow_redirects=True
            )
            response.raise_for_status()
            
            total_size = response.headers.get('content-length')
            if total_size:
                total_size = int(total_size)
                logger.debug(f"Размер файла: {total_size / 1024 / 1024:.2f} MB")
            
            downloaded = 0
            last_progress_update = 0
            with destination.open("wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        downloaded += len(chunk)
                        # Обновляем прогресс каждые 1MB или каждые 5%
                        if total_size:
                            progress_percent = (downloaded / total_size) * 100
                            if (downloaded - last_progress_update >= 1024 * 1024) or (progress_percent - last_progress_update >= 5):
                                logger.debug(f"Прогресс скачивания {file_name}: {progress_percent:.1f}%")
                                last_progress_update = progress_percent
                        else:
                            if downloaded - last_progress_update >= 1024 * 1024:
                                logger.debug(f"Скачано {file_name}: {downloaded / 1024 / 1024:.1f} MB")
                                last_progress_update = downloaded
                            
        except requests.Timeout as error:
            logger.error(f"Таймаут при скачивании документа '{file_name}': {error}")
            if destination.exists():
                try:
                    destination.unlink()
                except Exception:
                    pass
            raise DocumentSearchError(
                f"Таймаут при скачивании документа '{file_name}'. "
                f"Файл слишком большой или соединение медленное."
            ) from error
        except requests.RequestException as error:
            logger.error(f"Ошибка скачивания документа '{file_name}': {error}")
            if destination.exists():
                try:
                    destination.unlink()
                except Exception:
                    pass
            raise DocumentSearchError(f"Не удалось скачать документ '{file_name}'.") from error

        logger.info(f"Документ сохранен: {destination}")
        self._register_download(destination)
        return destination

    def download_required_documents(
        self,
        primary_doc: Dict[str, Any],
        all_documents: List[Dict[str, Any]],
        target_dir: Path,
    ) -> List[Path]:
        """Скачивание основного файла и всех частей архива (если необходимо)."""
        # Проверяем, является ли документ архивом
        name = (primary_doc.get("file_name") or "").lower()
        is_archive = name.endswith((".rar", ".zip", ".7z"))
        
        if not is_archive:
            return [self.download_document(primary_doc, target_dir)]

        related_docs = self._collect_related_archives(primary_doc, all_documents)
        logger.info(
            f"Для архива найдено частей: {len(related_docs)} "
            f"({', '.join(doc.get('file_name') or '' for doc in related_docs)})",
        )
        paths: List[Path] = []
        batch_size = 8
        total_docs = len(related_docs)
        for start in range(0, total_docs, batch_size):
            end = min(start + batch_size, total_docs)
            chunk = related_docs[start:end]
            logger.info(
                f"Скачивание документов {start + 1}-{end} из {total_docs} параллельно",
            )
            chunk_paths = self._download_documents_batch(chunk, target_dir)
            paths.extend(chunk_paths)
        return paths

    def _download_documents_batch(
        self,
        documents: List[Dict[str, Any]],
        target_dir: Path,
    ) -> List[Path]:
        """Параллельная загрузка группы документов с сохранением порядка."""
        ordered_paths: List[Optional[Path]] = [None] * len(documents)
        with ThreadPoolExecutor(max_workers=min(8, len(documents))) as executor:
            future_map = {
                executor.submit(self.download_document, doc, target_dir): index
                for index, doc in enumerate(documents)
            }

            try:
                for future in as_completed(future_map):
                    index = future_map[future]
                    ordered_paths[index] = future.result()
            except Exception as error:
                failed_future = future
                for fut in future_map:
                    fut.cancel()
                failed_index = future_map[failed_future]
                failed_doc = documents[failed_index]
                file_name = failed_doc.get("file_name") or failed_doc.get("document_links")
                raise DocumentSearchError(f"Не удалось скачать документ {file_name}") from error

        return [path for path in ordered_paths if path]

    def _collect_related_archives(
        self,
        primary_doc: Dict[str, Any],
        all_documents: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Сбор всех частей многофайлового архива."""
        from services.document_search.document_selector import DocumentSelector
        
        selector = DocumentSelector()
        primary_base, _ = selector.split_archive_name(primary_doc.get("file_name"))
        if not primary_base:
            return [primary_doc]

        related: List[Tuple[int, Dict[str, Any]]] = []
        for doc in all_documents:
            if not selector.is_rar_document(doc):
                continue
            base, part = selector.split_archive_name(doc.get("file_name"))
            if base == primary_base:
                part_index = part if part is not None else 0
                related.append((part_index, doc))

        if not related:
            return [primary_doc]

        related.sort(key=lambda item: item[0])
        ordered_docs: List[Dict[str, Any]] = []
        seen_names = set()
        for _, doc in related:
            name = doc.get("file_name")
            if name in seen_names:
                continue
            seen_names.add(name)
            ordered_docs.append(doc)

        return ordered_docs

    def _register_download(self, path: Path) -> None:
        """Регистрирует скачанный файл для последующей очистки."""
        if path not in self._active_downloads:
            self._active_downloads.append(path)

    @property
    def active_downloads(self) -> List[Path]:
        """Возвращает список скачанных файлов."""
        return self._active_downloads.copy()

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        """Удаление запрещенных символов из имени файла."""
        sanitized = re.sub(r"[<>:\"/\\\\|?*]", "_", name)
        return sanitized.strip() or "document.xlsx"

