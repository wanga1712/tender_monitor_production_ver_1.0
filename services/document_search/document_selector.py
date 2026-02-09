"""
MODULE: services.document_search.document_selector
RESPONSIBILITY: Select and prioritize documents for download/processing from a list.
ALLOWED: logging, re.
FORBIDDEN: Network access, file IO.
ERRORS: DocumentSearchError.

Модуль для выбора и группировки документов.

Класс DocumentSelector отвечает за:
- Выбор документов для обработки (сметы, Excel файлы)
- Группировку многочастных архивов
- Определение приоритета документов
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re

from loguru import logger

from core.exceptions import DocumentSearchError


class DocumentSelector:
    """Класс для выбора и группировки документов."""

    KEYWORD_PATTERN = re.compile(r"смет\w*", re.IGNORECASE)
    ARCHIVE_PATTERN = re.compile(
        r"^(?P<base>.+?)(?:[._ -]*(?:part)?(?P<part>\d+))?\.(rar|zip|7z)$",
        re.IGNORECASE,
    )

    def choose_documents(self, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Выбор документов для обработки:
        - ВСЕ документы и архивы к закупке (без фильтрации по типу)
        - Обработка будет происходить в зависимости от расширения файла
        """
        matches: List[Dict[str, Any]] = []
        seen_docs = set()
        
        for doc in documents:
            file_name = doc.get("file_name") or ""
            link = doc.get("document_links") or ""
            
            # Пропускаем документы без имени и ссылки
            if not file_name and not link:
                logger.debug(f"Пропущен документ без имени и ссылки: {doc.get('id')}")
                continue
            
            doc_key = (file_name, link)
            if doc_key in seen_docs:
                continue
            seen_docs.add(doc_key)
            
            # Принимаем ВСЕ документы - обработка будет по расширению
            matches.append(doc)
            logger.debug(f"Документ выбран для скачивания: {file_name or link}")

        if not matches:
            raise DocumentSearchError(
                "Не найдено документов для обработки."
            )

        matches.sort(key=self._document_priority)
        logger.info(f"Выбрано документов для скачивания и анализа: {len(matches)}")
        for doc in matches:
            logger.info(f"  - {doc.get('file_name') or doc.get('document_links')}")
        return matches

    def _document_priority(self, doc: Dict[str, Any]) -> Tuple[int, str]:
        """Определение приоритета документов (сначала XLSX, затем архивы)."""
        name = (doc.get("file_name") or "").lower()
        if name.endswith(".xlsx"):
            return (0, name)
        if name.endswith(".xls"):
            return (1, name)
        if name.endswith((".rar", ".zip", ".7z")):
            return (2, name)
        return (3, name)

    def group_documents_by_archive(
        self,
        selected_docs: List[Dict[str, Any]],
        all_documents: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Группирует документы по архивам, чтобы не скачивать части одного архива несколько раз.
        
        Для многочастных архивов возвращает только первую часть.
        """
        unique_docs: List[Dict[str, Any]] = []
        processed_archives: set = set()
        
        for doc in selected_docs:
            if not self.is_rar_document(doc):
                unique_docs.append(doc)
                continue
            
            base_name, _ = self.split_archive_name(doc.get("file_name"))
            if base_name:
                archive_key = base_name.casefold()
                if archive_key in processed_archives:
                    logger.debug(f"Архив {doc.get('file_name')} уже обработан, пропускаем")
                    continue
                processed_archives.add(archive_key)
            
            unique_docs.append(doc)
        
        logger.info(f"После группировки по архивам: {len(unique_docs)} уникальных документов/архивов")
        return unique_docs

    def is_rar_document(self, document: Dict[str, Any]) -> bool:
        """Проверка, является ли документ архивом (RAR, ZIP, 7Z)."""
        name = (document.get("file_name") or "").lower()
        return name.endswith((".rar", ".zip", ".7z"))

    def split_archive_name(self, file_name: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
        """Выделение базовой части имени архива и номера части."""
        if not file_name:
            return None, None
        match = self.ARCHIVE_PATTERN.match(file_name)
        if not match:
            return None, None
        base = match.group("base").casefold()
        part = match.group("part")
        return base, int(part) if part is not None else None

