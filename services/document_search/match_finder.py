"""
MODULE: services.document_search.match_finder
RESPONSIBILITY: Search for product matches within documents using specified logic.
ALLOWED: ExcelParser, DocumentParser, document_search_base, keyword_matcher, logging.
FORBIDDEN: Direct database access.
ERRORS: None.

Модуль для поиска совпадений товаров в документах.

Класс MatchFinder отвечает за:
- Извлечение ключевых слов из названий товаров
- Поиск совпадений в тексте с учетом опечаток
- Оценку качества совпадений
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import gc

from loguru import logger

from services.document_search.excel_parser import ExcelParser
from services.document_search.document_parser import DocumentParser
from services.document_search.pdf_search import search_additional_phrases_in_pdf
from services.document_search.document_search_base import search_document_for_products
from services.document_search.excel_additional_phrases import search_additional_phrases_in_excel
from services.document_search.keyword_matcher import extract_keywords, check_keywords_match


class MatchFinder:
    """Класс для поиска совпадений товаров в документах."""

    def __init__(
        self,
        product_names: List[str],
        stop_phrases: Optional[List[str]] = None,
        user_search_phrases: Optional[List[str]] = None,
    ):
        """
        Args:
            product_names: Список названий товаров для поиска
            stop_phrases: Список стоп-фраз, при наличии которых ячейки/текст
                должны игнорироваться при поиске (например, обобщающие описания).
            user_search_phrases: Список пользовательских фраз для поиска по документации
                (например, "инъектирование", "усиление"). Объединяются с дополнительными фразами.
        """
        self.product_names = product_names
        self.stop_phrases: List[str] = stop_phrases or []
        self.user_search_phrases: List[str] = user_search_phrases or []
        self._product_patterns: Optional[Dict[str, Dict[str, Any]]] = None
        self._excel_parser = ExcelParser()
        self._document_parser = DocumentParser()

    def search_workbook_for_products(self, file_path: Path) -> List[Dict[str, Any]]:
        """Парсинг Excel и поиск совпадений с названиями товаров по ключевым словам."""
        if not self.product_names:
            logger.warning("Список товаров пуст, поиск не будет выполнен.")
            return []

        if self._product_patterns is None:
            self._prepare_product_patterns()

        return search_document_for_products(
            file_path=file_path,
            cell_iterator=self._excel_parser.iter_workbook_cells(file_path),
            product_patterns=self._product_patterns,
            check_keywords_match_func=check_keywords_match,
            file_type="Excel",
            log_interval=10.0,
            log_every_n=10000,
            extract_row_data_func=lambda fp, sheet, row: self._excel_parser.extract_row_data(fp, sheet, row),
            extract_row_context_func=lambda fp, sheet, row, col, ctx: self._excel_parser.extract_full_row_with_context(fp, sheet, row, col, context_cols=ctx),
            stop_phrases=self.stop_phrases,
        )
    
    def search_pdf_for_products(self, file_path: Path) -> List[Dict[str, Any]]:
        """Парсинг PDF и поиск совпадений с названиями товаров по ключевым словам."""
        if not self.product_names:
            logger.warning("Список товаров пуст, поиск не будет выполнен.")
            return []
        
        if self._product_patterns is None:
            self._prepare_product_patterns()

        from services.document_search.pdf_processor import PDFProcessor
        pdf_processor = PDFProcessor()

        return search_document_for_products(
            file_path=file_path,
            cell_iterator=pdf_processor.iter_pdf_cells(file_path),
            product_patterns=self._product_patterns,
            check_keywords_match_func=check_keywords_match,
            file_type="PDF",
            log_interval=10.0,
            log_every_n=10000,
            stop_phrases=self.stop_phrases,
        )
    
    def search_additional_phrases(self, file_path: Path) -> List[Dict[str, Any]]:
        """Поиск дополнительных фраз в Excel файлах."""
        # Используем фразы из additional_phrases.py (инъектирование, усиление и т.д.)
        # Пользовательские фразы из БД пока не используются, все фразы в additional_phrases.py
        from services.document_search.additional_phrases import get_additional_search_phrases
        additional_phrases = get_additional_search_phrases()
        # Если есть пользовательские фразы из БД, объединяем их
        if self.user_search_phrases:
            all_phrases = list(set(additional_phrases + self.user_search_phrases))
        else:
            all_phrases = additional_phrases
        return search_additional_phrases_in_excel(file_path, self._excel_parser, custom_phrases=all_phrases)
    
    def search_additional_phrases_in_pdf(self, file_path: Path) -> List[Dict[str, Any]]:
        """Поиск дополнительных фраз в PDF файлах."""
        # Используем фразы из additional_phrases.py (инъектирование, усиление и т.д.)
        from services.document_search.additional_phrases import get_additional_search_phrases
        additional_phrases = get_additional_search_phrases()
        # Если есть пользовательские фразы из БД, объединяем их
        if self.user_search_phrases:
            all_phrases = list(set(additional_phrases + self.user_search_phrases))
        else:
            all_phrases = additional_phrases
        return search_additional_phrases_in_pdf(file_path, custom_phrases=all_phrases)
    
    def search_word_for_products(self, file_path: Path) -> List[Dict[str, Any]]:
        """Парсинг Word документа и поиск совпадений с названиями товаров по ключевым словам."""
        if not self.product_names:
            logger.warning("Список товаров пуст, поиск не будет выполнен.")
            return []
        
        if self._product_patterns is None:
            self._prepare_product_patterns()

        return search_document_for_products(
            file_path=file_path,
            cell_iterator=self._document_parser.iter_document_cells(file_path),
            product_patterns=self._product_patterns,
            check_keywords_match_func=check_keywords_match,
            file_type="Word",
            log_interval=10.0,
            log_every_n=1000,
            stop_phrases=self.stop_phrases,
        )
    
    def search_additional_phrases_in_word(self, file_path: Path) -> List[Dict[str, Any]]:
        """Поиск дополнительных фраз в Word документах."""
        try:
            text = self._document_parser.extract_text(file_path)
            if not text:
                return []
            
            from services.document_search.additional_phrases import get_additional_search_phrases
            additional_phrases = get_additional_search_phrases()
            # Если есть пользовательские фразы из БД, объединяем их
            if self.user_search_phrases:
                all_phrases = list(set(additional_phrases + self.user_search_phrases))
            else:
                all_phrases = additional_phrases
            found_phrases = []
            
            text_lower = text.lower()
            for phrase in all_phrases:
                if phrase.lower() in text_lower:
                    found_phrases.append({
                        "phrase": phrase,
                        "is_additional_phrase": True,
                        "file_path": str(file_path),
                    })
            
            return found_phrases
        except Exception as error:
            logger.error(f"Ошибка при поиске дополнительных фраз в Word документе {file_path.name}: {error}")
            return []

    def _prepare_product_patterns(self) -> None:
        """Подготовка паттернов поиска для каждого товара."""
        self._product_patterns = {}
        for product_name in self.product_names:
            pattern = extract_keywords(product_name)
            if pattern:
                self._product_patterns[product_name] = pattern
