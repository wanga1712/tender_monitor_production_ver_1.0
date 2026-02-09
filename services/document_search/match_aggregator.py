"""
MODULE: services.document_search.match_aggregator
RESPONSIBILITY: Aggregate match results from multiple sources/files.
ALLOWED: MatchFinder, logging.
FORBIDDEN: Direct parsing logic (delegate to MatchFinder).
ERRORS: None.

Модуль для агрегации совпадений из Excel файлов.
"""

import gc
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from services.document_search.match_finder import MatchFinder


class MatchAggregator:
    """Класс для агрегации совпадений из Excel файлов"""
    
    def __init__(
        self,
        progress_callback: Optional[callable] = None,
    ):
        """
        Инициализация агрегатора
        
        Args:
            progress_callback: Функция для обновления прогресса
        """
        self.progress_callback = progress_callback
    
    def _update_progress(self, stage: str, progress: int, detail: Optional[str] = None):
        """Обновление прогресса через callback"""
        if self.progress_callback:
            try:
                self.progress_callback(stage, progress, detail)
            except Exception as error:
                logger.debug(f"Ошибка при обновлении прогресса: {error}")
    
    def aggregate_matches_for_workbooks(
        self,
        workbook_paths: List[Path],
        product_names: List[str],
        stop_phrases: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Выполняет поиск по всем Excel и объединяет совпадения."""
        finder = MatchFinder(product_names, stop_phrases=stop_phrases)
        best_matches: Dict[str, Dict[str, Any]] = {}
        total_files = len(workbook_paths)
        
        for idx, workbook_path in enumerate(workbook_paths):
            progress = 60 + int(((idx + 1) / total_files) * 35) if total_files > 0 else 60
            file_name = workbook_path.name
            self._update_progress(
                "Сверка с данными из БД",
                progress,
                f"Обработка файла {idx + 1}/{total_files}: {file_name}"
            )
            
            logger.info(f"Поиск по документу: {workbook_path}")
            
            # Поиск по товарам из БД и дополнительным фразам одновременно
            matches = finder.search_workbook_for_products(workbook_path)
            additional_matches = finder.search_additional_phrases(workbook_path)
            
            # Объединяем все совпадения в один список
            all_matches = matches + additional_matches
            
            for match in all_matches:
                # Фильтруем только совпадения с оценкой >= 85 (100% и 85%)
                if match.get("score", 0) < 85.0:
                    continue
                product_name = match["product_name"]
                existing = best_matches.get(product_name)
                # Если уже есть совпадение с таким же названием, оставляем лучшее
                if existing and existing.get("score", 0) >= match.get("score", 0):
                    continue
                best_matches[product_name] = {
                    **match,
                    "source_file": str(workbook_path),
                }
            
            # Освобождаем память после обработки каждого файла
            gc.collect()

        self._update_progress("Сверка с данными из БД", 95, f"Обработка завершена, найдено совпадений: {len(best_matches)}")
        
        sorted_matches = sorted(best_matches.values(), key=lambda item: item["score"], reverse=True)
        return sorted_matches[:50]

