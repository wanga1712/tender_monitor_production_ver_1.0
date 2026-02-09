"""
Сервис для агрегации и сортировки совпадений товаров.

Отделяет агрегацию совпадений от основной логики поиска.
"""

from typing import List, Dict, Any
from loguru import logger


class MatchAggregatorService:
    """Сервис агрегации совпадений товаров."""

    def __init__(self):
        """Инициализация сервиса агрегации."""
        pass

    def aggregate_matches(self, all_matches: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Агрегирует совпадения из всех файлов, выбирая лучшие результаты.
        
        Args:
            all_matches: Список списков совпадений из каждого файла
        
        Returns:
            Отсортированный список лучших совпадений
        """
        best_matches: Dict[str, Dict[str, Any]] = {}
        
        for file_matches in all_matches:
            for match in file_matches:
                product_name = match["product_name"]
                existing = best_matches.get(product_name)
                
                # Сохраняем лучшее совпадение для каждого товара
                if existing and existing["score"] >= match["score"]:
                    continue
                    
                best_matches[product_name] = match
        
        # Сортируем по убыванию score и ограничиваем топ-50
        sorted_matches = sorted(
            best_matches.values(), 
            key=lambda item: item["score"], 
            reverse=True
        )
        
        logger.info(f"Агрегировано {len(sorted_matches)} уникальных совпадений")
        return sorted_matches[:50]

    def merge_matches_with_source(self, matches: List[Dict[str, Any]], 
                                 source_file: str) -> List[Dict[str, Any]]:
        """
        Добавляет информацию об исходном файле к совпадениям.
        
        Args:
            matches: Список совпадений
            source_file: Путь к исходному файлу
        
        Returns:
            Совпадения с добавленной информацией об источнике
        """
        return [
            {
                **match,
                "source_file": source_file,
            }
            for match in matches
        ]