"""
MODULE: services.archive_runner.tender_queue_manager
RESPONSIBILITY: Manage the queue of tenders to be processed, prioritizing by size.
ALLOWED: TenderFolderManager, logging.
FORBIDDEN: DB access, complex IO.
ERRORS: None.

Модуль для управления очередью обработки закупок с динамической приоритизацией по размеру папок.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from loguru import logger

from services.archive_runner.tender_folder_manager import TenderFolderManager


class TenderQueueManager:
    """
    Управляет очередью обработки закупок с динамической приоритизацией.
    
    Логика работы:
    1. Первая закупка проверяется один раз
    2. Если размер > 50 МБ → скачиваем следующую, эту ставим в конец
    3. Если следующая < 50 МБ → обрабатываем её
    4. Если следующая > 50 МБ → ставим её в конец, берём самую маленькую
    5. После обработки проверяем следующую папку и сравниваем с последней в очереди
    """
    
    SIZE_THRESHOLD_MB = 20  # Порог размера папки в МБ
    SIZE_THRESHOLD_BYTES = SIZE_THRESHOLD_MB * 1024 * 1024  # В байтах
    
    def __init__(self, folder_manager: TenderFolderManager, tender_type: str = 'new'):
        self.folder_manager = folder_manager
        self.tender_type = tender_type
        
        # Очередь: список кортежей (tender, folder_size, checked)
        # checked = True если размер уже проверен
        self._queue: List[Tuple[Dict[str, Any], int, bool]] = []
        self._current_index = 0
        self._first_checked = False  # Флаг проверки первой закупки
    
    def add_tenders(self, tenders: List[Dict[str, Any]]) -> None:
        """Добавляет закупки в очередь (без проверки размеров)"""
        for tender in tenders:
            self._queue.append((tender, 0, False))  # Размер будет определен позже
    
    def get_next_tender(self) -> Optional[Tuple[Dict[str, Any], int]]:
        """
        Получает следующую закупку для обработки с учетом приоритизации.
        
        Returns:
            Кортеж (tender, folder_size) или None если очередь пуста
        """
        if not self._queue:
            return None
        
        # Если это первая закупка - проверяем её размер один раз
        if not self._first_checked and self._current_index == 0:
            result = self._check_and_get_first()
            if result is None:
                return None
            return result
        
        # Если текущая закупка уже обработана, переходим к следующей
        if self._current_index >= len(self._queue):
            return None
        
        # Получаем текущую закупку
        current_tender, current_size, current_checked = self._queue[self._current_index]
        
        # Если размер не проверен - проверяем
        if not current_checked:
            current_size = self._get_folder_size(current_tender)
            self._queue[self._current_index] = (current_tender, current_size, True)
        
        # Если размер > 50 МБ - ищем следующую закупку
        if current_size > self.SIZE_THRESHOLD_BYTES:
            return self._handle_large_tender()
        
        # Размер <= 50 МБ - обрабатываем эту закупку
        return current_tender, current_size
    
    def _check_and_get_first(self) -> Optional[Tuple[Dict[str, Any], int]]:
        """Проверяет размер первой закупки"""
        if not self._queue:
            return None
        
        first_tender, _, _ = self._queue[0]
        first_size = self._get_folder_size(first_tender)
        self._queue[0] = (first_tender, first_size, True)
        self._first_checked = True
        
        # Если первая закупка > 50 МБ - скачиваем следующую и ставим первую в конец
        if first_size > self.SIZE_THRESHOLD_BYTES:
            logger.info(
                f"Первая закупка {first_tender.get('id')} имеет размер "
                f"{first_size / (1024 * 1024):.2f} МБ > {self.SIZE_THRESHOLD_MB} МБ, "
                f"ставим в конец очереди"
            )
            # Перемещаем в конец
            self._queue.append(self._queue.pop(0))
            # Проверяем следующую (которая теперь первая)
            result = self._handle_large_tender()
            if result is None:
                # Если ничего не найдено, возвращаем первую (даже если она большая)
                return first_tender, first_size
            return result
        
        return first_tender, first_size
    
    def _handle_large_tender(self) -> Optional[Tuple[Dict[str, Any], int]]:
        """
        Обрабатывает случай, когда текущая закупка > 50 МБ.
        Ищет следующую закупку с размером <= 50 МБ или самую маленькую.
        """
        # Проверяем следующие закупки в очереди
        checked_count = 0
        smallest_index = None
        smallest_size = float('inf')
        
        # Проверяем следующие закупки (начиная с current_index + 1)
        for idx in range(self._current_index + 1, len(self._queue)):
            tender, size, checked = self._queue[idx]
            
            # Если размер не проверен - проверяем
            if not checked:
                size = self._get_folder_size(tender)
                self._queue[idx] = (tender, size, True)
                checked_count += 1
            
            # Если нашли закупку <= 50 МБ - берём её
            if size <= self.SIZE_THRESHOLD_BYTES:
                logger.info(
                    f"Найдена закупка {tender.get('id')} с размером "
                    f"{size / (1024 * 1024):.2f} МБ <= {self.SIZE_THRESHOLD_MB} МБ, "
                    f"начинаем обработку"
                )
                # Перемещаем на текущую позицию
                self._queue[self._current_index], self._queue[idx] = self._queue[idx], self._queue[self._current_index]
                return tender, size
            
            # Запоминаем самую маленькую
            if size < smallest_size:
                smallest_size = size
                smallest_index = idx
        
        # Если не нашли закупку <= 50 МБ, берём самую маленькую
        if smallest_index is not None:
            tender, size, _ = self._queue[smallest_index]
            logger.info(
                f"Все проверенные закупки > {self.SIZE_THRESHOLD_MB} МБ, "
                f"берём самую маленькую {tender.get('id')} "
                f"({size / (1024 * 1024):.2f} МБ)"
            )
            # Перемещаем на текущую позицию
            self._queue[self._current_index], self._queue[smallest_index] = self._queue[smallest_index], self._queue[self._current_index]
            return tender, size
        
        # Если ничего не нашли, возвращаем текущую (даже если она большая)
        if self._current_index < len(self._queue):
            tender, size, _ = self._queue[self._current_index]
            logger.warning(
                f"Не найдено подходящих закупок, обрабатываем текущую "
                f"{tender.get('id')} ({size / (1024 * 1024):.2f} МБ)"
            )
            return tender, size
        
        return None
    
    def mark_processed(self) -> None:
        """Отмечает текущую закупку как обработанную и переходит к следующей"""
        if self._current_index < len(self._queue):
            self._current_index += 1
            
            # После обработки проверяем следующую папку и сравниваем с последней в очереди
            if self._current_index < len(self._queue):
                self._reorder_after_processing()
    
    def mark_tender_as_failed(self, tender_id: int, error_message: str) -> None:
        """Помечает закупку как неуспешно обработанную и переходит к следующей при необходимости"""
        failed_index: Optional[int] = None
        
        for index, (tender, size, checked) in enumerate(self._queue):
            if tender.get("id") == tender_id:
                tender["is_failed"] = True
                tender["error_message"] = error_message
                self._queue[index] = (tender, size, checked)
                failed_index = index
                logger.warning(f"Закупка {tender_id} помечена как неуспешная: {error_message}")
                break
        
        if failed_index is None:
            logger.warning(f"Попытка пометить неизвестную закупку {tender_id} как неуспешную")
            return
        
        if failed_index == self._current_index:
            self.mark_processed()
    
    def _reorder_after_processing(self) -> None:
        """
        После обработки проверяет следующую папку и сравнивает с последней в очереди.
        Если следующая больше последней - передвигает её в конец.
        """
        if len(self._queue) < 2:
            return
        
        # Проверяем следующую закупку (current_index)
        next_tender, next_size, next_checked = self._queue[self._current_index]
        if not next_checked:
            next_size = self._get_folder_size(next_tender)
            self._queue[self._current_index] = (next_tender, next_size, True)
        
        # Получаем последнюю закупку в очереди
        last_tender, last_size, last_checked = self._queue[-1]
        if not last_checked:
            last_size = self._get_folder_size(last_tender)
            self._queue[-1] = (last_tender, last_size, True)
        
        # Если следующая больше последней - передвигаем её в конец
        if next_size > last_size:
            logger.info(
                f"Следующая закупка {next_tender.get('id')} "
                f"({next_size / (1024 * 1024):.2f} МБ) больше последней "
                f"{last_tender.get('id')} ({last_size / (1024 * 1024):.2f} МБ), "
                f"передвигаем в конец"
            )
            # Перемещаем следующую в конец
            self._queue.append(self._queue.pop(self._current_index))
    
    def _get_folder_size(self, tender: Dict[str, Any]) -> int:
        """Получает размер папки закупки"""
        tender_id = tender.get("id")
        registry_type = tender.get("registry_type", "44fz")
        folder_path = self.folder_manager.prepare_tender_folder(tender_id, registry_type, self.tender_type)
        return self.folder_manager.get_folder_size(folder_path)
    
    def has_more(self) -> bool:
        """Проверяет, есть ли ещё закупки для обработки"""
        return self._current_index < len(self._queue)
    
    def get_queue_info(self) -> Dict[str, Any]:
        """Возвращает информацию об очереди для логирования"""
        total = len(self._queue)
        processed = self._current_index
        remaining = total - processed
        
        checked_count = sum(1 for _, _, checked in self._queue if checked)
        large_count = sum(1 for _, size, checked in self._queue if checked and size > self.SIZE_THRESHOLD_BYTES)
        small_count = sum(1 for _, size, checked in self._queue if checked and size <= self.SIZE_THRESHOLD_BYTES)
        
        return {
            "total": total,
            "processed": processed,
            "remaining": remaining,
            "checked": checked_count,
            "large": large_count,
            "small": small_count,
        }

