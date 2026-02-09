"""
MODULE: services.document_search.download_timeout_calculator
RESPONSIBILITY: Calculate download timeouts based on file statistics.
ALLOWED: DatabaseManager (optional), logging.
FORBIDDEN: Network access.
ERRORS: None.

Модуль для расчета адаптивного таймаута скачивания на основе статистики БД.
"""

from __future__ import annotations

from typing import Optional, Tuple, Callable
from pathlib import Path
from loguru import logger


class DownloadTimeoutCalculator:
    """
    Калькулятор адаптивного таймаута для скачивания файлов.
    Использует статистику из БД для расчета оптимального таймаута.
    """

    # Коэффициенты запаса для разных типов файлов
    FILE_TYPE_MULTIPLIERS = {
        '.pdf': 5.0,  # PDF может быть отсканированным, очень большим
        '.jpg': 4.0,  # Изображения могут быть большими
        '.jpeg': 4.0,
        '.png': 4.0,
        '.tiff': 6.0,  # TIFF часто используется для сканов
        '.tif': 6.0,
        '.rar': 3.0,  # Архивы
        '.zip': 3.0,
        '.7z': 3.0,
        '.xlsx': 2.5,  # Excel файлы
        '.xls': 2.5,
        '.docx': 2.5,  # Word документы
        '.doc': 2.5,
    }

    DEFAULT_MULTIPLIER = 3.0  # Для неизвестных типов файлов
    MIN_TIMEOUT = 120  # Минимум 2 минуты
    MAX_TIMEOUT = 10800  # Максимум 3 часа для очень больших файлов
    DEFAULT_TIMEOUT = 3600  # 1 час по умолчанию

    def __init__(self, db_manager=None):
        """
        Args:
            db_manager: Менеджер БД для получения статистики (опционально)
        """
        self.db_manager = db_manager

    def calculate_timeout(
        self,
        file_size_bytes: Optional[int] = None,
        file_name: Optional[str] = None,
    ) -> None:
        """
        Вычисляет таймаут на основе размера файла и статистики БД.
        Таймауты полностью убраны - файлы могут скачиваться сколь угодно долго.

        Args:
            file_size_bytes: Размер файла в байтах
            file_name: Имя файла (для определения типа)

        Returns:
            None - без таймаутов вообще
        """
        return None

    def _get_file_type_multiplier(self, file_name: Optional[str]) -> float:
        """Получает коэффициент запаса для типа файла."""
        if not file_name:
            return self.DEFAULT_MULTIPLIER

        file_ext = Path(file_name).suffix.lower()
        return self.FILE_TYPE_MULTIPLIERS.get(file_ext, self.DEFAULT_MULTIPLIER)

    def _get_average_download_speed(
        self,
        file_size_bytes: int,
        size_tolerance: float = 0.5,
    ) -> Optional[float]:
        """
        Получает среднюю скорость скачивания из БД для файлов похожего размера.

        Args:
            file_size_bytes: Размер файла в байтах
            size_tolerance: Допустимое отклонение размера (0.5 = ±50%)

        Returns:
            Средняя скорость в байтах/сек или None
        """
        if not self.db_manager:
            return None

        try:
            # Ищем записи с похожим размером файлов
            # Используем total_size_bytes и processing_time_seconds из tender_document_matches
            min_size = int(file_size_bytes * (1 - size_tolerance))
            max_size = int(file_size_bytes * (1 + size_tolerance))

            query = """
                SELECT 
                    AVG(total_size_bytes::numeric / NULLIF(processing_time_seconds, 0)) as avg_speed_bytes_per_sec,
                    COUNT(*) as sample_count
                FROM tender_document_matches
                WHERE total_size_bytes BETWEEN %s AND %s
                    AND processing_time_seconds > 0
                    AND total_size_bytes > 0
                    AND processed_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
                HAVING COUNT(*) >= 5
            """

            results = self.db_manager.execute_query(query, (min_size, max_size))
            if results and len(results) > 0:
                row = results[0]
                avg_speed = row.get('avg_speed_bytes_per_sec')
                sample_count = row.get('sample_count', 0)

                if avg_speed and sample_count >= 5:
                    return float(avg_speed)

        except Exception as e:
            logger.debug(f"Ошибка при получении статистики из БД: {e}")

        return None


def create_timeout_calculator(db_manager=None) -> Callable[[Optional[int], Optional[str]], None]:
    """
    Создает функцию для расчета таймаута, совместимую с DocumentDownloader.

    Args:
        db_manager: Менеджер БД для получения статистики (опционально)

    Returns:
        Функция (file_size_bytes, file_name) -> None (без таймаутов)
    """
    calculator = DownloadTimeoutCalculator(db_manager)

    def timeout_calculator(
        file_size_bytes: Optional[int] = None,
        file_name: Optional[str] = None,
    ) -> None:
        return calculator.calculate_timeout(file_size_bytes, file_name)

    return timeout_calculator
