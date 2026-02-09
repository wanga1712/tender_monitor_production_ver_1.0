"""
Сервис для обработки рабочих книг (Excel файлов и архивов).

Отвечает за:
- Определение путей к Excel файлам
- Распаковку архивов
- Обработку многочастных архивов
- Фильтрацию поддерживаемых форматов
"""

from pathlib import Path
from typing import List, Dict
from loguru import logger

from .archive_extractor import ArchiveExtractor


class WorkbookProcessor:
    """Сервис обработки рабочих книг и архивов."""

    def __init__(self, extractor: ArchiveExtractor):
        """
        Инициализация сервиса обработки рабочих книг.
        
        Args:
            extractor: Экстрактор архивов
        """
        self.extractor = extractor

    def prepare_workbook_paths(self, downloaded_paths: List[Path]) -> List[Path]:
        """
        Определение путей ко всем Excel файлам (напрямую или после распаковки архива).
        
        Args:
            downloaded_paths: Список путей к скачанным файлам
        
        Returns:
            Список путей к Excel файлам для обработки
        """
        if not downloaded_paths:
            from core.exceptions import DocumentSearchError
            raise DocumentSearchError("Не удалось скачать документ.")

        all_workbook_paths: List[Path] = []
        archive_paths: List[Path] = []
        excel_paths: List[Path] = []
        
        # Разделяем файлы по типам
        for path in downloaded_paths:
            suffix = path.suffix.lower()
            if suffix in {".rar", ".zip", ".7z"}:
                archive_paths.append(path)
            elif suffix in {".xlsx", ".xls"}:
                if self.extractor.is_file_archive(path):
                    logger.warning(f"Файл {path.name} имеет расширение Excel, но является архивом. Пропускаем.")
                    continue
                excel_paths.append(path)
            else:
                logger.warning(f"Неподдерживаемый формат файла: {path.name} (расширение: {suffix})")
        
        # Обрабатываем архивы
        if archive_paths:
            from .document_selector import DocumentSelector
            selector = DocumentSelector()
            
            # Сортируем архивы для правильной обработки многочастных
            sorted_archive_paths = sorted(
                archive_paths, 
                key=lambda p: (
                    selector.split_archive_name(p.name)[0] or p.stem.casefold(), 
                    selector.split_archive_name(p.name)[1] or 0
                )
            )
            
            # Группируем архивы по базовым именам
            archive_groups: Dict[str, List[Path]] = {}
            for path in sorted_archive_paths:
                base_name, _ = selector.split_archive_name(path.name)
                group_key = base_name or path.stem.casefold()
                if group_key not in archive_groups:
                    archive_groups[group_key] = []
                archive_groups[group_key].append(path)
            
            # Обрабатываем каждую группу архивов
            for group_paths in archive_groups.values():
                extracted_paths = self._process_archive_group(group_paths)
                all_workbook_paths.extend(extracted_paths)
        
        # Добавляем прямые Excel файлы
        all_workbook_paths.extend(excel_paths)
        
        if not all_workbook_paths:
            from core.exceptions import DocumentSearchError
            raise DocumentSearchError("Не найдено ни одного Excel файла для обработки.")
        
        logger.info(f"Подготовлено {len(all_workbook_paths)} файлов для парсинга")
        return all_workbook_paths

    def _process_archive_group(self, group_paths: List[Path]) -> List[Path]:
        """
        Обрабатывает группу архивов (многочастные или одиночные).
        
        Args:
            group_paths: Список путей к архивам одной группы
        
        Returns:
            Список путей к извлеченным файлам
        """
        if not group_paths:
            return []
            
        first_path = group_paths[0]
        suffix = first_path.suffix.lower()
        
        if suffix == ".rar":
            # Для RAR архивов обрабатываем первую часть
            archive_path = first_path
            if len(group_paths) > 1:
                logger.info(
                    "Обнаружен многочастный RAR (%s частей). Распаковываю, начиная с %s",
                    len(group_paths),
                    first_path.name,
                )
        else:
            # Для других форматов объединяем многочастные архивы
            archive_path = (
                self.extractor.combine_multi_part_archive(group_paths)
                if len(group_paths) > 1
                else first_path
            )
        
        # Извлекаем файлы из архива
        return self.extractor.extract_archive(archive_path)