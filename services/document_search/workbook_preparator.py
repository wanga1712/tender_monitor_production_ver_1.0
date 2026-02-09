"""
MODULE: services.document_search.workbook_preparator
RESPONSIBILITY: Prepare workbook paths, handling archives and multi-part files.
ALLOWED: document_selector, archive_extractor, core.exceptions, logging.
FORBIDDEN: Parsing cell data (only path preparation).
ERRORS: DocumentSearchError.

Модуль для подготовки путей к Excel файлам.
"""

from pathlib import Path
from typing import Dict, List

from loguru import logger

from core.exceptions import DocumentSearchError
from services.document_search.document_selector import DocumentSelector
from services.document_search.archive_extractor import ArchiveExtractor


class WorkbookPreparator:
    """Класс для подготовки путей к Excel файлам"""
    
    def __init__(
        self,
        selector: DocumentSelector,
        extractor: ArchiveExtractor,
    ):
        """
        Инициализация подготовителя
        
        Args:
            selector: Селектор документов
            extractor: Экстрактор архивов
        """
        self.selector = selector
        self.extractor = extractor
    
    def prepare_workbook_paths(self, downloaded_paths: List[Path]) -> List[Path]:
        """
        Определение путей ко всем Excel файлам (напрямую или после распаковки архива).
        """
        if not downloaded_paths:
            raise DocumentSearchError("Не удалось скачать документ.")

        all_workbook_paths: List[Path] = []
        archive_paths: List[Path] = []
        excel_paths: List[Path] = []
        
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
        
        if archive_paths:
            sorted_archive_paths = sorted(
                archive_paths,
                key=lambda p: (
                    self.selector.split_archive_name(p.name)[0] or p.stem.casefold(),
                    self.selector.split_archive_name(p.name)[1] or 0
                )
            )
            
            archive_groups: Dict[str, List[Path]] = {}
            for path in sorted_archive_paths:
                base_name, _ = self.selector.split_archive_name(path.name)
                group_key = base_name or path.stem.casefold()
                if group_key not in archive_groups:
                    archive_groups[group_key] = []
                archive_groups[group_key].append(path)
            
            for group_paths in archive_groups.values():
                first_path = group_paths[0]
                suffix = first_path.suffix.lower()
                
                # Проверяем, что все части архива существуют (для многотомных RAR)
                if suffix == ".rar" and len(group_paths) > 1:
                    # Проверяем наличие всех частей перед распаковкой
                    missing_parts = []
                    for part_path in group_paths:
                        if not part_path.exists():
                            missing_parts.append(part_path.name)
                    
                    if missing_parts:
                        logger.warning(
                            f"Не все части RAR архива скачаны. Отсутствуют: {', '.join(missing_parts)}. "
                            f"Пропускаем распаковку до скачивания всех частей."
                        )
                        continue
                    
                    archive_path = first_path
                    logger.info(
                        f"Обнаружен многочастный RAR ({len(group_paths)} частей). Все части скачаны. Распаковываю, начиная с {first_path.name}",
                    )
                elif suffix == ".rar":
                    archive_path = first_path
                else:
                    archive_path = (
                        self.extractor.combine_multi_part_archive(group_paths)
                        if len(group_paths) > 1
                        else first_path
                    )
                
                try:
                    extracted_paths = self.extractor.extract_archive(archive_path)
                    all_workbook_paths.extend(extracted_paths)
                except Exception as extract_error:
                    logger.error(f"Ошибка при распаковке архива {archive_path.name}: {extract_error}")
                    # Продолжаем обработку других архивов
                    continue
        
        all_workbook_paths.extend(excel_paths)
        
        if not all_workbook_paths:
            raise DocumentSearchError("Не найдено ни одного Excel файла для обработки.")
        
        logger.info(f"Подготовлено {len(all_workbook_paths)} файлов для парсинга")
        return all_workbook_paths

