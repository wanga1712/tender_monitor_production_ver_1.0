"""
Сервис обработки документов и архивов.
Отвечает за извлечение, парсинг и подготовку документов.
"""

from typing import List, Dict, Any
from pathlib import Path
import logging

from services.document_search.archive_extractor import ArchiveExtractor
from services.document_search.document_parser import DocumentParser
from services.document_search.excel_parser import ExcelParser

logger = logging.getLogger(__name__)


class DocumentProcessingService:
    """Сервис обработки документов и архивов."""
    
    def __init__(self, unrar_path: str, winrar_path: str = None):
        """
        Инициализация сервиса обработки.
        
        Args:
            unrar_path: Путь к утилите unrar
            winrar_path: Путь к WinRAR (опционально)
        """
        self.extractor = ArchiveExtractor(unrar_path=unrar_path, winrar_path=winrar_path)
        self.document_parser = DocumentParser()
        self.excel_parser = ExcelParser()
    
    def extract_archive(self, archive_path: Path, extract_dir: Path) -> List[Path]:
        """
        Извлечение архива в указанную директорию.
        
        Args:
            archive_path: Путь к архиву
            extract_dir: Директория для извлечения
            
        Returns:
            Список извлеченных файлов
        """
        try:
            return self.extractor.extract_archive(archive_path, extract_dir)
        except Exception as e:
            logger.error(f"Ошибка извлечения архива {archive_path}: {e}")
            raise
    
    def parse_document(self, file_path: Path, file_type: str = None) -> Dict[str, Any]:
        """
        Парсинг документа для извлечения содержимого.
        
        Args:
            file_path: Путь к файлу
            file_type: Тип файла (опционально)
            
        Returns:
            Результаты парсинга документа
        """
        try:
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                return self.excel_parser.parse_excel(file_path)
            else:
                return self.document_parser.parse_document(file_path, file_type)
        except Exception as e:
            logger.error(f"Ошибка парсинга документа {file_path}: {e}")
            raise
    
    def prepare_documents_for_analysis(self, extracted_files: List[Path]) -> List[Dict[str, Any]]:
        """
        Подготовка документов для анализа.
        
        Args:
            extracted_files: Список извлеченных файлов
            
        Returns:
            Подготовленные документы для анализа
        """
        prepared_docs = []
        for file_path in extracted_files:
            try:
                doc_info = self.parse_document(file_path)
                prepared_docs.append({
                    'path': file_path,
                    'info': doc_info,
                    'size': file_path.stat().st_size
                })
            except Exception as e:
                logger.warning(f"Не удалось подготовить файл {file_path}: {e}")
        
        return prepared_docs