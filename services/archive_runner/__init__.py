"""
MODULE: services.archive_runner
RESPONSIBILITY: Package initialization for archive runner service.
ALLOWED: services.archive_runner.runner.
FORBIDDEN: None.
ERRORS: None.

Модуль для автоматической обработки документов торгов.

Разделен на подмодули:
- file_cleaner: Очистка файлов после обработки
- existing_files_processor: Обработка существующих файлов
- tender_provider: Получение торгов из БД
- download_manager: Скачивание документов
- error_handler: Централизованная обработка ошибок
- folder_processor: Операции с файловой системой
- cloud_uploader: Загрузка в облачные хранилища
- tender_coordinator: Координация обработки торгов
"""

from services.archive_runner.runner import ArchiveBackgroundRunner
from services.archive_runner.error_handler import ErrorHandler
from services.archive_runner.folder_processor import FolderProcessor
from services.archive_runner.cloud_uploader import CloudUploader
from services.archive_runner.tender_coordinator import TenderCoordinator

__all__ = [
    'ArchiveBackgroundRunner',
    'ErrorHandler',
    'FolderProcessor',
    'CloudUploader',
    'TenderCoordinator'
]

