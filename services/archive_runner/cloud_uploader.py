"""
MODULE: services.archive_runner.cloud_uploader
RESPONSIBILITY:
- Управление загрузкой файлов в облачные хранилища
- Интеграция с Яндекс.Диском и другими облачными сервисами
- Обработка ошибок облачных операций
ALLOWED:
- Яндекс.Диск API и другие облачные клиенты
- Операции с файловой системой для временных файлов
- Логирование через loguru
FORBIDDEN:
- Прямое взаимодействие с базой данных
- Бизнес-логика обработки торгов
- Управление файловыми операциями вне облачного контекста
ERRORS:
- Должен пробрасывать CloudUploadError, NetworkError
"""

from pathlib import Path
from typing import Optional

from loguru import logger
# from services.storage.yandex_disk import YandexDiskClient  # Временное отключение


class CloudUploader:
    """Менеджер загрузки файлов в облачные хранилища (Яндекс Диск временно отключен)"""

    def __init__(self, yandex_disk_client: Optional[object] = None):
        self.yandex_disk = yandex_disk_client

    def upload_folder_to_yandex_disk(self, folder_path: Path, 
                                   tender_id: int, 
                                   registry_type: str) -> bool:
        """Загрузить папку на Яндекс.Диск"""
        if not self.yandex_disk:
            logger.warning("Яндекс.Диск не настроен, пропускаем загрузку")
            return False

        if not folder_path.exists():
            logger.warning(f"Папка для загрузки не существует: {folder_path}")
            return False

        try:
            # Создаем путь на Яндекс.Диске
            remote_path = f"/tenders/{registry_type}/{tender_id}"
            
            # Загружаем папку
            success = self.yandex_disk.upload_folder(folder_path, remote_path)
            
            if success:
                logger.info(f"Папка успешно загружена на Яндекс.Диск: {folder_path} -> {remote_path}")
            else:
                logger.warning(f"Не удалось загрузить папку на Яндекс.Диск: {folder_path}")
            
            return success
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке на Яндекс.Диск: {e}")
            return False

    def check_yandex_disk_connection(self) -> bool:
        """Проверить подключение к Яндекс.Диску"""
        if not self.yandex_disk:
            return False
            
        try:
            return self.yandex_disk.check_connection()
        except Exception as e:
            logger.error(f"Ошибка проверки подключения Яндекс.Диска: {e}")
            return False

    def create_yandex_disk_folder(self, path: str) -> bool:
        """Создать папку на Яндекс.Диске"""
        if not self.yandex_disk:
            return False
            
        try:
            return self.yandex_disk.create_folder(path)
        except Exception as e:
            logger.error(f"Ошибка создания папки на Яндекс.Диске: {e}")
            return False