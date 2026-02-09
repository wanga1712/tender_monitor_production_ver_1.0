"""
MODULE: services.archive_runner.tender_folder_manager
RESPONSIBILITY: Manage file system directories for tenders.
ALLOWED: pathlib, shutil, logging.
FORBIDDEN: Business logic regarding tender status (only FS ops).
ERRORS: None.

Модуль для управления папками тендеров.
"""

import os
import time
import shutil
from pathlib import Path
from typing import Optional
from loguru import logger


class TenderFolderManager:
    """Класс для управления папками тендеров"""
    
    def __init__(self, download_dir: Path):
        """
        Инициализация менеджера папок
        
        Args:
            download_dir: Базовая директория для скачивания документов
        """
        self.download_dir = download_dir
    
    def prepare_tender_folder(self, tender_id: int, registry_type: str, tender_type: str = 'new') -> Path:
        """
        Подготовка папки для тендера
        """
        # Проверяем доступность корневой директории
        if not self.download_dir.exists():
            try:
                self.download_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error(f"Не удалось получить доступ к корневой папке загрузки {self.download_dir}: {e}")
                raise

        folder_name = f"{registry_type}_{tender_id}"
        if tender_type == 'won':
            folder_name += "_won"
            
        target_dir = self.download_dir / folder_name
        
        if not target_dir.exists():
            logger.info(f"Создание папки: {target_dir}")
            try:
                target_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error(f"Ошибка создания папки {target_dir}: {e}")
                # Пробуем проверить, может это файл?
                if target_dir.is_file():
                    logger.error(f"Внимание! По пути {target_dir} находится ФАЙЛ, а не папка.")
                raise
        else:
            logger.info(f"Папка уже существует: {target_dir}")
            
        return target_dir
    
    def _migrate_folder_contents(self, source_folder: Path, target_folder: Path) -> int:
        """
        Переносит содержимое из одной папки в другую.
        
        Args:
            source_folder: Исходная папка
            target_folder: Целевая папка
        
        Returns:
            Количество перенесенных файлов/папок
        """
        moved_count = 0
        
        if not source_folder.exists() or not source_folder.is_dir():
            return moved_count
        
        try:
            for item in source_folder.iterdir():
                try:
                    target_item = target_folder / item.name
                    
                    # Если элемент уже существует в целевой папке, пропускаем
                    if target_item.exists():
                        logger.debug(f"Элемент {item.name} уже существует в целевой папке, пропускаем")
                        continue
                    
                    # Перемещаем файл или папку
                    if item.is_file():
                        shutil.move(str(item), str(target_item))
                        moved_count += 1
                    elif item.is_dir():
                        shutil.move(str(item), str(target_item))
                        moved_count += 1
                        
                except Exception as error:
                    logger.warning(f"Не удалось перенести {item.name} из {source_folder} в {target_folder}: {error}")
                    # Пробуем скопировать вместо перемещения
                    try:
                        target_item = target_folder / item.name
                        if item.is_file():
                            shutil.copy2(str(item), str(target_item))
                            moved_count += 1
                        elif item.is_dir():
                            shutil.copytree(str(item), str(target_item), dirs_exist_ok=True)
                            moved_count += 1
                    except Exception as copy_error:
                        logger.error(f"Не удалось скопировать {item.name}: {copy_error}")
                        
        except Exception as error:
            logger.error(f"Ошибка при переносе содержимого из {source_folder} в {target_folder}: {error}")
        
        return moved_count
    
    def clean_tender_folder(self, folder_path: Path) -> None:
        """
        Очистка папки тендера (обычная)
        
        Args:
            folder_path: Путь к папке тендера
        """
        if not folder_path.exists():
            return
        
        try:
            for item in folder_path.iterdir():
                if item.is_file():
                    self._remove_file(item)
                elif item.is_dir():
                    self._remove_directory_force(item)
        except Exception as e:
            logger.warning(f"Ошибка при очистке папки {folder_path}: {e}")
    
    def clean_tender_folder_force(self, folder_path: Path) -> None:
        """
        Принудительно очищает все файлы в папке тендера, убивая процессы, которые держат файлы.
        Используется при повторном запуске программы.
        
        Args:
            folder_path: Путь к папке тендера
        """
        if not folder_path.exists() or not folder_path.is_dir():
            return
        
        logger.info(f"Принудительная очистка папки тендера: {folder_path}")
        deleted_count = 0
        failed_items = []
        
        for item in folder_path.iterdir():
            try:
                if item.is_file():
                    self._remove_file_force(item)
                    deleted_count += 1
                elif item.is_dir():
                    # Рекурсивно удаляем содержимое подпапок
                    try:
                        shutil.rmtree(item, ignore_errors=True)
                        deleted_count += 1
                    except Exception:
                        # Если не удалось удалить папку, пробуем принудительно удалить файлы внутри
                        self._remove_directory_force(item)
                        try:
                            item.rmdir()
                            deleted_count += 1
                        except Exception:
                            failed_items.append(str(item))
            except Exception as error:
                logger.warning(f"Не удалось удалить {item}: {error}")
                failed_items.append(str(item))
        
        if deleted_count > 0:
            logger.info(f"Удалено файлов/папок из {folder_path}: {deleted_count}")
        if failed_items:
            logger.warning(f"Не удалось удалить {len(failed_items)} элементов: {failed_items[:5]}")
    
    def reset_tender_folder(self, folder_path: Path) -> None:
        """
        Сброс папки тендера (удаление и создание заново)
        
        Args:
            folder_path: Путь к папке тендера
        """
        if folder_path.exists():
            self._remove_directory_force(folder_path)
        folder_path.mkdir(parents=True, exist_ok=True)
    
    def _remove_directory_force(self, dir_path: Path) -> None:
        """
        Принудительно удаляет все файлы в директории.
        
        Args:
            dir_path: Путь к директории
        """
        try:
            for item in dir_path.rglob('*'):
                if item.is_file():
                    self._remove_file_force(item)
                elif item.is_dir():
                    try:
                        item.rmdir()
                    except Exception:
                        pass
        except Exception as error:
            logger.debug(f"Ошибка при принудительном удалении директории {dir_path}: {error}")
    
    def get_folder_size(self, folder_path: Path) -> int:
        """
        Подсчитывает размер папки в байтах (рекурсивно).
        
        Args:
            folder_path: Путь к папке тендера
            
        Returns:
            Размер папки в байтах, 0 если папка не существует
        """
        if not folder_path.exists() or not folder_path.is_dir():
            return 0
        
        total_size = 0
        try:
            for item in folder_path.rglob('*'):
                if item.is_file():
                    try:
                        total_size += item.stat().st_size
                    except (OSError, PermissionError):
                        # Игнорируем файлы, к которым нет доступа
                        pass
        except Exception as error:
            logger.debug(f"Ошибка при подсчете размера папки {folder_path}: {error}")
        
        return total_size
    
    def _remove_file_force(self, path: Path) -> None:
        """
        Принудительное удаление файла, убивая процессы, которые его держат (только на Windows).
        
        Args:
            path: Путь к файлу
        """
        if not path.exists():
            return
        
        import sys
        import subprocess
        
        # Пробуем обычное удаление
        try:
            path.unlink()
            return
        except (OSError, PermissionError) as error:
            error_code = getattr(error, 'winerror', None) or getattr(error, 'errno', None)
            
            # WinError 32 = файл занят другим процессом
            if sys.platform == 'win32' and error_code == 32:
                try:
                    # На Windows используем PowerShell для поиска и убийства процесса
                    ps_command = f'''
                    $file = "{path}"; 
                    $processes = Get-Process | Where-Object {{$_.Path -eq $file -or (Get-Process -Id $_.Id).Modules.FileName -like "*$file*"}};
                    if ($processes) {{ $processes | Stop-Process -Force }}
                    '''
                    
                    subprocess.run(
                        ['powershell', '-Command', ps_command],
                        capture_output=True,
                        timeout=5,
                        check=False,
                        encoding='utf-8',
                        errors='replace',  # Заменяем некорректные символы вместо ошибки
                    )
                    
                    # Ждем немного и пробуем снова
                    time.sleep(0.5)
                    
                    try:
                        path.unlink()
                        logger.debug(f"Файл {path.name} удален после завершения процесса")
                        return
                    except Exception:
                        pass
                except Exception as ps_error:
                    logger.debug(f"Не удалось завершить процесс через PowerShell: {ps_error}")
            
            # Если не помогло, пробуем переименовать и удалить позже
            try:
                temp_path = path.with_suffix(path.suffix + '.tmp_delete')
                if temp_path.exists():
                    temp_path.unlink()
                path.rename(temp_path)
                logger.debug(f"Файл {path.name} переименован для последующего удаления")
            except Exception:
                logger.warning(f"Не удалось принудительно удалить файл {path.name}")
    
    def _remove_file(self, path: Path, max_retries: int = 3, retry_delay: float = 2.0) -> None:
        """
        Удаление файла с повторными попытками
        
        Args:
            path: Путь к файлу
            max_retries: Максимальное количество попыток
            retry_delay: Задержка между попытками в секундах
        """
        if not path.exists():
            return
        
        for attempt in range(max_retries):
            try:
                os.chmod(path, 0o777)  # Устанавливаем права на запись
                path.unlink()
                return
            except (PermissionError, OSError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Попытка {attempt + 1}/{max_retries} удаления {path}: {e}")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Не удалось удалить файл {path} после {max_retries} попыток: {e}")
                    raise

