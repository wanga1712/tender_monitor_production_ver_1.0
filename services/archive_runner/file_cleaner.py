"""
MODULE: services.archive_runner.file_cleaner
RESPONSIBILITY: Cleanup temporary files (archives, processed Excel).
ALLOWED: psutil, pathlib, logging, time.
FORBIDDEN: Deleting critical data intentionally.
ERRORS: None.

Модуль для очистки файлов после обработки.

Удаляет:
- Архивы после распаковки
- Excel файлы после записи в БД
- При ошибке - файлы не удаляются
"""

import time
import os
from pathlib import Path
from typing import Sequence, Optional, List, Dict, Any
from loguru import logger

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("Библиотека psutil не установлена. Определение процессов, блокирующих файлы, будет недоступно.")


class FileCleaner:
    """Класс для очистки файлов после обработки."""
    
    ARCHIVE_EXTENSIONS = {'.rar', '.zip', '.7z'}
    EXCEL_EXTENSIONS = {'.xlsx', '.xls'}
    
    def cleanup_archives_after_extraction(
        self,
        archive_paths: Sequence[Path],
        success: bool = True,
    ) -> None:
        """
        Удаляет архивы после успешной распаковки.
        
        Args:
            archive_paths: Пути к архивным файлам
            success: True если распаковка успешна, False при ошибке
            
        Note:
            Метод не бросает исключения - ошибки удаления логируются.
        """
        if not success:
            logger.info("Ошибка при распаковке, архивы не удаляются")
            return
        
        for archive_path in archive_paths:
            if not archive_path.exists():
                continue
            
            suffix = archive_path.suffix.lower()
            if suffix not in self.ARCHIVE_EXTENSIONS:
                continue
            
            try:
                if self._remove_file_with_retry(archive_path):
                    logger.debug(f"Удален архив после распаковки: {archive_path.name}")
            except Exception as error:
                logger.warning(f"Не удалось удалить архив {archive_path.name}: {error}")
    
    def cleanup_excel_after_save(
        self,
        excel_paths: Sequence[Path],
        success: bool = True,
    ) -> None:
        """
        Удаляет Excel файлы после успешной записи в БД.
        
        Args:
            excel_paths: Пути к Excel файлам
            success: True если запись в БД успешна, False при ошибке
            
        Note:
            Метод не бросает исключения - ошибки удаления логируются.
        """
        if not success:
            logger.info("Ошибка при записи в БД, файлы не удаляются")
            return
        
        for excel_path in excel_paths:
            if not excel_path.exists():
                continue
            
            suffix = excel_path.suffix.lower()
            if suffix not in self.EXCEL_EXTENSIONS:
                continue
            
            try:
                if self._remove_file_with_retry(excel_path):
                    logger.debug(f"Удален Excel файл после записи в БД: {excel_path.name}")
            except Exception as error:
                logger.warning(f"Не удалось удалить Excel файл {excel_path.name}: {error}")
    
    def cleanup_all_files(
        self,
        archive_paths: Sequence[Path],
        excel_paths: Sequence[Path],
        extraction_success: bool = True,
        db_save_success: bool = True,
        failed_files: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Удаляет все файлы после обработки, кроме проблемных.
        
        Args:
            archive_paths: Пути к архивным файлам
            excel_paths: Пути к Excel файлам
            extraction_success: Успешность распаковки
            db_save_success: Успешность записи в БД
            failed_files: Список проблемных файлов, которые не нужно удалять
                Каждый элемент: {"path": str, "error": str, ...}
            
        Note:
            Метод не бросает исключения - ошибки удаления файлов логируются,
            но не прерывают выполнение программы. Удаление выполняется быстро,
            если файл заблокирован - пропускается без долгого ожидания.
            Проблемные файлы НЕ удаляются - они сохраняются для последующей обработки.
        """
        # Формируем множество путей проблемных файлов для исключения из удаления
        failed_paths = set()
        if failed_files:
            for failed_file in failed_files:
                try:
                    failed_paths.add(Path(failed_file["path"]).resolve())
                except Exception:
                    pass
        
        # Фильтруем архивы - исключаем проблемные файлы
        archives_to_clean = [p for p in archive_paths if Path(p).resolve() not in failed_paths]
        if failed_files and len(archives_to_clean) < len(archive_paths):
            logger.info(f"⚠️ Сохраняем {len(archive_paths) - len(archives_to_clean)} проблемных архивных файлов")
        
        try:
            self.cleanup_archives_after_extraction(archives_to_clean, extraction_success)
        except Exception as error:
            logger.warning(f"Ошибка при очистке архивов: {error}")
        
        # Фильтруем Excel файлы - исключаем проблемные файлы
        excel_to_clean = [p for p in excel_paths if Path(p).resolve() not in failed_paths]
        if failed_files and len(excel_to_clean) < len(excel_paths):
            logger.info(f"⚠️ Сохраняем {len(excel_paths) - len(excel_to_clean)} проблемных файлов для последующей обработки")
        
        try:
            self.cleanup_excel_after_save(excel_to_clean, db_save_success)
        except Exception as error:
            logger.warning(f"Ошибка при очистке Excel файлов: {error}")
    
    def _remove_file_with_retry(
        self, 
        path: Path, 
        max_retries: int = 2, 
        retry_delay: float = 1.0
    ) -> bool:
        """
        Удаляет файл с повторными попытками и таймаутом.
        
        Args:
            path: Путь к файлу для удаления
            max_retries: Максимальное количество попыток
            retry_delay: Задержка между попытками в секундах
        
        Returns:
            True если файл успешно удален, False если не удалось удалить
        """
        if not path.exists():
            return True
        
        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                # Пытаемся закрыть файл, если он открыт
                try:
                    if path.is_file():
                        # На Windows иногда помогает переименование перед удалением
                        temp_path = path.with_suffix(path.suffix + '.tmp')
                        if temp_path.exists():
                            temp_path.unlink()
                        path.rename(temp_path)
                        temp_path.unlink()
                    else:
                        path.unlink()
                except (OSError, PermissionError):
                    # Если переименование не помогло, пробуем обычное удаление
                    path.unlink()
                
                return True
                
            except (OSError, PermissionError) as error:
                last_error = error
                error_code = getattr(error, 'winerror', None) or getattr(error, 'errno', None)
                
                # WinError 32 = файл занят другим процессом
                # errno 13 = Permission denied
                if error_code in (32, 13) and attempt < max_retries:
                    logger.debug(
                        f"Файл {path.name} занят другим процессом. "
                        f"Попытка {attempt}/{max_retries}, повтор через {retry_delay} сек..."
                    )
                    time.sleep(retry_delay)
                    continue
                else:
                    break
            except Exception as error:
                last_error = error
                break
        
        # Если все попытки не удались, пытаемся найти и завершить процесс, держащий файл
        if last_error and error_code in (32, 13):
            logger.warning(
                f"Файл {path.name} заблокирован другим процессом. "
                f"Попытка найти и завершить процесс..."
            )
            from services.document_search.file_lock_handler import try_kill_process_holding_file
            if try_kill_process_holding_file(path):
                # Если процесс завершен, пробуем удалить файл еще раз
                logger.info(f"Процесс завершен. Повторная попытка удаления файла {path.name}...")
                try:
                    time.sleep(1.0)  # Даем время процессу освободить файл
                    path.unlink()
                    logger.info(f"Файл {path.name} успешно удален после завершения процесса.")
                    return True
                except Exception as final_error:
                    logger.warning(
                        f"Не удалось удалить файл {path.name} даже после завершения процесса: {final_error}"
                    )
            else:
                logger.warning(
                    f"Не удалось найти или завершить процесс, держащий файл {path.name}"
                )
        
        # Если всё равно не удалось, логируем предупреждение и пропускаем файл
        # Не бросаем исключение - просто возвращаем False
        logger.warning(
            f"Не удалось удалить файл {path.name} после всех попыток: {last_error}. "
            f"Файл будет пропущен и удален позже."
        )
        return False
    
    def _try_kill_process_holding_file(self, file_path: Path) -> bool:
        """
        Пытается найти и завершить процесс, который держит файл.
        
        Args:
            file_path: Путь к заблокированному файлу
            
        Returns:
            True если процесс найден и завершен, False в противном случае
        """
        if not PSUTIL_AVAILABLE:
            logger.debug("psutil недоступен, невозможно определить процесс, держащий файл")
            return False
        
        try:
            file_path_abs = file_path.resolve()
            file_path_str = str(file_path_abs).lower()
            
            # Список процессов, которые могут держать файл
            processes_to_kill: List[psutil.Process] = []
            
            # Используем более безопасный способ итерации по процессам
            # с защитой от Access Violation
            try:
                process_list = list(psutil.process_iter(['pid', 'name']))
            except Exception as e:
                logger.warning(f"Не удалось получить список процессов: {e}")
                return False
            
            for proc_info in process_list:
                try:
                    # Получаем процесс по PID для безопасности
                    proc = psutil.Process(proc_info.info['pid'])
                    
                    # Проверяем, что процесс еще существует
                    if not proc.is_running():
                        continue
                    
                    # Получаем открытые файлы процесса с защитой
                    try:
                        open_files = proc.open_files()
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        continue
                    except Exception as e:
                        # Игнорируем ошибки доступа к процессу
                        logger.debug(f"Ошибка при получении открытых файлов процесса PID={proc_info.info['pid']}: {e}")
                        continue
                    
                    for file_info in open_files:
                        try:
                            file_path_to_check = str(Path(file_info.path).resolve()).lower()
                            if file_path_to_check == file_path_str:
                                processes_to_kill.append(proc)
                                logger.info(
                                    f"Найден процесс, держащий файл {file_path.name}: "
                                    f"PID={proc.pid}, Name={proc.name()}"
                                )
                                break  # Найден процесс, переходим к следующему
                        except (OSError, ValueError, AttributeError):
                            # Не удалось разрешить путь, пропускаем
                            continue
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    # Процесс уже завершился или нет доступа
                    continue
                except Exception as error:
                    # Защита от любых других ошибок, включая Access Violation
                    logger.debug(f"Ошибка при проверке процесса: {error}")
                    continue
            
            if not processes_to_kill:
                logger.debug(f"Не найдено процессов, держащих файл {file_path.name}")
                return False
            
            # Завершаем найденные процессы с защитой от Access Violation
            killed_count = 0
            for proc in processes_to_kill:
                try:
                    # Проверяем, что процесс еще существует
                    if not proc.is_running():
                        killed_count += 1
                        continue
                    
                    try:
                        proc_name = proc.name()
                    except Exception:
                        proc_name = "unknown"
                    
                    proc_pid = proc.pid
                    
                    # Безопасные процессы для завершения (Excel, Word и т.д.)
                    safe_to_kill = [
                        'excel.exe', 'winword.exe', 'powerpnt.exe',
                        'notepad.exe', 'notepad++.exe', 'code.exe',
                        'devenv.exe', 'pycharm64.exe', 'idea64.exe'
                    ]
                    
                    # Если это текущий процесс Python, не завершаем его
                    current_pid = os.getpid()
                    if proc_pid == current_pid:
                        logger.info(
                            f"Файл {file_path.name} удерживается текущим процессом (PID={proc_pid}). "
                            f"Файл будет удален позже после освобождения ресурсов."
                        )
                        continue
                    
                    if proc_name.lower() not in safe_to_kill:
                        logger.warning(
                            f"Процесс {proc_name} (PID={proc_pid}) не в списке безопасных для завершения. "
                            f"Пропускаем."
                        )
                        continue
                    
                    logger.info(f"Завершаю процесс {proc_name} (PID={proc_pid})...")
                    try:
                        proc.terminate()  # Сначала мягкое завершение
                        try:
                            proc.wait(timeout=3)  # Ждем завершения до 3 секунд
                            logger.info(f"Процесс {proc_name} (PID={proc_pid}) успешно завершен")
                            killed_count += 1
                        except psutil.TimeoutExpired:
                            # Если не завершился мягко, убиваем жестко
                            logger.warning(
                                f"Процесс {proc_name} (PID={proc_pid}) не завершился мягко. "
                                f"Принудительное завершение..."
                            )
                            try:
                                proc.kill()
                                proc.wait(timeout=2)
                                logger.info(f"Процесс {proc_name} (PID={proc_pid}) принудительно завершен")
                                killed_count += 1
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                # Процесс уже завершился или нет доступа
                                killed_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        # Процесс уже завершился или нет доступа
                        killed_count += 1
                except Exception as error:
                    # Защита от любых ошибок, включая Access Violation
                    logger.warning(f"Ошибка при завершении процесса: {error}")
                    # Продолжаем обработку следующих процессов
                    continue
            
            return killed_count > 0
            
        except Exception as error:
            logger.error(f"Ошибка при поиске процесса, держащего файл {file_path.name}: {error}")
            return False

