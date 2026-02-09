"""
MODULE: services.document_search.archive_extractor
RESPONSIBILITY: Extract archive files (zip, rar, 7z) into a directory.
ALLOWED: py7zr, rarfile, zipfile, shutil, subprocess, logging, services.document_search.document_selector.
FORBIDDEN: Network access, database access.
ERRORS: DocumentSearchError.

Модуль для извлечения архивов.

Класс ArchiveExtractor отвечает за:
- Распаковку ZIP, RAR, 7Z архивов
- Обработку многочастных архивов
- Поиск Excel файлов в распакованных архивах
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional
import os
import shutil
import subprocess
import uuid

import py7zr
import rarfile
import zipfile
from loguru import logger

from core.exceptions import DocumentSearchError
from services.document_search.document_selector import DocumentSelector
from services.error_logger import get_error_logger


class ArchiveExtractor:
    """Класс для извлечения архивов различных форматов."""

    def __init__(
        self,
        unrar_path: Optional[str] = None,
        winrar_path: Optional[str] = None,
    ):
        """
        Args:
            unrar_path: Путь к инструменту UnRAR
            winrar_path: Путь к директории WinRAR
        """
        self._rar_tool_configured = False
        effective_unrar = unrar_path or os.environ.get("UNRAR_TOOL")
        self._unrar_path = Path(effective_unrar) if effective_unrar else None

        effective_winrar = winrar_path or os.environ.get("WINRAR_PATH")
        self._winrar_path = Path(effective_winrar) if effective_winrar else None

        if self._winrar_path and self._winrar_path.exists():
            current_path = os.environ.get("PATH", "")
            path_parts = current_path.split(os.pathsep) if current_path else []
            if str(self._winrar_path) not in path_parts:
                os.environ["PATH"] = (
                    os.pathsep.join(path_parts + [str(self._winrar_path)])
                    if path_parts
                    else str(self._winrar_path)
                )

        self._active_extract_dirs: List[Path] = []
        self._selector = DocumentSelector()

    def extract_archive(
        self,
        archive_path: Path,
        target_dir: Optional[Path] = None,
    ) -> List[Path]:
        """Распаковка архива (ZIP, RAR, 7Z) и поиск всех XLSX внутри."""
        logger.info(f"Распаковка архива {archive_path.name}")
        if target_dir:
            extract_dir = Path(target_dir)
            extract_dir.mkdir(parents=True, exist_ok=True)
        else:
            extract_dir = archive_path.parent / f"extract_{archive_path.stem}_{uuid.uuid4().hex[:6]}"
            extract_dir.mkdir(parents=True, exist_ok=True)
        self._register_extract_dir(extract_dir)
        suffix = archive_path.suffix.lower()

        try:
            if suffix == ".zip":
                self._extract_zip_archive(archive_path, extract_dir)
            elif suffix == ".rar":
                self._extract_rar_archive(archive_path, extract_dir)
            elif suffix == ".7z":
                self._extract_7z_archive(archive_path, extract_dir)
            else:
                raise DocumentSearchError(f"Неподдерживаемый формат архива: {suffix}")
        except Exception as error:
            logger.error(f"Ошибка распаковки архива {archive_path.name}: {error}")
            get_error_logger().log_extraction_error(
                archive_path=archive_path,
                error_message=str(error),
                archive_type=suffix,
            )
            raise DocumentSearchError(f"Ошибка распаковки архива: {error}") from error

        # Ищем все поддерживаемые типы документов: Excel, Word, PDF
        xlsx_files = [
            path for path in extract_dir.rglob("*.xlsx")
            if not path.name.startswith("~$")
        ]
        xls_files = [
            path for path in extract_dir.rglob("*.xls")
            if not path.name.startswith("~$")
        ]
        docx_files = [
            path for path in extract_dir.rglob("*.docx")
            if not path.name.startswith("~$")
        ]
        doc_files = [
            path for path in extract_dir.rglob("*.doc")
            if not path.name.startswith("~$")
        ]
        pdf_files = [
            path for path in extract_dir.rglob("*.pdf")
        ]
        
        all_documents = xlsx_files + xls_files + docx_files + doc_files + pdf_files
        
        if not all_documents:
            raise DocumentSearchError("В архиве не найден ни один поддерживаемый документ (.xlsx, .xls, .docx, .doc, .pdf).")

        logger.info(f"Найдено документов после распаковки: {len(all_documents)} (Excel: {len(xlsx_files) + len(xls_files)}, Word: {len(docx_files) + len(doc_files)}, PDF: {len(pdf_files)})")
        for file in all_documents:
            logger.debug(f"  - {file}")
        return all_documents

    def _extract_zip_archive(self, archive_path: Path, extract_dir: Path) -> None:
        """Распаковка ZIP-архива с правильной обработкой кодировок."""
        logger.debug(f"Распаковка ZIP-архива: {archive_path.name}")
        try:
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                for zip_info in zip_ref.infolist():
                    try:
                        zip_info.filename = zip_info.filename.encode('cp437').decode('cp866')
                    except (UnicodeEncodeError, UnicodeDecodeError):
                        pass
                    zip_ref.extract(zip_info, extract_dir)
            logger.info(f"ZIP-архив {archive_path.name} успешно распакован")
        except zipfile.BadZipFile as error:
            logger.error(f"Поврежденный ZIP-архив: {error}")
            raise DocumentSearchError("ZIP-архив поврежден или имеет неподдерживаемый формат.") from error
        except Exception as error:
            logger.error(f"Ошибка распаковки ZIP-архива: {error}")
            get_error_logger().log_extraction_error(
                archive_path=archive_path,
                error_message=str(error),
                archive_type="zip",
            )
            raise DocumentSearchError(f"Ошибка распаковки ZIP-архива: {error}") from error

    def _extract_rar_archive(self, archive_path: Path, extract_dir: Path) -> None:
        """Распаковка RAR-архива."""
        logger.debug(f"Распаковка RAR-архива: {archive_path.name}")
        unrar_executable = self._ensure_unrar_available()
        logger.debug(f"Используемый UNRAR_TOOL: {unrar_executable}")

        command = [
            unrar_executable,
            "x",
            "-y",
            "-inul",  # Подавляем вывод UnRAR в консоль
            archive_path.name,
            str(extract_dir),
        ]

        logger.debug(f"Запускаю распаковку: {' '.join(command)} (cwd={archive_path.parent})")
        
        # Используем правильную кодировку для Windows (cp866 для консоли, cp1251 для GUI)
        import sys
        if sys.platform == 'win32':
            # Пробуем cp866 (консоль Windows) или cp1251 (GUI Windows)
            encoding = 'cp866'
        else:
            encoding = 'utf-8'
        
        result = subprocess.run(
            command,
            cwd=str(archive_path.parent),
            capture_output=True,
            text=True,
            encoding=encoding,
            errors='replace',  # Заменяем некорректные символы вместо ошибки
        )

        if result.returncode != 0:
            exit_code = result.returncode
            logger.error(f"Не удалось распаковать RAR-архив. Код завершения: {exit_code}")
            logger.error(f"STDOUT:\n{result.stdout}")
            logger.error(f"STDERR:\n{result.stderr}")
            self._log_rar_volume_debug_info(archive_path)

            if exit_code == 3:
                human_hint = (
                    "RAR сообщает код 3 (CRC/повреждение или отсутствуют части архива). "
                    "Проверьте, что скачаны все части .partXX.rar и файл не битый."
                )
            elif exit_code == 1:
                human_hint = (
                    "RAR завершился с кодом 1 (предупреждение). Проверьте целостность архива."
                )
            else:
                human_hint = "RAR-архив поврежден или имеет неподдерживаемый формат."

            get_error_logger().log_extraction_error(
                archive_path=archive_path,
                error_message=(
                    f"Код завершения: {exit_code}, "
                    f"STDOUT: {result.stdout}, STDERR: {result.stderr}"
                ),
                archive_type="rar",
            )
            raise DocumentSearchError(human_hint)

        logger.info(f"RAR-архив {archive_path.name} успешно распакован")

    def _extract_7z_archive(self, archive_path: Path, extract_dir: Path) -> None:
        """Распаковка 7Z-архива."""
        logger.debug(f"Распаковка 7Z-архива: {archive_path.name}")
        try:
            with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                archive.extractall(path=extract_dir)
            logger.info(f"7Z-архив {archive_path.name} успешно распакован")
        except Exception as error:
            logger.error(f"Ошибка распаковки 7Z-архива {archive_path.name}: {error}")
            get_error_logger().log_extraction_error(
                archive_path=archive_path,
                error_message=str(error),
                archive_type="7z",
            )
            raise DocumentSearchError(f"Ошибка распаковки 7Z-архива: {error}") from error

    def _ensure_unrar_available(self) -> str:
        """Проверка наличия инструмента распаковки RAR."""
        if self._rar_tool_configured and rarfile.UNRAR_TOOL:
            return rarfile.UNRAR_TOOL

        env_tool = os.environ.get("UNRAR_TOOL")
        candidate_paths = self._collect_unrar_candidates(env_tool)

        for candidate in candidate_paths:
            if not candidate:
                continue
            candidate_path = Path(candidate)
            if candidate_path.exists():
                rarfile.UNRAR_TOOL = str(candidate_path)
                self._rar_tool_configured = True
                logger.info(f"Найден инструмент распаковки RAR: {candidate_path}")
                return rarfile.UNRAR_TOOL

        self._rar_tool_configured = False
        logger.warning(
            "Инструмент распаковки RAR не найден. Установите WinRAR/UnRAR "
            "и задайте путь через переменную окружения UNRAR_TOOL.",
        )
        raise DocumentSearchError(
            "Инструмент для распаковки RAR не найден. "
            "Установите WinRAR/UnRAR и задайте UNRAR_TOOL.",
        )

    def _collect_unrar_candidates(self, env_tool: Optional[str]) -> List[str]:
        """Собирает список потенциальных путей к инструментам распаковки RAR."""
        candidates: List[str] = []

        if self._unrar_path:
            candidates.append(str(self._unrar_path))

        if env_tool:
            candidates.append(env_tool)

        possible_dirs = [
            str(self._winrar_path) if self._winrar_path else None,
            os.environ.get("WINRAR_PATH"),
        ]

        exe_names = ["UnRAR.exe", "UNRAR.exe", "UNRARG.exe", "WinRAR.exe", "rar.exe"]
        for directory in possible_dirs:
            if not directory:
                continue
            for exe_name in exe_names:
                candidates.append(os.path.join(directory, exe_name))

        for exe_name in exe_names:
            which_path = shutil.which(exe_name)
            if which_path:
                candidates.append(which_path)

        seen = set()
        unique_candidates = []
        for path in candidates:
            if path and path not in seen:
                unique_candidates.append(path)
                seen.add(path)

        return unique_candidates

    def _log_rar_volume_debug_info(self, archive_path: Path) -> None:
        """Логирование отладочной информации при ошибках распаковки."""
        logger.debug("=== Отладочная информация по архиву ===")
        logger.debug(f"Имя архива: {archive_path.name}")
        logger.debug(f"Расположение: {archive_path.parent}")
        logger.debug(f"Размер файла: {archive_path.stat().st_size} байт")
        logger.debug(f"Расширение: {archive_path.suffix}")

        base_name = archive_path.stem.replace("_combined", "")
        for neighbor in archive_path.parent.glob(f"{base_name}*"):
            if neighbor == archive_path:
                continue
            logger.debug(f"Найден соседний файл: {neighbor.name}")

    def combine_multi_part_archive(self, archive_paths: List[Path]) -> Path:
        """Склейка многофайлового архива в один файл."""
        if len(archive_paths) == 1:
            return archive_paths[0]
        
        logger.info(f"Обнаружен многофайловый архив из {len(archive_paths)} частей. Начинаю склейку...")
        
        first_path = archive_paths[0]
        base_name, _ = self._selector.split_archive_name(first_path.name)
        
        if not base_name:
            base_name = (
                first_path.stem.rsplit('.part', 1)[0]
                if '.part' in first_path.stem.lower()
                else first_path.stem
            )
        
        suffix = first_path.suffix.lower()
        combined_path = first_path.parent / f"{base_name}_combined{suffix}"
        
        try:
            with open(combined_path, 'wb') as combined_file:
                for part_path in sorted(archive_paths, key=lambda p: self._get_part_number(p)):
                    logger.debug(f"Добавляю часть: {part_path.name}")
                    with open(part_path, 'rb') as part_file:
                        while True:
                            chunk = part_file.read(8192)
                            if not chunk:
                                break
                            combined_file.write(chunk)
            
            logger.info(f"Многофайловый архив успешно склеен: {combined_path.name}")
            return combined_path
            
        except Exception as error:
            logger.error(f"Ошибка при склейке архива: {error}")
            logger.warning(f"Использую первую часть архива: {first_path.name}")
            return first_path

    def _get_part_number(self, path: Path) -> int:
        """Извлечение номера части из имени файла."""
        match = self._selector.ARCHIVE_PATTERN.match(path.name)
        if match and match.group("part"):
            return int(match.group("part"))
        
        name_lower = path.name.lower()
        if '.part' in name_lower:
            import re
            part_match = re.search(r'\.part(\d+)', name_lower)
            if part_match:
                return int(part_match.group(1))
        return 0

    def _register_extract_dir(self, path: Path) -> None:
        """Регистрирует директорию распаковки."""
        if path not in self._active_extract_dirs:
            self._active_extract_dirs.append(path)

    @property
    def active_extract_dirs(self) -> List[Path]:
        """Возвращает список директорий распаковки."""
        return self._active_extract_dirs.copy()

    def clear_active_extract_dirs(self) -> None:
        """Очищает список зарегистрированных директорий распаковки."""
        self._active_extract_dirs.clear()

    def is_file_archive(self, file_path: Path) -> bool:
        """
        Проверка, является ли файл архивом по его содержимому (magic bytes).
        
        Проверяет только RAR и 7Z, так как .xlsx технически является ZIP.
        """
        try:
            if not file_path.exists():
                return False
            
            with open(file_path, 'rb') as f:
                header = f.read(8)
            
            if len(header) >= 4:
                if header[:4] == b'Rar!':
                    return True
                if len(header) >= 2 and header[:2] == b'7z':
                    return True
            
            return False
        except Exception as error:
            logger.debug(f"Ошибка при проверке файла на архив: {error}")
            return False

