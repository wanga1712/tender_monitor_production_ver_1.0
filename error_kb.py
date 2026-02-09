"""
Модуль для работы с Error Knowledge Base (EKB).

Цели:
- сохранять информацию о критических и повторяющихся ошибках;
- использовать накопленные знания при отладке и эксплуатации;
- не смешивать логику EKB с логированием (loguru).

На данном этапе реализован минимальный функционал:
- запись ошибок в EKB (пока в виде простого файла/логической заглушки);
- заготовки для дальнейшей интеграции с PostgreSQL / SQLite.
"""

from __future__ import annotations

import json
import os
import platform
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

from config import PROJECT_ROOT


@dataclass
class ErrorRecord:
    """Структура записи об ошибке для Error Knowledge Base."""

    error_type: str
    message: str
    module: str
    func_name: str
    file_path: str
    os_name: str
    context: str
    execution_context: str
    traceback_str: str
    created_at: str


class ErrorKnowledgeBase:
    """
    Минимальная реализация Error Knowledge Base.

    Сейчас хранение реализовано в виде JSON-файла ekb_errors.json в корне проекта.
    В дальнейшем хранилище может быть перенесено в PostgreSQL / SQLite
    без изменения интерфейса класса.
    """

    def __init__(self, storage_path: Optional[Path] = None) -> None:
        """
        Инициализация EKB.

        :param storage_path: Путь к файлу/хранилищу EKB (по умолчанию ekb_errors.json)
        """
        if storage_path is None:
            storage_path = PROJECT_ROOT / "ekb_errors.json"
        self._storage_path = storage_path

    def _load_existing(self) -> list[Dict[str, Any]]:
        """Загружает существующие записи из локального JSON-файла."""
        if not self._storage_path.exists():
            return []
        try:
            with self._storage_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
            return []
        except Exception:
            # Не ломаем основную логику при ошибке в EKB.
            return []

    def _append_record(self, record: ErrorRecord) -> None:
        """
        Добавляет запись об ошибке в локальное хранилище.

        В будущем реализация будет заменена на запись в БД.
        """
        try:
            records = self._load_existing()
            records.append(asdict(record))
            with self._storage_path.open("w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            # Ошибки EKB не должны останавливать программу.
            logger.warning(f"Не удалось сохранить запись в EKB: {e}")

    def record_exception(
        self,
        exc: BaseException,
        *,
        module: str,
        func_name: str,
        file_path: str,
        context: str = "",
        execution_context: str = "systemd",
    ) -> None:
        """
        Записывает информацию об исключении в Error Knowledge Base.

        :param exc: Объект исключения
        :param module: Имя модуля, в котором произошла ошибка
        :param func_name: Имя функции/метода
        :param file_path: Путь к файлу
        :param context: Дополнительный текстовый контекст
        :param execution_context: Контекст исполнения (systemd, cli, cron и т.п.)
        """
        tb_str = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        record = ErrorRecord(
            error_type=type(exc).__name__,
            message=str(exc),
            module=module,
            func_name=func_name,
            file_path=file_path,
            os_name=platform.system().lower(),
            context=context,
            execution_context=execution_context,
            traceback_str=tb_str,
            created_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        )
        self._append_record(record)


# Глобальный экземпляр EKB для простого использования
_global_ekb = ErrorKnowledgeBase()


def get_ekb() -> ErrorKnowledgeBase:
    """
    Возвращает глобальный экземпляр ErrorKnowledgeBase.

    Используется в местах, где нет смысла создавать отдельный экземпляр.
    """
    return _global_ekb


