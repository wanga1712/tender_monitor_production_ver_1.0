"""Small fail-closed helpers for parser state and temporary storage."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable


class DiskGuardError(RuntimeError):
    """Raised when a download/unpack is unsafe for the current disk state."""


def atomic_write_text(path: str | Path, content: str, validator: Callable[[str], Any] | None = None) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if validator is not None:
            validator(temp_path.read_text(encoding="utf-8"))
        os.replace(temp_path, target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def atomic_write_json(path: str | Path, value: Any) -> None:
    content = json.dumps(value, ensure_ascii=False, indent=4, sort_keys=True)
    atomic_write_text(path, content, validator=json.loads)


def disk_usage_percent(path: str | Path) -> float:
    usage = shutil.disk_usage(path)
    return usage.used * 100.0 / usage.total


def ensure_download_allowed(path: str | Path, direction: str) -> float:
    """Fail closed at 95%, and pause backward earlier at 90%."""
    percent = disk_usage_percent(path)
    normalized = (direction or "forward").lower()
    if percent >= 95.0:
        raise DiskGuardError(f"disk guard: {percent:.1f}% used; download/unpack blocked")
    if normalized == "backward" and percent >= 90.0:
        raise DiskGuardError(f"disk guard: backward paused at {percent:.1f}% used")
    return percent


def cleanup_files(paths: list[str | Path]) -> None:
    for value in paths:
        try:
            Path(value).unlink(missing_ok=True)
        except OSError:
            pass
