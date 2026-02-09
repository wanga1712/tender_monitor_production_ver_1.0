#!/usr/bin/env python3
"""
MODULE: scripts.test_document_analysis
RESPONSIBILITY: Testing document analysis logic with detailed logging.
ALLOWED: sys, os, time, json, pathlib, config.settings, services.archive_runner.runner, services.document_search.product_search_service, core.tender_database.
FORBIDDEN: None.
ERRORS: None.

Скрипт для тестирования анализа документов с детальным логированием.
"""

import sys
import os
import time
import json
from pathlib import Path

# Добавляем корневую папку проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def log_message(message, data=None, hypothesis_id="TEST"):
    """Логирование в debug.log"""
    log_path = project_root / ".cursor" / "debug.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    log_entry = {
        "sessionId": "debug-session",
        "runId": "test-run",
        "hypothesisId": hypothesis_id,
        "location": "test_document_analysis.py",
        "message": message,
        "data": data or {},
        "timestamp": int(time.time() * 1000)
    }

    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        print(f"✅ LOG: {message}")
    except Exception as e:
        print(f"❌ LOG ERROR: {e}")

def test_imports():
    """Тестирование импортов"""
    log_message("Начинаем тестирование импортов")

    try:
        from config.settings import config
        log_message("✅ config imported", {"config_loaded": True})
    except Exception as e:
        log_message("❌ config import error", {"error": str(e)})
        return False

    try:
        from services.archive_runner.runner import ArchiveBackgroundRunner
        log_message("✅ ArchiveBackgroundRunner imported", {"runner_loaded": True})
    except Exception as e:
        log_message("❌ ArchiveBackgroundRunner import error", {"error": str(e)})
        return False

    try:
        from services.document_search.product_search_service import ProductSearchService
        log_message("✅ ProductSearchService imported", {"search_loaded": True})
    except Exception as e:
        log_message("❌ ProductSearchService import error", {"error": str(e)})
        return False

    return True

def test_database_connection():
    """Тестирование подключения к БД"""
    log_message("Тестируем подключение к БД")

    try:
        from config.settings import config
        from core.tender_database import TenderDatabaseManager

        db_manager = TenderDatabaseManager(config.database)
        db_manager.connect()
        log_message("✅ Подключение к БД установлено", {"connected": True})

        # Проверяем количество торгов
        count_result = db_manager.execute_query("SELECT COUNT(*) as count FROM reestr_contract_44_fz WHERE status_id IN (2, 3)")
        if count_result:
            count = count_result[0]['count']
            log_message("✅ Найдено торгов для анализа", {"won_tenders_count": count})
        else:
            log_message("⚠️ Нет данных о количестве торгов", {"count_result": None})

        db_manager.close()
        log_message("✅ Подключение к БД закрыто", {"disconnected": True})
        return True

    except Exception as e:
        log_message("❌ Ошибка подключения к БД", {"error": str(e)})
        return False

def main():
    """Главная функция тестирования"""
    print("🚀 Запуск тестирования анализа документов")
    print("Логи будут записаны в .cursor/debug.log")

    log_message("=== НАЧАЛО ТЕСТИРОВАНИЯ ===")

    # Тест 1: Импорты
    if not test_imports():
        log_message("❌ Тестирование остановлено из-за проблем с импортами")
        return

    # Тест 2: БД
    if not test_database_connection():
        log_message("❌ Тестирование остановлено из-за проблем с БД")
        return

    log_message("✅ Все тесты пройдены успешно")
    print("✅ Тестирование завершено. Проверьте логи в .cursor/debug.log")

if __name__ == "__main__":
    main()
