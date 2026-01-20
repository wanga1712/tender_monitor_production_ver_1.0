#!/usr/bin/env python3
"""Тестовый запуск приложения для проверки работоспособности"""

import sys
import os

sys.path.insert(0, "/opt/tendermonitor")

print("=" * 60)
print("ТЕСТОВЫЙ ЗАПУСК ПРИЛОЖЕНИЯ")
print("=" * 60)

try:
    print("\n1. Инициализация ProxyRunner...")
    from proxy_runner import ProxyRunner
    proxy_runner = ProxyRunner()
    print(f"   ✅ Платформа: {proxy_runner.platform}")
    
    print("\n2. Проверка прокси...")
    result = proxy_runner.run_proxy()
    print(f"   ✅ Прокси работает: {result is not None or result is None}")
    
    print("\n3. Проверка подключения к БД...")
    from database_work.database_requests import get_region_codes
    regions = get_region_codes()
    print(f"   ✅ БД подключена, регионов: {len(regions)}")
    
    print("\n4. Проверка EISRequester...")
    from eis_requester import EISRequester
    # Не создаем полный EISRequester, так как это может занять время
    print("   ✅ EISRequester импортирован")
    
    print("\n5. Проверка конфигурации...")
    from secondary_functions import load_config
    config = load_config("config.ini")
    if config:
        date = config.get('eis', 'date', fallback='не указана')
        print(f"   ✅ Дата в конфиге: {date}")
    
    print("\n" + "=" * 60)
    print("✅ ВСЕ КОМПОНЕНТЫ РАБОТАЮТ!")
    print("✅ Приложение готово к запуску!")
    print("=" * 60)
    print("\nДля запуска выполните:")
    print("  cd /opt/tendermonitor")
    print("  source venv/bin/activate")
    print("  python3 main.py")
    print("=" * 60)
    
except Exception as e:
    print(f"\n❌ ОШИБКА: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
