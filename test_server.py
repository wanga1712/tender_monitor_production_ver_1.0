#!/usr/bin/env python3
"""Тестовый скрипт для проверки работы приложения на сервере"""

import sys
import os

print("=" * 60)
print("ТЕСТ РАБОТЫ ПРИЛОЖЕНИЯ НА СЕРВЕРЕ")
print("=" * 60)

# Проверка импортов
print("\n1. Проверка импортов...")
try:
    from proxy_runner import ProxyRunner
    print("   ✅ ProxyRunner импортирован")
    
    from eis_requester import EISRequester
    print("   ✅ EISRequester импортирован")
    
    from database_work.database_requests import get_region_codes
    print("   ✅ database_requests импортирован")
except Exception as e:
    print(f"   ❌ Ошибка импорта: {e}")
    sys.exit(1)

# Проверка ProxyRunner
print("\n2. Проверка ProxyRunner...")
try:
    pr = ProxyRunner()
    print(f"   ✅ Платформа: {pr.platform}")
    print(f"   ✅ Конфигурация загружена")
except Exception as e:
    print(f"   ❌ Ошибка ProxyRunner: {e}")
    sys.exit(1)

# Проверка подключения к БД
print("\n3. Проверка подключения к БД...")
try:
    regions = get_region_codes()
    print(f"   ✅ БД работает, регионов: {len(regions)}")
except Exception as e:
    print(f"   ❌ Ошибка БД: {e}")

# Проверка порта 8080
print("\n4. Проверка порта 8080...")
import socket
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    result = sock.connect_ex(('localhost', 8080))
    sock.close()
    if result == 0:
        print("   ✅ Порт 8080 доступен")
    else:
        print("   ❌ Порт 8080 недоступен")
except Exception as e:
    print(f"   ❌ Ошибка проверки порта: {e}")

# Проверка конфигурации
print("\n5. Проверка конфигурации...")
try:
    from secondary_functions import load_config
    config = load_config("config.ini")
    if config:
        print("   ✅ Конфигурация загружена")
        print(f"   ✅ env_file: {config.get('path', 'env_file', fallback='не указан')}")
    else:
        print("   ❌ Конфигурация не загружена")
except Exception as e:
    print(f"   ❌ Ошибка конфигурации: {e}")

print("\n" + "=" * 60)
print("ТЕСТ ЗАВЕРШЕН")
print("=" * 60)
