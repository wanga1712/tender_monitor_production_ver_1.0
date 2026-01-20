#!/usr/bin/env python3
"""Проверка готовности приложения к работе"""

import sys
import os

sys.path.insert(0, "/opt/tendermonitor")

print("=" * 60)
print("ПРОВЕРКА ГОТОВНОСТИ ПРИЛОЖЕНИЯ")
print("=" * 60)

errors = []
warnings = []

# 1. Проверка импортов
print("\n1. Проверка импортов...")
try:
    from proxy_runner import ProxyRunner
    print("   ✅ ProxyRunner")
except Exception as e:
    errors.append(f"ProxyRunner: {e}")
    print(f"   ❌ ProxyRunner: {e}")

try:
    from eis_requester import EISRequester
    print("   ✅ EISRequester")
except Exception as e:
    errors.append(f"EISRequester: {e}")
    print(f"   ❌ EISRequester: {e}")

try:
    from database_work.database_requests import get_region_codes
    print("   ✅ database_requests")
except Exception as e:
    errors.append(f"database_requests: {e}")
    print(f"   ❌ database_requests: {e}")

# 2. Проверка конфигурации
print("\n2. Проверка конфигурации...")
try:
    from secondary_functions import load_config
    config = load_config("config.ini")
    if config:
        print("   ✅ config.ini загружен")
        env_file = config.get('path', 'env_file', fallback='')
        if env_file and os.path.exists(env_file):
            print(f"   ✅ env_file найден: {env_file}")
            # Проверка токена
            with open(env_file, 'r') as f:
                content = f.read()
                if 'TOKEN=' in content and 'your_token_here' not in content:
                    print("   ✅ Токен настроен")
                else:
                    warnings.append("Токен не настроен в brum.env")
                    print("   ⚠️  Токен не настроен (your_token_here)")
        else:
            errors.append(f"env_file не найден: {env_file}")
            print(f"   ❌ env_file не найден: {env_file}")
    else:
        errors.append("config.ini не загружен")
        print("   ❌ config.ini не загружен")
except Exception as e:
    errors.append(f"Ошибка конфигурации: {e}")
    print(f"   ❌ Ошибка: {e}")

# 3. Проверка БД
print("\n3. Проверка базы данных...")
try:
    regions = get_region_codes()
    print(f"   ✅ БД работает, регионов: {len(regions)}")
except Exception as e:
    errors.append(f"БД: {e}")
    print(f"   ❌ Ошибка БД: {e}")

# 4. Проверка ProxyRunner
print("\n4. Проверка ProxyRunner...")
try:
    pr = ProxyRunner()
    print(f"   ✅ Платформа: {pr.platform}")
    # Проверка порта
    if pr.check_port_available("localhost", 8080, timeout=3):
        print("   ✅ Порт 8080 доступен")
    else:
        warnings.append("Порт 8080 недоступен")
        print("   ⚠️  Порт 8080 недоступен")
except Exception as e:
    errors.append(f"ProxyRunner: {e}")
    print(f"   ❌ Ошибка: {e}")

# 5. Проверка директорий
print("\n5. Проверка директорий...")
try:
    config = load_config("config.ini")
    dirs_to_check = [
        config.get('path', 'reest_new_contract_archive_44_fz_xml', fallback=''),
        config.get('path', 'recouped_contract_archive_44_fz_xml', fallback=''),
        config.get('path', 'reest_new_contract_archive_223_fz_xml', fallback=''),
        config.get('path', 'recouped_contract_archive_223_fz_xml', fallback=''),
        config.get('path', 'unziped_xml_files', fallback=''),
    ]
    for dir_path in dirs_to_check:
        if dir_path and os.path.exists(dir_path):
            print(f"   ✅ {os.path.basename(dir_path)}")
        elif dir_path:
            warnings.append(f"Директория не найдена: {dir_path}")
            print(f"   ⚠️  Не найдена: {os.path.basename(dir_path)}")
except Exception as e:
    warnings.append(f"Ошибка проверки директорий: {e}")

# Итоги
print("\n" + "=" * 60)
if errors:
    print("❌ КРИТИЧЕСКИЕ ОШИБКИ:")
    for error in errors:
        print(f"   - {error}")
    print("\n⚠️  Приложение не может работать!")
    sys.exit(1)
elif warnings:
    print("⚠️  ПРЕДУПРЕЖДЕНИЯ:")
    for warning in warnings:
        print(f"   - {warning}")
    print("\n✅ Приложение может работать, но есть предупреждения")
else:
    print("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ!")
    print("✅ Приложение готово к работе!")
print("=" * 60)
