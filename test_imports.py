"""
Тестовый скрипт для проверки импортов.
"""
try:
    print("Тестирование импортов...")
    
    print("1. Импорт get_logger...")
    from utils.logger_config import get_logger
    logger = get_logger()
    print("   ✅ get_logger импортирован успешно")
    
    print("2. Импорт XMLParser...")
    from utils import XMLParser
    print("   ✅ XMLParser импортирован успешно")
    
    print("3. Импорт EISRequester...")
    from eis_requester import EISRequester
    print("   ✅ EISRequester импортирован успешно")
    
    print("\n✅ Все импорты работают корректно!")
    
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    import traceback
    traceback.print_exc()
except Exception as e:
    print(f"❌ Неожиданная ошибка: {e}")
    import traceback
    traceback.print_exc()

