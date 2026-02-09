"""
Простой тестовый скрипт для вывода списка торговых площадок из БД.

Задача:
- подключиться к БД через существующий DatabaseManager;
- выбрать несколько записей из таблицы trading_platform;
- вывести их в консоль в удобном виде.

Скрипт НЕ использует основной цикл main.py и не трогает боевой код.
"""

from __future__ import annotations

from database_work.database_connection import DatabaseManager
from utils.logger_config import get_logger


logger = get_logger()


def print_trading_platforms(limit: int = 20) -> None:
    """
    Выводит в консоль первые N торговых площадок из таблицы trading_platform.

    :param limit: Максимальное количество записей для вывода.
    """
    db = DatabaseManager()
    try:
        query = """
            SELECT id, trading_platform_name, trading_platform_url
            FROM trading_platform
            ORDER BY id
            LIMIT %s;
        """
        db.cursor.execute(query, (limit,))
        rows = db.cursor.fetchall()

        if not rows:
            print("⚠️  В таблице trading_platform нет записей.")
            return

        print("\n============================================================")
        print("СПИСОК ТОРГОВЫХ ПЛОЩАДОК (trading_platform)")
        print("============================================================\n")

        for row in rows:
            tp_id, name, url = row
            print(f"[{tp_id}] {name}  |  {url}")

        print("\n============================================================")
        print(f"Всего показано записей: {len(rows)}")
        print("============================================================\n")

    except Exception as e:
        logger.error(f"Ошибка при получении списка торговых площадок: {e}", exc_info=True)
        print(f"❌ Ошибка при получении списка торговых площадок: {e}")
    finally:
        try:
            db.close()
        except Exception:
            pass


if __name__ == "__main__":
    print_trading_platforms(limit=50)

