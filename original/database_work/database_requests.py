from loguru import logger

from database_work.database_connection import DatabaseManager


def get_region_codes():
    """
    Получает список всех кодов регионов из базы данных.

    Выполняет запрос к базе данных, чтобы извлечь все коды регионов из таблицы `region`.
    Коды регионов сортируются по возрастанию.

    :return: Список кодов регионов (list[str]), если запрос выполнен успешно.
             Пустой список, если произошла ошибка.
    """
    db = DatabaseManager()

    try:
        # Выполняем запрос к базе данных для получения кодов регионов
        query = "SELECT code FROM region ORDER BY code;"
        db.cursor.execute(query)

        # Получаем все коды регионов и преобразуем их в список
        codes = [row[0] for row in db.cursor.fetchall()]

        # Логируем полученные коды
        logger.debug(f"Получены коды регионов: {codes}")

        return codes

    except Exception as e:
        # Логируем ошибку, если произошла ошибка при получении данных
        logger.exception(f"Ошибка при получении кодов регионов: {e}")
        return []

    finally:
        # Закрываем курсор и соединение с базой данных
        db.cursor.close()
        db.connection.close()
