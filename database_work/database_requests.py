from database_work.database_connection import DatabaseManager
from utils.exceptions import DatabaseError

from utils.logger_config import get_logger

# Получаем logger (только ошибки в файл)
logger = get_logger()


def get_region_codes():
    """
    Получает список всех кодов регионов из базы данных.

    Выполняет запрос к базе данных, чтобы извлечь все коды регионов из таблицы `region`.
    Коды регионов сортируются по возрастанию.

    :return: Список кодов регионов (list[str]), если запрос выполнен успешно.
    :raises DatabaseError: Если произошла ошибка при подключении к БД или выполнении запроса.
    """
    db = DatabaseManager()

    try:
        # Выполняем запрос к базе данных для получения кодов регионов
        # Используем DISTINCT для получения только уникальных кодов
        query = "SELECT DISTINCT code FROM region ORDER BY code;"
        db.cursor.execute(query)

        # Получаем все коды регионов и преобразуем их в список
        codes = [row[0] for row in db.cursor.fetchall()]

        return codes

    except DatabaseError:
        # Ошибка подключения к БД - пробрасываем дальше
        raise
    except Exception as e:
        # Другие ошибки БД - пробрасываем как DatabaseError
        error_msg = f"Ошибка при получении кодов регионов из БД: {e}"
        logger.error(error_msg, exc_info=True)
        raise DatabaseError(error_msg, original_error=e) from e

    finally:
        # Закрываем курсор и соединение с базой данных
        if db.cursor:
            db.cursor.close()
        if db.connection:
            db.connection.close()