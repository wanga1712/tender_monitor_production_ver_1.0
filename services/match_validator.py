"""
MODULE: services.match_validator

RESPONSIBILITY:
- Валидация данных и проверка существования сущностей в БД
- Проверка структуры таблиц и колонок
- Валидация входных параметров

ALLOWED:
- Запросы метаданных БД через TenderDatabaseManager
- Проверки существования таблиц и колонок
- Валидация параметров

FORBIDDEN:
- Бизнес-логика
- Изменение данных
- Логирование ошибок (только возврат результатов)

ERRORS:
- Должен возвращать False/None при ошибках, не выбрасывать исключения
"""

from typing import Optional
from core.tender_database import TenderDatabaseManager
from psycopg2.extras import RealDictCursor


class MatchValidator:
    """Валидатор для проверок существования и корректности данных"""
    
    def __init__(self, db_manager: TenderDatabaseManager):
        """
        Инициализация валидатора
        
        Args:
            db_manager: Менеджер базы данных tender_monitor
        """
        self.db_manager = db_manager
    
    def column_exists(self, table_name: str, column_name: str) -> bool:
        """
        Проверяет, существует ли колонка в таблице.
        
        Args:
            table_name: Название таблицы
            column_name: Название колонки
        
        Returns:
            True если колонка существует, False в противном случае
        """
        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = %s 
                    AND column_name = %s
                ) AS exists
            """
            result = self.db_manager.execute_query(query, (table_name, column_name), RealDictCursor)
            if result and len(result) > 0:
                return result[0].get('exists', False)
            return False
        except Exception:
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """
        Проверяет, существует ли таблица в базе данных.
        
        Args:
            table_name: Название таблицы
        
        Returns:
            True если таблица существует, False в противном случае
        """
        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                ) AS exists
            """
            result = self.db_manager.execute_query(query, (table_name,), RealDictCursor)
            if result and len(result) > 0:
                return result[0].get('exists', False)
            return False
        except Exception:
            return False
    
    def validate_match_params(
        self,
        tender_id: int,
        registry_type: str,
        match_count: int,
        match_percentage: float
    ) -> bool:
        """
        Валидация основных параметров результата поиска
        
        Args:
            tender_id: ID закупки
            registry_type: Тип реестра
            match_count: Количество совпадений
            match_percentage: Процент совпадений
        
        Returns:
            True если параметры валидны, False в противном случае
        """
        # Проверка обязательных полей
        if tender_id <= 0:
            return False
        
        if registry_type not in ('44fz', '223fz'):
            return False
        
        if match_count < 0:
            return False
        
        if match_percentage < 0.0 or match_percentage > 100.0:
            return False
        
        return True
    
    def validate_folder_name(self, folder_name: Optional[str]) -> bool:
        """
        Валидация имени папки
        
        Args:
            folder_name: Имя папки
        
        Returns:
            True если имя валидно, False в противном случае
        """
        if folder_name is None:
            return True
        
        # Базовые проверки имени папки
        if not isinstance(folder_name, str):
            return False
        
        if len(folder_name.strip()) == 0:
            return False
        
        # Запрещенные символы в путях
        forbidden_chars = ['<', '>', ':', '"', '|', '?', '*']
        if any(char in folder_name for char in forbidden_chars):
            return False
        
        return True