"""
MODULE: services.interest_calculator

RESPONSIBILITY:
- Расчет бизнес-логики определения интереса к закупкам
- Определение правил и пороговых значений для is_interesting

ALLOWED:
- Чистая бизнес-логика без side effects
- Математические расчеты
- Константы и пороговые значения

FORBIDDEN:
- Доступ к базе данных
- Логирование
- Внешние зависимости
- Изменение состояния

ERRORS:
- Не должен выбрасывать исключения, только возвращать значения
"""

from typing import Optional


class InterestCalculator:
    """Калькулятор бизнес-логики определения интереса к закупкам"""
    
    # Пороговые значения для определения интереса
    INTEREST_THRESHOLD = 85.0  # Минимальный процент совпадений для интереса
    
    @classmethod
    def calculate_interest(
        cls,
        match_count: int,
        match_percentage: float,
        error_reason: Optional[str] = None
    ) -> bool:
        """
        Расчет интереса к закупке на основе результатов поиска
        
        Бизнес-правила:
        - Если найдено 0 совпадений → неинтересно (False)
        - Если процент совпадений ≥ 85% → интересно (True)  
        - Если процент совпадений < 85% → неинтересно (False)
        - При наличии ошибки → неинтересно (False)
        
        Args:
            match_count: Количество найденных совпадений
            match_percentage: Процент совпадений (0.0-100.0)
            error_reason: Причина ошибки (None если успешно)
        
        Returns:
            True если закупка интересна, False в противном случае
        """
        # При наличии ошибки - всегда неинтересно
        if error_reason is not None:
            return False
        
        # Если нет совпадений - неинтересно
        if match_count == 0:
            return False
        
        # Если процент совпадений выше порога - интересно
        if match_percentage >= cls.INTEREST_THRESHOLD:
            return True
        
        # Во всех остальных случаях - неинтересно
        return False
    
    @classmethod
    def should_update_interest(
        cls,
        existing_interest: Optional[bool],
        new_interest: bool
    ) -> bool:
        """
        Определяет, нужно ли обновлять значение интереса
        
        Правила:
        - Если существующее значение None → обновить
        - Если существующее значение False и новое True → обновить
        - Если существующее значение True → не обновлять (сохраняем положительный интерес)
        
        Args:
            existing_interest: Текущее значение is_interesting из БД
            new_interest: Новое рассчитанное значение
        
        Returns:
            True если нужно обновить, False в противном случае
        """
        # Если значения нет в БД - обязательно обновляем
        if existing_interest is None:
            return True
        
        # Если было неинтересно, а стало интересно - обновляем
        if not existing_interest and new_interest:
            return True
        
        # Сохраняем существующее положительное значение
        return False