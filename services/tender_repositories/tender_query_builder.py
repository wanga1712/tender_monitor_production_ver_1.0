"""
MODULE: services.tender_repositories.tender_query_builder
RESPONSIBILITY: Build SQL queries for tender search/filtering.
ALLOWED: typing, datetime, loguru, json, time.
FORBIDDEN: Executing queries (only building).
ERRORS: ValueError.

Билдер для построения оптимизированных SQL запросов для тендеров.
"""

from typing import List, Dict, Any, Optional
from datetime import date, timedelta
from loguru import logger


class TenderQueryBuilder:
    """Класс для построения оптимизированных SQL запросов"""

    REGISTRY_TABLES = {
        "44fz": "reestr_contract_44_fz",
        "223fz": "reestr_contract_223_fz",
    }

    @classmethod
    def resolve_registry_table(cls, registry_type: str) -> str:
        normalized = (registry_type or "").lower()
        table_name = cls.REGISTRY_TABLES.get(normalized)
        if not table_name:
            raise ValueError(f"Неизвестный тип реестра: {registry_type}")
        return table_name

    @staticmethod
    def build_status_filter_223fz() -> str:
        """
        Фильтр для закупок 223ФЗ по статусам.
        
        Использует только записи без статуса (status_id IS NULL) или с хорошими статусами.
        Исключает "Плохие" (status_id = 4).
        """
        return " AND (r.status_id IS NULL OR r.status_id != 4)"
    
    @staticmethod
    def build_base_select_fields(table_alias: str = "r") -> str:
        """Построение базового SELECT для тендеров"""
        return f"""
            {table_alias}.id,
            {table_alias}.contract_number,
            {table_alias}.tender_link,
            {table_alias}.start_date,
            {table_alias}.end_date,
            {table_alias}.delivery_start_date,
            {table_alias}.delivery_end_date,
            {table_alias}.auction_name,
            {table_alias}.initial_price,
            {table_alias}.final_price,
            {table_alias}.guarantee_amount,
            {table_alias}.customer_id,
            {table_alias}.contractor_id,
            {table_alias}.trading_platform_id,
            {table_alias}.okpd_id,
            {table_alias}.region_id,
            {table_alias}.delivery_region,
            {table_alias}.delivery_address,
            {table_alias}.status_id,
            c.customer_short_name,
            c.customer_full_name,
            reg.name as region_name,
            reg.code as region_code,
            cont.short_name as contractor_short_name,
            cont.full_name as contractor_full_name,
            okpd.main_code as okpd_main_code,
            okpd.sub_code as okpd_sub_code,
            okpd.name as okpd_name,
            tp.trading_platform_name as platform_name,
            tp.trading_platform_url as platform_url,
            c.customer_short_name as balance_holder_name,
            c.customer_inn as balance_holder_inn,
            tdm.processed_at
        """
    
    @staticmethod
    def build_base_joins(table_name: Optional[str], registry_type: str) -> str:
        """Построение базовых JOIN для тендеров"""
        resolved_table = table_name or TenderQueryBuilder.resolve_registry_table(registry_type)
        return f"""
            FROM {resolved_table} r
            LEFT JOIN customer c ON r.customer_id = c.id
            LEFT JOIN region reg ON r.region_id = reg.id
            LEFT JOIN contractor cont ON r.contractor_id = cont.id
            LEFT JOIN collection_codes_okpd okpd ON r.okpd_id = okpd.id
            LEFT JOIN trading_platform tp ON r.trading_platform_id = tp.id
            LEFT JOIN tender_document_matches tdm ON tdm.tender_id = r.id AND tdm.registry_type = '{registry_type}'
        """
    
    @staticmethod
    def build_new_tenders_filter(today: date, use_status: bool = True) -> tuple[str, List[Any]]:
        """
        Фильтр для новых тендеров.
        
        Если use_status=True (после миграции): использует статус (status_id = 1)
        Если use_status=False (до миграции): использует даты (end_date >= сегодня)
        
        Статус:
        - 1 = Новая (end_date <= CURRENT_DATE)
        
        ВАЖНО: Для "Работа комиссии" используется отдельный фильтр build_commission_tenders_filter
        """
        if use_status:
            # Используем статус - намного быстрее благодаря индексу
            # Только новые закупки (status_id = 1), без работы комиссии
            return " AND r.status_id = 1", []
        else:
            # Старый способ через даты (для обратной совместимости)
            return " AND (r.end_date IS NULL OR r.end_date >= %s)", [today]
    
    @staticmethod
    def build_commission_tenders_filter(use_status: bool = True) -> tuple[str, List[Any]]:
        """
        Фильтр для закупок со статусом "Работа комиссии".
        
        Если use_status=True (после миграции): использует статус (status_id = 2)
        Если use_status=False (до миграции): использует даты
        
        Статус:
        - 2 = Работа комиссии (end_date > CURRENT_DATE AND end_date <= CURRENT_DATE + 90 дней)
        """
        if use_status:
            # Используем статус - намного быстрее благодаря индексу
            return " AND r.status_id = 2", []
        else:
            # Старый способ через даты (для обратной совместимости)
            from datetime import timedelta
            today = date.today()
            max_end_date = today + timedelta(days=90)
            return " AND r.end_date > %s AND r.end_date <= %s", [today, max_end_date]
    
    @staticmethod
    def build_won_tenders_filter(today: date, use_status: bool = True) -> tuple[str, List[Any]]:
        """
        Фильтр для разыгранных тендеров.

        Если use_status=True: использует статус (status_id IN (2, 3) - разыгранные торги)
        Если use_status=False (до миграции): использует даты

        Статусы:
        - 2 = Разыграна (основной статус разыгранных торгов)
        - 3 = Разыграна (дополнительный статус)
        """
        if use_status:
            # Используем статусы разыгранных торгов - намного быстрее благодаря индексу
            return " AND r.status_id IN (2, 3)", []
        else:
            # Старый способ через даты (для обратной совместимости)
            min_delivery_date = today + timedelta(days=90)
            return (
                " AND r.end_date IS NOT NULL AND r.end_date < %s AND r.delivery_end_date IS NOT NULL AND r.delivery_end_date >= %s",
                [today, min_delivery_date]
            )
    
    @staticmethod
    def build_okpd_filter(user_okpd_codes: List[str]) -> tuple[str, List[Any]]:
        """Фильтр по ОКПД кодам"""
        if not user_okpd_codes:
            return "", []
        placeholders = ','.join(['%s'] * len(user_okpd_codes))
        return (
            f" AND (okpd.main_code IN ({placeholders}) OR okpd.sub_code IN ({placeholders}))",
            user_okpd_codes + user_okpd_codes
        )
    
    @staticmethod
    def build_region_filter(region_id: Optional[int]) -> tuple[str, List[Any]]:
        """Фильтр по региону"""
        if region_id is None:
            return "", []
        return " AND r.region_id = %s", [region_id]
    
    @staticmethod
    def build_stop_words_filter(user_stop_words: List[str]) -> tuple[str, List[Any]]:
        """Фильтр по стоп-словам"""
        if not user_stop_words:
            logger.debug("Стоп-слова не переданы в build_stop_words_filter")
            return "", []
        # #region agent log STOP_WORDS_STATS
        import json
        import time
        log_path = r"c:\Users\wangr\PycharmProjects\pythonProject89\.cursor\debug.log"
        started_at = time.time()
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "perf-startup",
                    "hypothesisId": "stop-words-slow",
                    "location": "tender_query_builder.py:build_stop_words_filter",
                    "message": "STOP_WORDS_START",
                    "data": {
                        "stop_words_count": len(user_stop_words),
                    },
                    "timestamp": int(started_at * 1000),
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        # #endregion
        conditions = []
        params = []
        for stop_word in user_stop_words:
            if stop_word and stop_word.strip():  # Проверяем, что стоп-слово не пустое
                conditions.append("LOWER(r.auction_name) NOT LIKE %s")
                params.append(f"%{stop_word.lower().strip()}%")
        if conditions:
            logger.debug(f"Применяется фильтр по стоп-словам: {len(conditions)} условий")
            # #region agent log STOP_WORDS_DONE
            finished_at = time.time()
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "perf-startup",
                        "hypothesisId": "stop-words-slow",
                        "location": "tender_query_builder.py:build_stop_words_filter",
                        "message": "STOP_WORDS_DONE",
                        "data": {
                            "conditions": len(conditions),
                            "duration_ms": int((finished_at - started_at) * 1000),
                        },
                        "timestamp": int(finished_at * 1000),
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion
            return f" AND {' AND '.join(conditions)}", params
        else:
            logger.warning(f"Все стоп-слова пустые или невалидные: {user_stop_words}")
            return "", []
    
    @staticmethod
    def build_is_interesting_filter(registry_type: str) -> str:
        """Фильтр для исключения неинтересных тендеров (is_interesting = FALSE)"""
        if registry_type == "44fz":
            registry_exists = """
                AND EXISTS (
                    SELECT 1 FROM (
                        SELECT id FROM reestr_contract_44_fz
                        UNION ALL SELECT id FROM reestr_contract_44_fz_commission_work
                        UNION ALL SELECT id FROM reestr_contract_44_fz_awarded
                    ) rc
                    WHERE rc.id = tdm_filter.tender_id
                )
            """
        else:
            registry_exists = """
                AND EXISTS (
                    SELECT 1 FROM (
                        SELECT id FROM reestr_contract_223_fz
                        UNION ALL SELECT id FROM reestr_contract_223_fz_commission_work
                        UNION ALL SELECT id FROM reestr_contract_223_fz_awarded
                    ) rc
                    WHERE rc.id = tdm_filter.tender_id
                )
            """
        return f"""
            AND NOT EXISTS (
                SELECT 1 FROM tender_document_matches tdm_filter
                WHERE tdm_filter.tender_id = r.id 
                AND tdm_filter.registry_type = '{registry_type}'
                AND tdm_filter.is_interesting = FALSE
                {registry_exists}
            )
        """
    
    @staticmethod
    def build_funnel_exclusion_filter(user_id: int) -> str:
        """Фильтр для исключения закупок, перемещенных в воронки продаж"""
        # Исключаем закупки, которые уже перемещены в воронку продаж
        return f"""
            AND NOT EXISTS (
                SELECT 1 FROM sales_deals sd
                WHERE sd.tender_id = r.id 
                AND sd.user_id = {user_id}
                AND sd.status != 'archived'
            )
        """
    
    @staticmethod
    def build_order_by() -> str:
        """Построение ORDER BY для сортировки"""
        return " ORDER BY tdm.processed_at DESC NULLS LAST, r.start_date DESC, r.id DESC"

