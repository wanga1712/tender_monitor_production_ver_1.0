"""
MODULE: services.tender_repositories.feeds.new_tenders_service
RESPONSIBILITY: Service for fetching new tenders (44FZ/223FZ).
ALLOWED: datetime, typing, loguru, psycopg2.extras, core.tender_database, services.tender_repositories.
FORBIDDEN: Hardcoded SQL literals (use query builder).
ERRORS: Database exceptions.

Сервис загрузки новых тендеров 44ФЗ и 223ФЗ.
"""

from __future__ import annotations

from typing import List, Dict, Any, Optional

from loguru import logger
from psycopg2.extras import RealDictCursor

from core.tender_database import TenderDatabaseManager
from services.tender_repositories.tender_documents_repository import TenderDocumentsRepository
from services.tender_repositories.tender_query_builder import TenderQueryBuilder
from services.tender_repositories.feeds.feed_filters import FeedFilters
from services.tender_repositories.feeds.base_feed_service import BaseFeedService


class NewTendersService(BaseFeedService):
    """Обрабатывает запросы для вкладок новых тендеров."""

    def __init__(
        self,
        db_manager: TenderDatabaseManager,
        documents_repo: TenderDocumentsRepository,
    ):
        super().__init__(db_manager, documents_repo)

    def fetch_44fz(self, filters: FeedFilters) -> List[Dict[str, Any]]:
        okpd_ids = self._resolve_okpd_ids(filters.okpd_codes)
        if not okpd_ids:
            logger.info("Нет ОКПД кодов для пользователя %s (44ФЗ)", filters.user_id)
            return []

        select_query, select_params = self._build_new_query("44fz", filters, okpd_ids)
        count_query, count_params = self._build_new_count_query("44fz", filters, okpd_ids)
        return self.execute_feed_query(
            select_query,
            select_params,
            count_query,
            count_params,
            registry_type="44fz",
            limit=filters.limit,
        )

    def fetch_223fz(self, filters: FeedFilters) -> List[Dict[str, Any]]:
        okpd_ids = self._resolve_okpd_ids(filters.okpd_codes)
        if not okpd_ids:
            logger.info("Нет ОКПД кодов для пользователя %s (223ФЗ)", filters.user_id)
            return []

        select_query, select_params = self._build_new_query("223fz", filters, okpd_ids)
        count_query, count_params = self._build_new_count_query("223fz", filters, okpd_ids)
        return self.execute_feed_query(
            select_query,
            select_params,
            count_query,
            count_params,
            registry_type="223fz",
            limit=filters.limit,
        )
    
    def fetch_commission_44fz(self, filters: FeedFilters) -> List[Dict[str, Any]]:
        """
        Загрузка закупок 44ФЗ со статусом "Работа комиссии" (status_id = 2)
        """
        okpd_ids = self._resolve_okpd_ids(filters.okpd_codes)
        if not okpd_ids:
            logger.info("Нет ОКПД кодов для пользователя %s (Работа комиссии 44ФЗ)", filters.user_id)
            return []

        select_query, select_params = self._build_commission_query("44fz", filters, okpd_ids)
        count_query, count_params = self._build_commission_count_query("44fz", filters, okpd_ids)
        return self.execute_feed_query(
            select_query,
            select_params,
            count_query,
            count_params,
            registry_type="44fz",
            limit=filters.limit,
        )

    def fetch_commission_223fz(self, filters: FeedFilters) -> List[Dict[str, Any]]:
        okpd_ids = self._resolve_okpd_ids(filters.okpd_codes)
        if not okpd_ids:
            logger.info("Нет ОКПД кодов для пользователя %s (Работа комиссии 223ФЗ)", filters.user_id)
            return []

        select_query, select_params = self._build_commission_query("223fz", filters, okpd_ids)
        count_query, count_params = self._build_commission_count_query("223fz", filters, okpd_ids)
        return self.execute_feed_query(
            select_query,
            select_params,
            count_query,
            count_params,
            registry_type="223fz",
            limit=filters.limit,
        )

    def _build_new_query(
        self,
        registry_type: str,
        filters: FeedFilters,
        okpd_ids: List[int],
    ) -> tuple[str, List[Any]]:
        select_fields = TenderQueryBuilder.build_base_select_fields()
        table_name = TenderQueryBuilder.resolve_registry_table(registry_type)
        base_joins = TenderQueryBuilder.build_base_joins(table_name, registry_type)
        query = f"SELECT DISTINCT {select_fields} {base_joins} WHERE 1=1"
        params: List[Any] = []

        placeholders = ",".join(["%s"] * len(okpd_ids))
        query += f" AND r.okpd_id IN ({placeholders})"
        params.extend(okpd_ids)

        region_filter, region_params = TenderQueryBuilder.build_region_filter(filters.region_id)
        query += region_filter
        params.extend(region_params)

        stop_filter, stop_params = TenderQueryBuilder.build_stop_words_filter(filters.stop_words)
        query += stop_filter
        params.extend(stop_params)

        query += TenderQueryBuilder.build_is_interesting_filter(registry_type)
        query += TenderQueryBuilder.build_order_by()

        if filters.limit and filters.limit > 0:
            query += " LIMIT %s"
            params.append(filters.limit)

        logger.debug(f"SQL запрос для новых торгов {registry_type}:\n{query}\nПараметры: {params}")
        
        # #region agent log
        import json
        import time
        log_path = r"c:\Users\wangr\PycharmProjects\pythonProject89\.cursor\debug.log"
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "B",
                    "location": "services/tender_repositories/feeds/new_tenders_service.py:_build_new_query:after_filters",
                    "message": "SQL запрос для новых торгов после применения фильтров",
                    "data": {
                        "registry_type": registry_type,
                        "okpd_ids_count": len(okpd_ids),
                        "region_id": filters.region_id,
                        "stop_words_count": len(filters.stop_words) if filters.stop_words else 0,
                        "limit": filters.limit,
                        "query_length": len(query),
                        "params_count": len(params)
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        # #endregion

        return query, params

    def _build_new_count_query(
        self,
        registry_type: str,
        filters: FeedFilters,
        okpd_ids: List[int],
    ) -> tuple[str, List[Any]]:
        table_name = TenderQueryBuilder.resolve_registry_table(registry_type)
        query = (
            f"SELECT COUNT(DISTINCT r.id) as total_count FROM {table_name} r "
            "LEFT JOIN customer c ON r.customer_id = c.id "
            "LEFT JOIN region reg ON r.region_id = reg.id "
            "LEFT JOIN contractor cont ON r.contractor_id = cont.id "
            "LEFT JOIN collection_codes_okpd okpd ON r.okpd_id = okpd.id "
            "LEFT JOIN trading_platform tp ON r.trading_platform_id = tp.id "
            "LEFT JOIN tender_document_matches tdm ON tdm.tender_id = r.id "
            f"AND tdm.registry_type = '{registry_type}' "
            "WHERE 1=1"
        )
        params: List[Any] = []

        placeholders = ",".join(["%s"] * len(okpd_ids))
        query += f" AND r.okpd_id IN ({placeholders})"
        params.extend(okpd_ids)

        region_filter, region_params = TenderQueryBuilder.build_region_filter(filters.region_id)
        query += region_filter
        params.extend(region_params)

        stop_filter, stop_params = TenderQueryBuilder.build_stop_words_filter(filters.stop_words)
        query += stop_filter
        params.extend(stop_params)

        query += TenderQueryBuilder.build_is_interesting_filter(registry_type)
        logger.debug(f"SQL запрос COUNT для новых торгов {registry_type}:\n{query}\nПараметры: {params}")
        
        # #region agent log
        import json
        import time
        log_path = r"c:\Users\wangr\PycharmProjects\pythonProject89\.cursor\debug.log"
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "C",
                    "location": "services/tender_repositories/feeds/new_tenders_service.py:_build_new_count_query:after_filters",
                    "message": "SQL запрос COUNT для новых торгов после применения фильтров",
                    "data": {
                        "registry_type": registry_type,
                        "okpd_ids_count": len(okpd_ids),
                        "region_id": filters.region_id,
                        "stop_words_count": len(filters.stop_words) if filters.stop_words else 0,
                        "query_length": len(query),
                        "params_count": len(params)
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        # #endregion
        
        return query, params

    def _build_commission_query(
        self,
        registry_type: str,
        filters: FeedFilters,
        okpd_ids: List[int],
    ) -> tuple[str, List[Any]]:
        select_fields = TenderQueryBuilder.build_base_select_fields()
        if registry_type == "44fz":
            table_name = "reestr_contract_44_fz_commission_work"
        elif registry_type == "223fz":
            table_name = "reestr_contract_223_fz_commission_work"
        else:
            table_name = TenderQueryBuilder.resolve_registry_table(registry_type)
        base_joins = TenderQueryBuilder.build_base_joins(table_name, registry_type)
        query = f"SELECT DISTINCT {select_fields} {base_joins} WHERE 1=1"
        params: List[Any] = []

        placeholders = ",".join(["%s"] * len(okpd_ids))
        query += f" AND r.okpd_id IN ({placeholders})"
        params.extend(okpd_ids)

        region_filter, region_params = TenderQueryBuilder.build_region_filter(filters.region_id)
        query += region_filter
        params.extend(region_params)

        stop_filter, stop_params = TenderQueryBuilder.build_stop_words_filter(filters.stop_words)
        query += stop_filter
        params.extend(stop_params)

        query += TenderQueryBuilder.build_is_interesting_filter(registry_type)
        query += TenderQueryBuilder.build_order_by()

        if filters.limit and filters.limit > 0:
            query += " LIMIT %s"
        params.append(filters.limit)

        logger.debug(f"SQL запрос для работы комиссии {registry_type}:\n{query}\nПараметры: {params}")

        return query, params

    def get_new_tenders_44fz(
        self,
        user_id: int,
        user_okpd_codes: Optional[List[str]] = None,
        user_stop_words: Optional[List[str]] = None,
        region_id: Optional[int] = None,
        category_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Получение новых тендеров 44-ФЗ."""
        filters = FeedFilters(
            user_id=user_id,
            okpd_codes=user_okpd_codes or [],
            stop_words=user_stop_words or [],
            region_id=region_id,
            category_id=category_id,
            limit=limit,
        )
        return self.fetch_44fz(filters)

    def get_new_tenders_223fz(
        self,
        user_id: int,
        user_okpd_codes: Optional[List[str]] = None,
        user_stop_words: Optional[List[str]] = None,
        region_id: Optional[int] = None,
        category_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Получение новых тендеров 223-ФЗ."""
        filters = FeedFilters(
            user_id=user_id,
            okpd_codes=user_okpd_codes or [],
            stop_words=user_stop_words or [],
            region_id=region_id,
            category_id=category_id,
            limit=limit,
        )
        return self.fetch_223fz(filters)
    
    def _build_commission_count_query(
        self,
        registry_type: str,
        filters: FeedFilters,
        okpd_ids: List[int],
    ) -> tuple[str, List[Any]]:
        if registry_type == "44fz":
            table_name = "reestr_contract_44_fz_commission_work"
        elif registry_type == "223fz":
            table_name = "reestr_contract_223_fz_commission_work"
        else:
            table_name = TenderQueryBuilder.resolve_registry_table(registry_type)
        select = (
            f"SELECT COUNT(DISTINCT r.id) as total_count FROM {table_name} r "
            "LEFT JOIN customer c ON r.customer_id = c.id "
            "LEFT JOIN region reg ON r.region_id = reg.id "
            "LEFT JOIN contractor cont ON r.contractor_id = cont.id "
            "LEFT JOIN collection_codes_okpd okpd ON r.okpd_id = okpd.id "
            "LEFT JOIN trading_platform tp ON r.trading_platform_id = tp.id "
            "LEFT JOIN tender_document_matches tdm ON tdm.tender_id = r.id "
            f"AND tdm.registry_type = '{registry_type}' "
            "WHERE 1=1"
        )
        params: List[Any] = []
        query = select

        placeholders = ",".join(["%s"] * len(okpd_ids))
        query += f" AND r.okpd_id IN ({placeholders})"
        params.extend(okpd_ids)

        region_filter, region_params = TenderQueryBuilder.build_region_filter(filters.region_id)
        query += region_filter
        params.extend(region_params)

        stop_filter, stop_params = TenderQueryBuilder.build_stop_words_filter(filters.stop_words)
        query += stop_filter
        params.extend(stop_params)

        query += TenderQueryBuilder.build_is_interesting_filter(registry_type)
        
        logger.debug(f"SQL запрос COUNT для работы комиссии {registry_type}:\n{query}\nПараметры: {params}")
        
        return query, params

    def _resolve_okpd_ids(self, user_okpd_codes: List[str]) -> List[int]:
        if not user_okpd_codes:
            return []
        query = """
            SELECT DISTINCT id FROM collection_codes_okpd
            WHERE main_code = ANY(%s) OR sub_code = ANY(%s)
        """
        results = self.db_manager.execute_query(
            query,
            (user_okpd_codes, user_okpd_codes),
            RealDictCursor,
        )
        return [row["id"] for row in results if row.get("id")] if results else []

