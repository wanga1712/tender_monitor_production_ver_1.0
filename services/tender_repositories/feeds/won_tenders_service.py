"""
MODULE: services.tender_repositories.feeds.won_tenders_service
RESPONSIBILITY: Service for fetching won tenders.
ALLOWED: datetime, typing, loguru, psycopg2.extras, core.tender_database, services.tender_repositories.
FORBIDDEN: Hardcoded SQL literals (use query builder).
ERRORS: Database exceptions.

Сервис загрузки разыгранных тендеров 44ФЗ и 223ФЗ.
"""

from __future__ import annotations

from datetime import date
from typing import List, Dict, Any, Optional

from loguru import logger
from psycopg2.extras import RealDictCursor

from core.tender_database import TenderDatabaseManager
from services.tender_repositories.tender_documents_repository import TenderDocumentsRepository
from services.tender_repositories.tender_query_builder import TenderQueryBuilder
from services.tender_repositories.feeds.feed_filters import WonFilters
from services.tender_repositories.feeds.base_feed_service import BaseFeedService


class WonTendersService(BaseFeedService):
    """Обрабатывает запросы для вкладок разыгранных тендеров."""

    def __init__(
        self,
        db_manager: TenderDatabaseManager,
        documents_repo: TenderDocumentsRepository,
    ):
        super().__init__(db_manager, documents_repo)

    def fetch_44fz(self, filters: WonFilters) -> List[Dict[str, Any]]:
        # Если OKPD коды не указаны (None или пустой список), получаем все торги без фильтра OKPD
        if not filters.okpd_codes:  # None или пустой список
            okpd_ids = None  # Без фильтра OKPD
        else:
            okpd_ids = self._resolve_okpd_ids(filters.okpd_codes)
            if not okpd_ids:
                return []

        # Используем "44fz_won" для разделения с новыми торгами
        reg_type = "44fz_won"
        select_query, select_params = self._build_won_query("44fz", filters, okpd_ids, None)
        count_query, count_params = self._build_won_count_query("44fz", filters, okpd_ids, None)
        return self.execute_feed_query(
            select_query,
            select_params,
            count_query,
            count_params,
            registry_type=reg_type,
            limit=filters.limit,
            include_total_count=False # Оптимизация для демона
        )

    def fetch_223fz(self, filters: WonFilters) -> List[Dict[str, Any]]:
        # Если OKPD коды не указаны (None или пустой список), получаем все торги без фильтра OKPD
        if not filters.okpd_codes:  # None или пустой список
            okpd_ids = None  # Без фильтра OKPD
        else:
            okpd_ids = self._resolve_okpd_ids(filters.okpd_codes)
            if not okpd_ids:
                return []

        # Используем "223fz_won" для разделения с новыми торгами
        reg_type = "223fz_won"
        select_query, select_params = self._build_won_query("223fz", filters, okpd_ids, None)
        count_query, count_params = self._build_won_count_query("223fz", filters, okpd_ids, None)
        return self.execute_feed_query(
            select_query,
            select_params,
            count_query,
            count_params,
            registry_type=reg_type,
            limit=filters.limit,
            include_total_count=False # Оптимизация для демона
        )

    def get_won_tenders_44fz(
        self,
        user_id: int,
        user_okpd_codes: Optional[List[str]] = None,
        user_stop_words: Optional[List[str]] = None,
        region_id: Optional[int] = None,
        category_id: Optional[int] = None,
        limit: int = 100,
        exclude_processed: bool = False,
    ) -> List[Dict[str, Any]]:
        """Получение разыгранных тендеров 44-ФЗ."""
        filters = WonFilters(
            user_id=user_id,
            okpd_codes=user_okpd_codes or [],
            stop_words=user_stop_words or [],
            region_id=region_id,
            category_id=category_id,
            limit=limit,
            exclude_processed=exclude_processed,
        )
        return self.fetch_44fz(filters)

    def get_won_tenders_223fz(
        self,
        user_id: int,
        user_okpd_codes: Optional[List[str]] = None,
        user_stop_words: Optional[List[str]] = None,
        region_id: Optional[int] = None,
        category_id: Optional[int] = None,
        limit: int = 100,
        exclude_processed: bool = False,
    ) -> List[Dict[str, Any]]:
        """Получение разыгранных тендеров 223-ФЗ."""
        filters = WonFilters(
            user_id=user_id,
            okpd_codes=user_okpd_codes or [],
            stop_words=user_stop_words or [],
            region_id=region_id,
            category_id=category_id,
            limit=limit,
            exclude_processed=exclude_processed,
        )
        return self.fetch_223fz(filters)

    def _build_won_query(
        self,
        registry_type: str,
        filters: WonFilters,
        okpd_ids: Optional[List[int]],
        processed_reg_type: str = None,
    ) -> tuple[str, List[Any]]:
        select_fields = TenderQueryBuilder.build_base_select_fields()
        if registry_type == "44fz":
            table_name = "reestr_contract_44_fz_awarded"
        elif registry_type == "223fz":
            table_name = "reestr_contract_223_fz_awarded"
        else:
            table_name = TenderQueryBuilder.resolve_registry_table(registry_type)
        base_joins = TenderQueryBuilder.build_base_joins(table_name, registry_type, processed_reg_type=processed_reg_type)
        query = f"SELECT {select_fields} {base_joins} WHERE 1=1"
        params: List[Any] = []

        # Добавляем фильтр OKPD только если он указан
        if okpd_ids:
            placeholders = ",".join(["%s"] * len(okpd_ids))
            query += f" AND r.okpd_id IN ({placeholders})"
            params.extend(okpd_ids)

        region_filter, region_params = TenderQueryBuilder.build_region_filter(filters.region_id)
        query += region_filter
        params.extend(region_params)

        stop_filter, stop_params = TenderQueryBuilder.build_stop_words_filter(filters.user_id, filters.stop_words)
        query += stop_filter
        params.extend(stop_params)

        query += TenderQueryBuilder.build_funnel_exclusion_filter(filters.user_id)
        
        # Для разыгранных торгов всегда исключаем уже обработанные/занятые записи
        query += " AND tdm.id IS NULL"
        query += TenderQueryBuilder.build_order_by(include_processed_at=False)

        if filters.limit and filters.limit > 0:
            query += " LIMIT %s"
            params.append(filters.limit)

        return query, params

    def _build_won_count_query(
        self,
        registry_type: str,
        filters: WonFilters,
        okpd_ids: Optional[List[int]],
        processed_reg_type: str = None,
    ) -> tuple[str, List[Any]]:
        if registry_type == "44fz":
            table_name = "reestr_contract_44_fz_awarded"
        elif registry_type == "223fz":
            table_name = "reestr_contract_223_fz_awarded"
        else:
            table_name = TenderQueryBuilder.resolve_registry_table(registry_type)
        
        reg_type_for_join = processed_reg_type if processed_reg_type else registry_type
            
        select = (
            f"SELECT COUNT(DISTINCT r.id) as total_count FROM {table_name} r "
            "LEFT JOIN customer c ON r.customer_id = c.id "
            "LEFT JOIN region reg ON r.region_id = reg.id "
            "LEFT JOIN contractor cont ON r.contractor_id = cont.id "
            "LEFT JOIN collection_codes_okpd okpd ON r.okpd_id = okpd.id "
            "LEFT JOIN trading_platform tp ON r.trading_platform_id = tp.id "
            "LEFT JOIN tender_document_matches tdm ON tdm.tender_id = r.id "
            f"AND tdm.registry_type = '{reg_type_for_join}' "
            "WHERE 1=1"
        )
        query = select
        params: List[Any] = []

        # Добавляем фильтр OKPD только если он указан
        if okpd_ids:
            placeholders = ",".join(["%s"] * len(okpd_ids))
            query += f" AND r.okpd_id IN ({placeholders})"
            params.extend(okpd_ids)

        region_filter, region_params = TenderQueryBuilder.build_region_filter(filters.region_id)
        query += region_filter
        params.extend(region_params)

        stop_filter, stop_params = TenderQueryBuilder.build_stop_words_filter(filters.user_id, filters.stop_words)
        query += stop_filter
        params.extend(stop_params)

        query += TenderQueryBuilder.build_funnel_exclusion_filter(filters.user_id)
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

