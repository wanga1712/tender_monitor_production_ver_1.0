"""
MODULE: services.tender_repositories.feeds.base_feed_service
RESPONSIBILITY: Base service for executing feed queries.
ALLOWED: typing, json, time, loguru, psycopg2.extras, core.tender_database, services.tender_repositories.
FORBIDDEN: Direct business logic for specific feeds (should be in subclasses).
ERRORS: Database exceptions.

Базовый сервис для формирования и выполнения запросов по тендерам.
"""

from __future__ import annotations

from typing import List, Dict, Any, Optional

from loguru import logger
from psycopg2.extras import RealDictCursor

from core.tender_database import TenderDatabaseManager
from services.tender_repositories.tender_documents_repository import TenderDocumentsRepository
from services.tender_repositories.tender_query_builder import TenderQueryBuilder
from services.tender_repositories.feeds.feed_filters import FeedFilters


class BaseFeedService:
    """Общий функционал для выборки тендеров разных типов."""

    def __init__(
        self,
        db_manager: TenderDatabaseManager,
        documents_repo: TenderDocumentsRepository,
    ):
        self.db_manager = db_manager
        self.documents_repo = documents_repo

    def execute_feed_query(
        self,
        select_query: str,
        select_params: List[Any],
        count_query: str,
        count_params: List[Any],
        registry_type: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        # #region agent log SELECT_START
        import json
        import time
        log_path = r"c:\Users\wangr\PycharmProjects\pythonProject89\.cursor\debug.log"
        select_started_at = time.time()
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "perf-startup",
                    "hypothesisId": "feed-query-slow",
                    "location": "base_feed_service.py:execute_feed_query",
                    "message": "SELECT_START",
                    "data": {
                        "registry_type": registry_type,
                        "limit": limit,
                        "params_count": len(select_params) if select_params else 0,
                    },
                    "timestamp": int(select_started_at * 1000),
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        # #endregion

        results = self.db_manager.execute_query(
            select_query,
            tuple(select_params) if select_params else None,
            RealDictCursor,
        )
        tenders = [dict(row) for row in results] if results else []

        # #region agent log SELECT_DONE
        select_finished_at = time.time()
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "perf-startup",
                    "hypothesisId": "feed-query-slow",
                    "location": "base_feed_service.py:execute_feed_query",
                    "message": "SELECT_DONE",
                    "data": {
                        "registry_type": registry_type,
                        "duration_ms": int((select_finished_at - select_started_at) * 1000),
                        "tenders_count": len(tenders),
                    },
                    "timestamp": int(select_finished_at * 1000),
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        # #endregion

        total_count = self._fetch_total_count(count_query, count_params)
        total = total_count if total_count is not None else "неизвестно"
        logger.info(f"Загружено торгов {registry_type}: {len(tenders)} из {total}")
        
        # #region agent log
        import json
        import time
        log_path = r"c:\Users\wangr\PycharmProjects\pythonProject89\.cursor\debug.log"
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "run1",
                    "hypothesisId": "D",
                    "location": "services/tender_repositories/feeds/base_feed_service.py:execute_feed_query:after_query",
                    "message": "Результат выполнения запроса закупок",
                    "data": {
                        "registry_type": registry_type,
                        "tenders_count": len(tenders),
                        "total_count": total_count,
                        "limit": limit,
                        "results_is_none": results is None,
                        "results_length": len(results) if results else 0
                    },
                    "timestamp": int(time.time() * 1000)
                }, ensure_ascii=False) + "\n")
        except Exception:
            pass
        # #endregion

        if tenders:
            tender_ids = [t["id"] for t in tenders]
            documents = self._load_documents_batch(tender_ids, registry_type)
            for tender in tenders:
                tender["document_links"] = documents.get(tender["id"], [])

            if total_count is not None:
                tenders[0]["_total_count"] = total_count
                tenders[0]["_loaded_count"] = len(tenders)
            else:
                inferred = len(tenders) if not limit or len(tenders) < limit else None
                tenders[0]["_total_count"] = inferred
                tenders[0]["_loaded_count"] = len(tenders)

        return tenders

    def _fetch_total_count(self, query: str, params: List[Any]) -> Optional[int]:
        if not query:
            return None
        try:
            # #region agent log COUNT_START
            import json
            import time
            log_path = r"c:\Users\wangr\PycharmProjects\pythonProject89\.cursor\debug.log"
            count_started_at = time.time()
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "perf-startup",
                        "hypothesisId": "feed-query-slow",
                        "location": "base_feed_service.py:_fetch_total_count",
                        "message": "COUNT_START",
                        "data": {
                            "params_count": len(params) if params else 0,
                        },
                        "timestamp": int(count_started_at * 1000),
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion

            result = self.db_manager.execute_query(query, tuple(params), RealDictCursor)
            # #region agent log COUNT_DONE
            count_finished_at = time.time()
            try:
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "sessionId": "debug-session",
                        "runId": "perf-startup",
                        "hypothesisId": "feed-query-slow",
                        "location": "base_feed_service.py:_fetch_total_count",
                        "message": "COUNT_DONE",
                        "data": {
                            "duration_ms": int((count_finished_at - count_started_at) * 1000),
                            "result_rows": len(result) if result else 0,
                        },
                        "timestamp": int(count_finished_at * 1000),
                    }, ensure_ascii=False) + "\n")
            except Exception:
                pass
            # #endregion

            return result[0].get("total_count") if result else None
        except Exception as error:
            logger.debug("Не удалось получить общее количество: %s", error)
            return None

    def _load_documents_batch(self, tender_ids: List[int], registry_type: str) -> Dict[int, List[Dict[str, Any]]]:
        if not tender_ids:
            return {}
        if registry_type == "223fz":
            return self.documents_repo.get_tender_documents_223fz_batch(tender_ids)
        return self.documents_repo.get_tender_documents_44fz_batch(tender_ids)

