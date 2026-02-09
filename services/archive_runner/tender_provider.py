"""
MODULE: services.archive_runner.tender_provider
RESPONSIBILITY: Provide tenders for processing, abstracting DB access and caching.
ALLOWED: TenderRepository, TenderCache, ProcessedTendersRepository, logging.
FORBIDDEN: Direct SQL queries (use repositories).
ERRORS: None.

Модуль для получения торгов и документов из базы данных.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime, date
import re
from loguru import logger

from services.tender_services.tender_repository_facade import TenderRepositoryFacade
from services.archive_runner.tender_cache import TenderCache, CachedTender, AnalysisTenderCache
from services.archive_runner.processed_tenders_repository import ProcessedTendersRepository


class TenderProvider:
    """Предоставляет торги и документы для обработки."""

    def __init__(self, tender_repo: TenderRepositoryFacade, user_id: int, use_cache: bool = True, worker_id: Optional[str] = None):
        self.tender_repo = tender_repo
        self.user_id = user_id
        self.worker_id = worker_id
        self.cache = TenderCache() if use_cache else None
        self.processed_repo = ProcessedTendersRepository(tender_repo.db_manager)

    def get_target_tenders(
        self,
        region_id: Optional[int] = None,
        limit: int = 1000,
        specific_tender_ids: Optional[List[Dict[str, Any]]] = None,
        registry_type: Optional[str] = None,
        tender_type: str = 'new',
    ) -> List[Dict[str, Any]]:
        """
        Возвращает список торгов (44ФЗ + 223ФЗ) согласно настройкам пользователя.
        """
        # ... (код выше) ...

        # Иначе используем стандартную логику с настройками пользователя
        logger.info("Получение списка торгов для обработки (через TenderProvider)")
        user_okpd_list = self.tender_repo.get_user_okpd_codes(self.user_id)
        user_okpd_codes = [item.get("okpd_code") for item in user_okpd_list if item.get("okpd_code")]

        user_stop_words_list = self.tender_repo.get_user_stop_words(self.user_id)
        user_stop_words = [item.get("stop_word") for item in user_stop_words_list if item.get("stop_word")]
        
        # Подготавливаем Regex для фильтрации стоп-слов
        stop_words_pattern = None
        if user_stop_words:
            escaped_words = [re.escape(w) for w in user_stop_words if w and w.strip()]
            if escaped_words:
                try:
                    stop_words_pattern = re.compile("|".join(escaped_words), re.IGNORECASE)
                    logger.debug(f"Подготовлен Regex фильтр для {len(escaped_words)} стоп-слов")
                except Exception as e:
                    logger.error(f"Ошибка компиляции Regex для стоп-слов: {e}")

        if not user_okpd_codes:
            logger.warning(f"❌ У пользователя {self.user_id} нет настроенных ОКПД кодов. Настройте OKPD категории в разделе настроек.")
            return []

        # Формируем фильтры для кеша
        filters = {
            "okpd_codes": sorted(user_okpd_codes),
            "stop_words": sorted(user_stop_words),
            "region_id": region_id,
            "registry_type": registry_type,
            "tender_type": tender_type,
            "limit": limit,
        }
        
        # Пытаемся загрузить из кеша с защитой от ошибок
        cached_tenders = None
        if self.cache:
            try:
                cached_tenders = self.cache.load_tenders(self.user_id, filters)
            except Exception as cache_error:
                logger.warning(f"Ошибка при загрузке кеша торгов: {cache_error}", exc_info=True)
                cached_tenders = None
        else:
            logger.info("🚫 Кеш ОТКЛЮЧЕН (use_cache=False). Запрос будет выполнен напрямую к БД.")
        
        if cached_tenders:
             # ... (код для работы с кешем) ...
             pass
        
        logger.info("Кеш не найден или отключен, получаем закупки из БД...")
        
        base_limit = limit
        fetch_limit = base_limit
        filtered_tenders: List[Dict[str, Any]] = []
        skipped_count = 0
        stop_word_count = 0
        seen_keys: set = set()
        attempts = 0
        max_attempts = 5

        while len(filtered_tenders) < limit and attempts < max_attempts:
            tenders_44fz: List[Dict[str, Any]] = []
            tenders_223fz: List[Dict[str, Any]] = []

            if registry_type is None or registry_type == '44fz':
                if tender_type == 'new':
                    tenders_44fz = self.tender_repo.get_new_tenders_44fz(
                        user_id=self.user_id,
                        user_okpd_codes=user_okpd_codes,
                        user_stop_words=user_stop_words,
                        limit=fetch_limit
                    )
                    for tender in tenders_44fz:
                        tender["registry_type"] = "44fz"
                elif tender_type == 'won':
                    tenders_44fz = self.tender_repo.get_won_tenders_44fz(
                        user_id=self.user_id,
                        user_okpd_codes=user_okpd_codes,
                        user_stop_words=user_stop_words,
                        limit=fetch_limit
                    )
                    for tender in tenders_44fz:
                        tender["registry_type"] = "44fz_won"

            if registry_type is None or registry_type == '223fz':
                if tender_type == 'new':
                    tenders_223fz = self.tender_repo.get_new_tenders_223fz(
                        user_id=self.user_id,
                        user_okpd_codes=user_okpd_codes,
                        user_stop_words=user_stop_words,
                        limit=fetch_limit
                    )
                    for tender in tenders_223fz:
                        tender["registry_type"] = "223fz"
                elif tender_type == 'won':
                    tenders_223fz = self.tender_repo.get_won_tenders_223fz(
                        user_id=self.user_id,
                        user_okpd_codes=user_okpd_codes,
                        user_stop_words=user_stop_words,
                        limit=fetch_limit
                    )
                    for tender in tenders_223fz:
                        tender["registry_type"] = "223fz_won"

            all_tenders = tenders_44fz + tenders_223fz

            for tender in all_tenders:
                if len(filtered_tenders) >= limit:
                    break
                tender_id = tender.get("id")
                reg_type = tender.get("registry_type", registry_type or "44fz")
                key = (tender_id, reg_type)
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                if stop_words_pattern:
                    auction_name = tender.get("auction_name") or ""
                    if stop_words_pattern.search(auction_name):
                        stop_word_count += 1
                        continue

                base_folder_name = f"{reg_type}_{tender_id}"
                is_processed = self.processed_repo.is_tender_processed(
                    tender_id,
                    reg_type,
                    base_folder_name,
                    worker_id=self.worker_id
                )
                if is_processed:
                    skipped_count += 1
                else:
                    filtered_tenders.append(tender)

            if len(filtered_tenders) < limit:
                attempts += 1
                fetch_limit = min(fetch_limit * 2, base_limit * 10)
                continue
            break

        sorted_tenders = self._sort_tenders_by_time(filtered_tenders[:limit], tender_type)

        logger.info(
            "Получено торгов: %s (44ФЗ: %s, 223ФЗ: %s)%s%s | Отфильтровано обработанных: %s",
            len(sorted_tenders),
            len([t for t in sorted_tenders if t.get("registry_type", "").startswith("44fz")]),
            len([t for t in sorted_tenders if t.get("registry_type", "").startswith("223fz")]),
            f" [фильтр: {registry_type}]" if registry_type else "",
            f" [тип: {tender_type}]" if tender_type != 'new' else "",
            skipped_count
        )

        if self.cache and sorted_tenders:
            self.cache.save_tenders(self.user_id, filters, sorted_tenders)

        return sorted_tenders
    
    def _get_statuses_batch(self, tender_ids: List[Tuple[int, str]]) -> Dict[Tuple[int, str], Optional[int]]:
        """
        Получает статусы закупок батч-запросом.
        
        Args:
            tender_ids: Список кортежей (tender_id, registry_type)
            
        Returns:
            Словарь {(tender_id, registry_type): status_id}
        """
        if not tender_ids:
            return {}
        
        # Группируем по registry_type
        ids_by_registry: Dict[str, List[int]] = {}
        for tender_id, registry_type in tender_ids:
            if registry_type not in ids_by_registry:
                ids_by_registry[registry_type] = []
            ids_by_registry[registry_type].append(tender_id)
        
        status_map = {}
        
        # Получаем статусы для каждого реестра
        for reg_type, ids in ids_by_registry.items():
            try:
                # Используем метод репозитория для получения статусов
                # Получаем только необходимые поля (id, status_id) для оптимизации
                tenders = self.tender_repo.get_tenders_by_ids(
                    tender_ids_44fz=ids if reg_type == '44fz' else None,
                    tender_ids_223fz=ids if reg_type == '223fz' else None,
                )
                
                for tender in tenders:
                    key = (tender['id'], reg_type)
                    status_map[key] = tender.get('status_id')
            except Exception as e:
                logger.warning(f"Ошибка при получении статусов для {reg_type}: {e}")
        
        return status_map
    
    def _matches_tender_type(self, cached_tender: CachedTender, tender_type: str) -> bool:
        """
        Проверяет, соответствует ли кешированная закупка типу tender_type.
        """
        if tender_type == 'new':
            # Новые торги: status_id = 1
            return cached_tender.status_id in (None, 1)
        elif tender_type == 'won':
            # Разыгранные торги: status_id = 2 (Разыграна) или 3 (Разыграна)
            # ВНИМАНИЕ: status_id = 2 также используется для "Работа комиссии" в старой схеме,
            # но для won_tenders_service мы используем build_won_tenders_filter, который берет 2 и 3.
            return cached_tender.status_id in (2, 3)
        elif tender_type == 'commission':
            # Работа комиссии: status_id = 2
            return cached_tender.status_id == 2
        return True
    
    def _cached_to_tenders(self, cached_tenders: List[CachedTender]) -> List[Dict[str, Any]]:
        """Преобразует кешированные закупки в формат для возврата"""
        tenders = []
        for cached in cached_tenders:
            tender = {
                "id": cached.tender_id,
                "registry_type": cached.registry_type,
                "status_id": cached.status_id,
            }
            if cached.auction_name:
                tender["auction_name"] = cached.auction_name
            if cached.end_date:
                tender["end_date"] = cached.end_date
            if cached.delivery_end_date:
                tender["delivery_end_date"] = cached.delivery_end_date
            
            tenders.append(tender)
        
        return tenders

    def _parse_date(self, value: Any) -> Optional[date]:
        if not value:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        try:
            return datetime.fromisoformat(str(value)).date()
        except Exception:
            return None

    def _sort_tenders_by_time(self, tenders: List[Dict[str, Any]], tender_type: str) -> List[Dict[str, Any]]:
        today = datetime.utcnow().date()

        def sort_key(tender: Dict[str, Any]) -> tuple[int, int]:
            if tender_type == 'new':
                end_date_value = self._parse_date(tender.get("end_date"))
                if end_date_value is None:
                    return 1, 0
                days_left = (end_date_value - today).days
                return 0, -days_left
            if tender_type == 'won':
                delivery_end_date_value = self._parse_date(tender.get("delivery_end_date"))
                if delivery_end_date_value is None:
                    return 1, 0
                days_left = (delivery_end_date_value - today).days
                return 0, -days_left
            return 1, 0

        return sorted(tenders, key=sort_key)

    def get_tender_documents(self, tender_id: int, registry_type: str) -> List[Dict[str, Any]]:
        """
        Возвращает список документов торга по ID и типу реестра.
        """
        documents = self.tender_repo.get_tender_documents(tender_id, registry_type)
        if not documents:
            logger.warning(
                "Для торга %s (%s) не найдено документов",
                tender_id,
                registry_type,
            )
        return documents

    def get_tenders_for_analysis(self, filters: Dict[str, Any], registry_type: str = "44fz",
                                tender_type: str = "won") -> List[Dict[str, Any]]:
        """
        Получает торги для анализа документов с использованием кэша анализа.

        Args:
            filters: Фильтры пользователя (okpd_codes, stop_words, region_id, category_id)
            registry_type: Тип реестра ("44fz" или "223fz")
            tender_type: Тип торгов ("new", "commission", "won")

        Returns:
            Список торгов для анализа
        """
        # Создаем кэш анализа
        analysis_cache = AnalysisTenderCache(db_manager=self.tender_repo.db_manager)

        # Пытаемся загрузить из кэша анализа
        cached_tenders = None
        if analysis_cache:
            try:
                cached_tenders = analysis_cache.load_tenders(self.user_id, filters)
                if cached_tenders:
                    logger.info(f"Найдено в кэше анализа: {len(cached_tenders)} торгов")
            except Exception as cache_error:
                logger.warning(f"Ошибка загрузки кэша анализа: {cache_error}", exc_info=True)
                cached_tenders = None

        if cached_tenders:
            # Фильтруем по типу торгов и преобразуем в формат для анализа
            filtered_tenders = []
            for cached in cached_tenders:
                if self._matches_tender_type(cached, tender_type):
                    tender = self._convert_cached_to_tender(cached)
                    filtered_tenders.append(tender)

            logger.info(f"После фильтрации по типу '{tender_type}': {len(filtered_tenders)} торгов")
            return filtered_tenders

        # Кэш не найден - получаем из TenderRepository
        logger.info("Кэш анализа не найден, получаем торги из БД...")

        if tender_type == "won":
            tenders = self.tender_repo.get_won_tenders_44fz(
                user_id=self.user_id,
                user_okpd_codes=filters.get("okpd_codes"),
                user_stop_words=filters.get("stop_words"),
                region_id=filters.get("region_id"),
                category_id=filters.get("category_id"),
                limit=10000  # Больший лимит для анализа
            ) if registry_type == "44fz" else self.tender_repo.get_won_tenders_223fz(
                user_id=self.user_id,
                user_okpd_codes=filters.get("okpd_codes"),
                user_stop_words=filters.get("stop_words"),
                region_id=filters.get("region_id"),
                category_id=filters.get("category_id"),
                limit=10000
            )
        elif tender_type == "commission":
            tenders = self.tender_repo.get_commission_tenders_44fz(
                user_id=self.user_id,
                user_okpd_codes=filters.get("okpd_codes"),
                user_stop_words=filters.get("stop_words"),
                region_id=filters.get("region_id"),
                category_id=filters.get("category_id"),
                limit=10000
            )
        else:  # new
            tenders = self.tender_repo.get_new_tenders_44fz(
                user_id=self.user_id,
                user_okpd_codes=filters.get("okpd_codes"),
                user_stop_words=filters.get("stop_words"),
                region_id=filters.get("region_id"),
                category_id=filters.get("category_id"),
                limit=10000
            ) if registry_type == "44fz" else self.tender_repo.get_new_tenders_223fz(
                user_id=self.user_id,
                user_okpd_codes=filters.get("okpd_codes"),
                user_stop_words=filters.get("stop_words"),
                region_id=filters.get("region_id"),
                category_id=filters.get("category_id"),
                limit=10000
            )

        logger.info(f"Получено из БД: {len(tenders) if tenders else 0} торгов для анализа")

        # Сохраняем в кэш анализа
        if analysis_cache and tenders:
            try:
                logger.info("Сохраняем торги в кэш анализа...")
                analysis_cache.save_tenders(self.user_id, filters, tenders)
                logger.info("Торги сохранены в кэш анализа")
            except Exception as cache_error:
                logger.warning(f"Ошибка сохранения в кэш анализа: {cache_error}", exc_info=True)

        return tenders or []

    def _matches_tender_type(self, cached_tender: CachedTender, tender_type: str) -> bool:
        """Проверяет, соответствует ли кешированная торг типу анализа"""
        if tender_type == 'new':
            return cached_tender.status_id in (1, 2)  # Новые или Работа комиссии
        elif tender_type == 'won':
            return cached_tender.status_id in (2, 3)  # Работа комиссии или Разыгранные
        elif tender_type == 'commission':
            return cached_tender.status_id == 2  # Только Работа комиссии
        return True

    def _convert_cached_to_tender(self, cached: CachedTender) -> Dict[str, Any]:
        """Преобразует CachedTender в формат торгов для анализа"""
        return {
            "id": cached.tender_id,
            "registry_type": cached.registry_type,
            "status_id": cached.status_id,
            "auction_name": cached.auction_name,
            "end_date": cached.end_date,
            "delivery_end_date": cached.delivery_end_date,
        }
