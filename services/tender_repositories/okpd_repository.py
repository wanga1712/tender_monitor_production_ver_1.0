"""
MODULE: services.tender_repositories.okpd_repository
RESPONSIBILITY: Manage OKPD codes (search, get by code) with caching.
ALLOWED: typing, loguru, core.tender_database, psycopg2.extras.
FORBIDDEN: Business logic outside DB operations.
ERRORS: Database exceptions.

Репозиторий для работы с кодами ОКПД.
"""

from typing import List, Dict, Any, Optional
from loguru import logger
from core.tender_database import TenderDatabaseManager
from psycopg2.extras import RealDictCursor


class OkpdRepository:
    """Репозиторий для работы с кодами ОКПД с кэшированием"""
    
    # Кэш для всех ОКПД кодов (ключ: None для всех, или (region_id, search_text))
    _cache_all_okpd: Optional[List[Dict[str, Any]]] = None
    _cache_region_okpd: Dict[tuple, List[Dict[str, Any]]] = {}
    _cache_search_okpd: Dict[str, List[Dict[str, Any]]] = {}
    
    def __init__(self, db_manager: TenderDatabaseManager):
        self.db_manager = db_manager
    
    def search_okpd_codes(
        self, 
        search_text: Optional[str] = None, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Поиск кодов ОКПД по тексту или коду с кэшированием"""
        try:
            # Проверяем кэш для поиска
            if search_text:
                search_key = search_text.strip().lower()
                if search_key in self._cache_search_okpd:
                    cached = self._cache_search_okpd[search_key]
                    logger.debug(f"Использован кэш для поиска ОКПД: '{search_text}' ({len(cached)} записей)")
                    return cached[:limit] if limit else cached
            
            # Если нет поиска, используем кэш всех ОКПД
            if not search_text and self._cache_all_okpd is not None:
                logger.debug(f"Использован кэш всех ОКПД ({len(self._cache_all_okpd)} записей)")
                return self._cache_all_okpd[:limit] if limit else self._cache_all_okpd
            
            # Загружаем из БД
            query = """
                SELECT 
                    id,
                    main_code,
                    sub_code,
                    parent_id,
                    name
                FROM collection_codes_okpd
            """
            params = []
            
            if search_text:
                search_text = search_text.strip()
                if search_text.isdigit() or (search_text.replace('.', '').isdigit()):
                    query += " WHERE (main_code LIKE %s OR sub_code LIKE %s)"
                    search_pattern = f"{search_text}%"
                    params = [search_pattern, search_pattern]
                else:
                    query += " WHERE LOWER(name) LIKE %s"
                    search_pattern = f"%{search_text.lower()}%"
                    params = [search_pattern]
            
            query += " ORDER BY main_code NULLS LAST, sub_code NULLS LAST"
            # Для кэширования загружаем больше, чем limit
            query_limit = limit * 2 if limit else 1000
            query += f" LIMIT {query_limit}"
            
            logger.debug(f"Выполнение запроса ОКПД из БД (search={search_text}, limit={query_limit})")
            results = self.db_manager.execute_query(query, tuple(params) if params else None, RealDictCursor)
            okpd_list = [dict(row) for row in results] if results else []
            
            # Сохраняем в кэш
            if search_text:
                search_key = search_text.strip().lower()
                self._cache_search_okpd[search_key] = okpd_list
            else:
                # Кэшируем все ОКПД
                self._cache_all_okpd = okpd_list
            
            logger.info(f"Загружено из БД и закэшировано ОКПД кодов: {len(okpd_list)}")
            return okpd_list[:limit] if limit else okpd_list
            
        except Exception as e:
            logger.error(f"Ошибка при поиске кодов ОКПД: {e}", exc_info=True)
            return []
    
    def get_all_okpd_codes(self, limit: int = 500) -> List[Dict[str, Any]]:
        """Получение всех кодов ОКПД"""
        return self.search_okpd_codes(search_text=None, limit=limit)
    
    def get_okpd_by_code(self, okpd_code: str) -> Optional[Dict[str, Any]]:
        """Получение информации об ОКПД по коду"""
        try:
            query = """
                SELECT 
                    id,
                    main_code,
                    sub_code,
                    parent_id,
                    name
                FROM collection_codes_okpd
                WHERE main_code = %s OR sub_code = %s
                LIMIT 1
            """
            results = self.db_manager.execute_query(
                query,
                (okpd_code, okpd_code),
                RealDictCursor
            )
            if results:
                return dict(results[0])
            return None
            
        except Exception as e:
            logger.error(f"Ошибка при получении ОКПД по коду: {e}")
            return None
    
    def search_okpd_codes_by_region(
        self, 
        search_text: Optional[str] = None,
        region_id: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Поиск кодов ОКПД с фильтрацией по региону (оптимизированная версия с кэшированием)"""
        try:
            if region_id is None:
                return self.search_okpd_codes(search_text, limit)
            
            # Проверяем кэш для региона
            cache_key = (region_id, search_text.strip().lower() if search_text else None)
            if cache_key in self._cache_region_okpd:
                cached = self._cache_region_okpd[cache_key]
                logger.debug(f"Использован кэш ОКПД для региона {region_id} ({len(cached)} записей)")
                return cached[:limit] if limit else cached
            
            # Оптимизированный запрос: сначала получаем уникальные okpd_id из обеих таблиц,
            # затем джойним с collection_codes_okpd - это быстрее чем EXISTS на 22 млн записей
            query = """
                SELECT DISTINCT
                    c.id,
                    c.main_code,
                    c.sub_code,
                    c.parent_id,
                    c.name
                FROM collection_codes_okpd c
                INNER JOIN (
                    SELECT DISTINCT okpd_id FROM reestr_contract_44_fz WHERE region_id = %s
                    UNION
                    SELECT DISTINCT okpd_id FROM reestr_contract_223_fz WHERE region_id = %s
                ) AS region_okpd ON region_okpd.okpd_id = c.id
            """
            params = [region_id, region_id]
            
            if search_text:
                search_text = search_text.strip()
                if search_text.isdigit() or (search_text.replace('.', '').isdigit()):
                    query += " WHERE (c.main_code LIKE %s OR c.sub_code LIKE %s)"
                    search_pattern = f"{search_text}%"
                    params.extend([search_pattern, search_pattern])
                else:
                    query += " WHERE LOWER(c.name) LIKE %s"
                    search_pattern = f"%{search_text.lower()}%"
                    params.append(search_pattern)
            
            # Для кэширования загружаем больше
            query_limit = limit * 2 if limit else 1000
            query += f" ORDER BY c.main_code NULLS LAST, c.sub_code NULLS LAST LIMIT {query_limit}"
            
            logger.debug(f"Выполнение запроса ОКПД по региону {region_id} из БД, limit={query_limit}")
            results = self.db_manager.execute_query(query, tuple(params), RealDictCursor)
            okpd_list = [dict(row) for row in results] if results else []
            
            # Сохраняем в кэш
            self._cache_region_okpd[cache_key] = okpd_list
            
            logger.info(f"Загружено из БД и закэшировано ОКПД кодов по региону {region_id}: {len(okpd_list)}")
            return okpd_list[:limit] if limit else okpd_list
            
        except Exception as e:
            logger.error(f"Ошибка при поиске кодов ОКПД по региону: {e}", exc_info=True)
            # Fallback на простой поиск без фильтра по региону
            logger.warning("Используется fallback: поиск ОКПД без фильтра по региону")
            return self.search_okpd_codes(search_text, limit)

