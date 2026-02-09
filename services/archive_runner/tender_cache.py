"""
MODULE: services.archive_runner.tender_cache
RESPONSIBILITY: Caching of tender data to reduce DB load.
ALLOWED: json, pathlib, hashlib, datetime, logging.
FORBIDDEN: DB connection management.
ERRORS: None.

Модуль кеширования закупок с учетом статусов.

Кеширует список закупок по настройкам пользователя и проверяет статусы
при последующих запусках для ускорения работы.
"""

from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

from loguru import logger


@dataclass
class CachedTender:
    """Кешированная закупка"""
    tender_id: int
    registry_type: str
    status_id: Optional[int]
    cached_at: datetime
    # Дополнительные поля для быстрого доступа
    auction_name: Optional[str] = None
    end_date: Optional[str] = None
    delivery_end_date: Optional[str] = None


@dataclass
class TenderCacheEntry:
    """Запись в кеше закупок"""
    user_id: int
    filters_hash: str
    tenders: List[CachedTender]
    cached_at: datetime
    expires_at: datetime


@dataclass
class AnalysisCacheEntry:
    """Запись в кеше анализа закупок"""
    user_id: int
    filters_hash: str
    tenders: List[CachedTender]
    cached_at: datetime
    expires_at: datetime
    last_status_check: Optional[datetime] = None


class TenderCache:
    """
    Кеш закупок с учетом статусов.
    
    Логика работы:
    1. При первом запуске: получаем закупки из БД, кешируем с их статусами
    2. При последующих запусках: проверяем статусы только для закупок из кеша (батч-запрос)
    3. Обновляем кеш на основе новых статусов
    4. Фильтруем закупки по статусам для отображения в правильных вкладках
    """
    
    def __init__(self, cache_dir: Optional[Path] = None, cache_ttl_hours: int = 24):
        """
        Args:
            cache_dir: Директория для хранения кеша (по умолчанию: .cache/tender_cache)
            cache_ttl_hours: Время жизни кеша в часах (по умолчанию: 24 часа)
        """
        if cache_dir is None:
            cache_dir = Path.home() / ".cache" / "tender_cache"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_ttl = timedelta(hours=cache_ttl_hours)
        
    def _get_filters_hash(self, filters: Dict[str, Any]) -> str:
        """Вычисляет хеш фильтров для уникальной идентификации кеша"""
        # Сортируем ключи для стабильности хеша
        sorted_filters = json.dumps(filters, sort_keys=True, default=str)
        return hashlib.md5(sorted_filters.encode()).hexdigest()
    
    def _get_cache_file_path(self, user_id: int, filters_hash: str) -> Path:
        """Получает путь к файлу кеша"""
        return self.cache_dir / f"user_{user_id}_{filters_hash}.json"
    
    def save_tenders(
        self,
        user_id: int,
        filters: Dict[str, Any],
        tenders: List[Dict[str, Any]],
    ) -> None:
        """
        Сохраняет закупки в кеш.
        
        Args:
            user_id: ID пользователя
            filters: Словарь с фильтрами (okpd_codes, stop_words, region_id, etc.)
            tenders: Список закупок из БД
        """
        filters_hash = self._get_filters_hash(filters)
        cache_file = self._get_cache_file_path(user_id, filters_hash)
        
        # Преобразуем закупки в CachedTender
        cached_tenders = []
        for tender in tenders:
            cached_tender = CachedTender(
                tender_id=tender.get("id"),
                registry_type=tender.get("registry_type", "44fz"),
                status_id=tender.get("status_id"),
                cached_at=datetime.now(),
                auction_name=tender.get("auction_name"),
                end_date=str(tender.get("end_date")) if tender.get("end_date") else None,
                delivery_end_date=str(tender.get("delivery_end_date")) if tender.get("delivery_end_date") else None,
            )
            cached_tenders.append(cached_tender)
        
        # Создаем запись кеша
        now = datetime.now()
        cache_entry = TenderCacheEntry(
            user_id=user_id,
            filters_hash=filters_hash,
            tenders=cached_tenders,
            cached_at=now,
            expires_at=now + self.cache_ttl,
        )
        
        # Сохраняем в файл с защитой от Access Violation
        try:
            # Создаем директорию если не существует
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    # Преобразуем dataclass в dict для JSON
                    cache_dict = asdict(cache_entry)
                    # Преобразуем datetime в строки
                    cache_dict["cached_at"] = cache_entry.cached_at.isoformat()
                    cache_dict["expires_at"] = cache_entry.expires_at.isoformat()
                    for tender in cache_dict["tenders"]:
                        tender["cached_at"] = tender["cached_at"].isoformat()
                    
                    json.dump(cache_dict, f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                
                logger.info(
                    f"Кеш закупок сохранен: {len(cached_tenders)} закупок для user_id={user_id}, "
                    f"фильтры={filters_hash[:8]}..."
                )
            except (OSError, IOError, PermissionError) as file_error:
                logger.warning(f"Ошибка доступа при сохранении кеш файла {cache_file.name}: {file_error}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении кеша закупок: {e}", exc_info=True)
    
    def load_tenders(
        self,
        user_id: int,
        filters: Dict[str, Any],
    ) -> Optional[List[CachedTender]]:
        """
        Загружает закупки из кеша.
        
        Args:
            user_id: ID пользователя
            filters: Словарь с фильтрами
            
        Returns:
            Список кешированных закупок или None если кеш не найден/истек
        """
        filters_hash = self._get_filters_hash(filters)
        cache_file = self._get_cache_file_path(user_id, filters_hash)
        
        if not cache_file.exists():
            logger.debug(f"Кеш не найден: {cache_file}")
            return None
        
        try:
            # Безопасное чтение файла с защитой от Access Violation
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cache_dict = json.load(f)
            except (OSError, IOError, PermissionError) as file_error:
                logger.warning(f"Ошибка доступа к кэш файлу {cache_file.name}: {file_error}")
                return None
            
            # Проверяем срок действия
            try:
                expires_at = datetime.fromisoformat(cache_dict["expires_at"])
            except (ValueError, TypeError, KeyError) as time_error:
                logger.warning(f"Ошибка парсинга времени в кэш файле {cache_file.name}: {time_error}")
                try:
                    cache_file.unlink(missing_ok=True)
                except Exception:
                    pass
                return None
            
            if datetime.now() > expires_at:
                logger.debug(f"Кеш истек: {cache_file}")
                try:
                    cache_file.unlink()  # Удаляем истекший кеш
                except Exception:
                    pass
                return None
            
            # Восстанавливаем из JSON
            cached_tenders = []
            try:
                for tender_dict in cache_dict["tenders"]:
                    cached_tender = CachedTender(
                        tender_id=tender_dict["tender_id"],
                        registry_type=tender_dict["registry_type"],
                        status_id=tender_dict.get("status_id"),
                        cached_at=datetime.fromisoformat(tender_dict["cached_at"]),
                        auction_name=tender_dict.get("auction_name"),
                        end_date=tender_dict.get("end_date"),
                        delivery_end_date=tender_dict.get("delivery_end_date"),
                    )
                    cached_tenders.append(cached_tender)
            except (ValueError, TypeError, KeyError) as parse_error:
                logger.warning(f"Ошибка парсинга данных в кэш файле {cache_file.name}: {parse_error}")
                try:
                    cache_file.unlink(missing_ok=True)
                except Exception:
                    pass
                return None
            
            logger.info(
                f"Кеш закупок загружен: {len(cached_tenders)} закупок для user_id={user_id}, "
                f"фильтры={filters_hash[:8]}..."
            )
            return cached_tenders
            
        except json.JSONDecodeError as e:
            logger.warning(f"Ошибка чтения JSON из кэш файла {cache_file.name}: {e}, удаляем.")
            try:
                cache_file.unlink(missing_ok=True)
            except Exception:
                pass
            return None
        except Exception as e:
            logger.error(f"Ошибка при загрузке кеша закупок: {e}", exc_info=True)
            # Удаляем поврежденный кеш
            try:
                cache_file.unlink(missing_ok=True)
            except Exception:
                pass
            return None
    
    def clear_cache(self, user_id: Optional[int] = None) -> None:
        """
        Очищает кеш.
        
        Args:
            user_id: Если указан, очищает кеш только для этого пользователя,
                     иначе очищает весь кеш
        """
        if user_id is not None:
            pattern = f"user_{user_id}_*.json"
            for cache_file in self.cache_dir.glob(pattern):
                try:
                    cache_file.unlink()
                    logger.debug(f"Удален кеш: {cache_file}")
                except Exception as e:
                    logger.warning(f"Не удалось удалить кеш {cache_file}: {e}")
        else:
            # Очищаем весь кеш
            for cache_file in self.cache_dir.glob("user_*.json"):
                try:
                    cache_file.unlink()
                    logger.debug(f"Удален кеш: {cache_file}")
                except Exception as e:
                    logger.warning(f"Не удалось удалить кеш {cache_file}: {e}")
        
        logger.info(f"Кеш очищен для user_id={user_id if user_id else 'всех пользователей'}")
    
    def get_tender_ids_for_status_check(
        self,
        user_id: int,
        filters: Dict[str, Any],
    ) -> Optional[List[Tuple[int, str]]]:
        """
        Получает список (tender_id, registry_type) из кеша для проверки статусов.
        
        Args:
            user_id: ID пользователя
            filters: Словарь с фильтрами
            
        Returns:
            Список кортежей (tender_id, registry_type) или None если кеш не найден
        """
        cached_tenders = self.load_tenders(user_id, filters)
        if cached_tenders is None:
            return None
        
        return [(t.tender_id, t.registry_type) for t in cached_tenders]


class AnalysisTenderCache(TenderCache):
    """
    Кеш закупок для анализа документов.

    Отличия от обычного TenderCache:
    - Время жизни: 30 дней (месяц)
    - Проверка статусов: каждый день в 17:00
    - Отдельная директория: analysis_cache
    """

    def __init__(self, db_manager=None):
        # Отдельная директория для кэша анализа
        cache_dir = Path.home() / ".cache" / "analysis_cache"
        # Время жизни: 30 дней
        super().__init__(cache_dir=cache_dir, cache_ttl_hours=30*24)
        self.db_manager = db_manager

    def _get_cache_file_path(self, user_id: int, filters_hash: str) -> Path:
        """Путь к файлу кэша анализа"""
        return self.cache_dir / f"analysis_user_{user_id}_{filters_hash}.json"

    def should_check_statuses(self, cache_file: Path) -> bool:
        """
        Проверяет, нужно ли обновить статусы в кэше.
        Обновление происходит каждый день в 17:00.
        """
        if not cache_file.exists():
            return False

        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache_dict = json.load(f)

            last_check_str = cache_dict.get("last_status_check")
            if not last_check_str:
                return True  # Если не было проверки, нужно проверить

            last_check = datetime.fromisoformat(last_check_str)
            now = datetime.now()

            # Проверяем, был ли сегодня запуск после 17:00
            today_17 = now.replace(hour=17, minute=0, second=0, microsecond=0)
            if now >= today_17 and last_check < today_17:
                return True

            return False

        except Exception as e:
            logger.warning(f"Ошибка проверки необходимости обновления статусов: {e}")
            return True  # В случае ошибки лучше проверить

    def update_statuses_if_needed(self, user_id: int, filters: Dict[str, Any], tenders: List[CachedTender]) -> List[CachedTender]:
        """
        Обновляет статусы в кэше, если прошло время проверки.
        """
        if not self.db_manager:
            return tenders

        filters_hash = self._get_filters_hash(filters)
        cache_file = self._get_cache_file_path(user_id, filters_hash)

        if not self.should_check_statuses(cache_file):
            return tenders

        logger.info("Обновляем статусы в кэше анализа...")

        # Получаем актуальные статусы для всех торгов
        tender_ids = [(t.tender_id, t.registry_type) for t in tenders]
        try:
            status_updates = self._get_statuses_batch(tender_ids)
            logger.info(f"Получено обновлений статусов: {len(status_updates)}")

            # Обновляем статусы в кэше
            updated_count = 0
            for tender in tenders:
                key = (tender.tender_id, tender.registry_type)
                if key in status_updates:
                    old_status = tender.status_id
                    tender.status_id = status_updates[key]
                    if old_status != tender.status_id:
                        updated_count += 1

            # Сохраняем обновленный кэш с меткой времени проверки
            if updated_count > 0:
                logger.info(f"Обновлено статусов: {updated_count}")
                self._save_cache_with_status_check(user_id, filters, tenders)

            return tenders

        except Exception as e:
            logger.warning(f"Ошибка обновления статусов в кэше: {e}")
            return tenders

    def _save_cache_with_status_check(self, user_id: int, filters: Dict[str, Any], tenders: List[CachedTender]) -> None:
        """Сохраняет кэш с меткой времени последней проверки статусов"""
        filters_hash = self._get_filters_hash(filters)
        cache_file = self._get_cache_file_path(user_id, filters_hash)

        cache_entry = AnalysisCacheEntry(
            user_id=user_id,
            filters_hash=filters_hash,
            tenders=tenders,
            cached_at=datetime.now(),
            expires_at=datetime.now() + timedelta(hours=self.cache_ttl_hours),
            last_status_check=datetime.now()
        )

        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(asdict(cache_entry), f, ensure_ascii=False, default=str, indent=2)
        except Exception as e:
            logger.warning(f"Ошибка сохранения кэша анализа: {e}")

    def load_tenders(self, user_id: int, filters: Dict[str, Any]) -> Optional[List[CachedTender]]:
        """
        Загружает закупки из кэша анализа с автоматическим обновлением статусов.
        """
        filters_hash = self._get_filters_hash(filters)
        cache_file = self._get_cache_file_path(user_id, filters_hash)

        if not cache_file.exists():
            logger.debug(f"Кеш анализа не найден: {cache_file}")
            return None

        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache_dict = json.load(f)
        except (OSError, IOError, PermissionError) as file_error:
            logger.warning(f"Ошибка доступа к кэш файлу анализа {cache_file.name}: {file_error}")
            return None

        # Проверяем срок действия (30 дней)
        try:
            expires_at = datetime.fromisoformat(cache_dict["expires_at"])
        except (ValueError, TypeError, KeyError) as time_error:
            logger.warning(f"Ошибка парсинга времени в кэш файле анализа {cache_file.name}: {time_error}")
            try:
                cache_file.unlink(missing_ok=True)
            except Exception:
                pass
            return None

        if datetime.now() > expires_at:
            logger.debug(f"Кеш анализа истек: {cache_file}")
            try:
                cache_file.unlink()
            except Exception:
                pass
            return None

        # Восстанавливаем из JSON
        cached_tenders = []
        try:
            for tender_dict in cache_dict["tenders"]:
                cached_tender = CachedTender(
                    tender_id=tender_dict.get("tender_id"),
                    registry_type=tender_dict.get("registry_type", "44fz"),
                    status_id=tender_dict.get("status_id"),
                    cached_at=datetime.fromisoformat(tender_dict["cached_at"]),
                    auction_name=tender_dict.get("auction_name"),
                    end_date=tender_dict.get("end_date"),
                    delivery_end_date=tender_dict.get("delivery_end_date"),
                )
                cached_tenders.append(cached_tender)
        except (KeyError, ValueError, TypeError) as parse_error:
            logger.warning(f"Ошибка парсинга кешированных данных анализа {cache_file.name}: {parse_error}")
            try:
                cache_file.unlink(missing_ok=True)
            except Exception:
                pass
            return None

        # Обновляем статусы если нужно
        cached_tenders = self.update_statuses_if_needed(user_id, filters, cached_tenders)

        return cached_tenders

