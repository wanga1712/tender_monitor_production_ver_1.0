"""
Поиск контракта по номеру во всех реестрах (identity lookup).

Набор status-таблиц НЕ зависит от incoming end_date: ищем во ВСЕХ применимых
таблицах (completed → awarded → unclear → unknown → commission_work → main) и
выбираем самую terminal-локацию. Локатор никогда не должен «пропускать» terminal
строку из-за будущего дедлайна входящего XML.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from utils.logger_config import get_logger

from database_work.contract_location import ContractLocation
from database_work.database_connection import DatabaseManager
from database_work.registry_tables import all_lookup_tables, lookup_order, tables_for_fz


def build_unified_lookup_sql(table_names: list[str]) -> str:
    """One round-trip lookup. Each LIMIT 1 branch must be parenthesized
    or PostgreSQL rejects UNION ALL (syntax error at UNION).
    """
    branches = []
    for priority, table_name in enumerate(table_names):
        branches.append(
            "("
            f"SELECT id, '{table_name}'::text AS table_name, "
            f"{priority} AS priority FROM {table_name} "
            "WHERE contract_number = %s LIMIT 1"
            ")"
        )
    return (
        "SELECT id, table_name FROM ("
        + " UNION ALL ".join(branches)
        + ") candidates ORDER BY priority LIMIT 1"
    )


def build_unified_pairs_sql(pairs: list[tuple[str, str]]) -> str:
    """One round-trip identity lookup across several (fz_type, table) pairs.

    ``pairs`` is already in terminal-first precedence order, so the winning row
    is the most terminal location for the contract number.
    """
    branches = []
    for priority, (fz_type, table_name) in enumerate(pairs):
        branches.append(
            "("
            f"SELECT id, '{table_name}'::text AS table_name, "
            f"'{fz_type}'::text AS fz_type, {priority} AS priority "
            f"FROM {table_name} WHERE contract_number = %s LIMIT 1"
            ")"
        )
    return (
        "SELECT id, table_name, fz_type FROM ("
        + " UNION ALL ".join(branches)
        + ") candidates ORDER BY priority LIMIT 1"
    )


logger = get_logger()


class ContractRegistryLocator:
    """Ищет контракт по номеру в реестрах 44/223."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self._db = db_manager or DatabaseManager()

    def find_by_number(
        self,
        contract_number: str,
        end_date: Any = None,
        fz_type: Optional[str] = None,
    ) -> Optional[ContractLocation]:
        """
        Глобальный поиск по номеру.

        ``end_date`` НЕ сужает набор status-таблиц: identity lookup всегда
        полный (terminal-first). Параметр сохранён для обратной совместимости
        вызовов и больше не влияет на результат.
        """
        number = self._normalize_number(contract_number)
        if not number:
            return None
        if fz_type:
            return self.find_in_fz(fz_type, number)
        return self._find_pairs(number, all_lookup_tables())

    def find_in_fz(
        self,
        fz_type: str,
        contract_number: str,
        end_date: Any = None,
        force_full: bool = False,
    ) -> Optional[ContractLocation]:
        """Поиск в контуре одного ФЗ (полный набор status-таблиц, terminal-first)."""
        number = self._normalize_number(contract_number)
        if not number:
            return None
        return self._find_tables(number, fz_type, lookup_order(tables_for_fz(fz_type)))

    def find_in_fz_one_query(
        self, fz_type: str, contract_number: str
    ) -> Optional[ContractLocation]:
        """Terminal-first identity lookup using one DB round-trip."""
        number = self._normalize_number(contract_number)
        if not number:
            return None
        return self._find_tables(number, fz_type, lookup_order(tables_for_fz(fz_type)))

    def _find_pairs(
        self,
        number: str,
        pairs: List[Tuple[str, str]],
    ) -> Optional[ContractLocation]:
        """Identity lookup across 44+223 (one UNION ALL round-trip)."""
        query = build_unified_pairs_sql(pairs)
        params = [number] * len(pairs)
        try:
            with self._db.connection.cursor() as cursor:
                cursor.execute(query, tuple(params))
                row = cursor.fetchone()
            if not row:
                return None
            return ContractLocation(
                fz_type=str(row[2]),
                table_name=str(row[1]),
                record_id=int(row[0]),
                contract_number=number,
            )
        except Exception as exc:
            logger.error(f"Ошибка unified lookup контракта {number}: {exc}")
            try:
                self._db.connection.rollback()
            except Exception:
                pass
            return None

    def _find_tables(
        self,
        number: str,
        fz_type: str,
        table_names: List[str],
    ) -> Optional[ContractLocation]:
        """Identity lookup within one FZ (one UNION ALL round-trip)."""
        query = build_unified_lookup_sql(table_names)
        params = [number] * len(table_names)
        try:
            with self._db.connection.cursor() as cursor:
                cursor.execute(query, tuple(params))
                row = cursor.fetchone()
            if not row:
                return None
            return ContractLocation(
                fz_type=fz_type,
                table_name=str(row[1]),
                record_id=int(row[0]),
                contract_number=number,
            )
        except Exception as exc:
            logger.error(
                f"Ошибка unified lookup контракта {number} ({fz_type}): {exc}"
            )
            try:
                self._db.connection.rollback()
            except Exception:
                pass
            return None

    def _search_pairs(
        self,
        contract_number: str,
        pairs: List[Tuple[str, str]],
    ) -> Optional[ContractLocation]:
        for fz_type, table_name in pairs:
            record_id = self._fetch_id(table_name, contract_number)
            if record_id is not None:
                return ContractLocation(
                    fz_type=fz_type,
                    table_name=table_name,
                    record_id=record_id,
                    contract_number=contract_number,
                )
        return None

    def _search_tables(
        self,
        contract_number: str,
        table_names: List[str],
        fz_type: str,
    ) -> Optional[ContractLocation]:
        for table_name in table_names:
            record_id = self._fetch_id(table_name, contract_number)
            if record_id is not None:
                return ContractLocation(
                    fz_type=fz_type,
                    table_name=table_name,
                    record_id=record_id,
                    contract_number=contract_number,
                )
        return None

    @staticmethod
    def _normalize_number(contract_number: str) -> Optional[str]:
        if not contract_number:
            return None
        number = str(contract_number).strip()
        return number or None

    def _fetch_id(self, table_name: str, contract_number: str) -> Optional[int]:
        try:
            with self._db.connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT id FROM {table_name} WHERE contract_number = %s LIMIT 1",
                    (contract_number,),
                )
                row = cursor.fetchone()
                return int(row[0]) if row else None
        except Exception as exc:
            logger.error(
                f"Ошибка поиска контракта {contract_number} в {table_name}: {exc}"
            )
            try:
                self._db.connection.rollback()
            except Exception:
                pass
            return None
