"""
Карта таблиц реестра контрактов 44/223.

Комментарии на русском. Без бизнес-логики — только имена и порядок поиска.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class RegistryTables:
    """Имена таблиц одного контура ФЗ."""

    fz_type: str
    main: str
    commission_work: str
    unclear: str
    awarded: str
    completed: Optional[str] = None
    unknown: Optional[str] = None


TABLES_44 = RegistryTables(
    fz_type="44",
    main="reestr_contract_44_fz",
    commission_work="reestr_contract_44_fz_commission_work",
    unclear="reestr_contract_44_fz_unclear",
    awarded="reestr_contract_44_fz_awarded",
    completed="reestr_contract_44_fz_completed",
    unknown="reestr_contract_44_fz_unknown",
)

TABLES_223 = RegistryTables(
    fz_type="223",
    main="reestr_contract_223_fz",
    commission_work="reestr_contract_223_fz_commission_work",
    unclear="reestr_contract_223_fz_unclear",
    awarded="reestr_contract_223_fz_awarded",
    completed="reestr_contract_223_fz_completed",
    unknown=None,
)


def lookup_order(tables: RegistryTables) -> List[str]:
    """Identity lookup order — terminal-first precedence.

    COMPLETED > AWARDED > UNCLEAR > UNKNOWN > COMMISSION_WORK > MAIN.

    The SET of status tables is independent of the incoming ``end_date``: a
    procurement that already lives in a terminal table must never be re-inserted
    into ``main`` just because the incoming XML carries a future deadline.

    ``completed`` participates in identity lookup (READ/FIND); the parser never
    mutates it (see xml_parser: only main/commission_work are updated on match).
    This ordering is also the conflict resolution when a contract number exists
    in several historical status tables at once.
    """
    ordered: List[str] = []
    if tables.completed:
        ordered.append(tables.completed)
    ordered.append(tables.awarded)
    ordered.append(tables.unclear)
    if tables.unknown:
        ordered.append(tables.unknown)
    ordered.append(tables.commission_work)
    ordered.append(tables.main)
    return ordered


def tables_for_fz(fz_type: str) -> RegistryTables:
    """Возвращает карту таблиц для '44' или '223'."""
    if fz_type == "44":
        return TABLES_44
    if fz_type == "223":
        return TABLES_223
    raise ValueError(f"Неизвестный тип ФЗ: {fz_type}")


def all_lookup_tables() -> List[Tuple[str, str]]:
    """
    Плоский список (fz_type, table_name) для глобального поиска номера.
    Порядок внутри ФЗ: terminal-first (completed → awarded → unclear →
    unknown → commission_work → main), независимо от end_date.
    """
    result: List[Tuple[str, str]] = []
    for tables in (TABLES_44, TABLES_223):
        for name in lookup_order(tables):
            result.append((tables.fz_type, name))
    return result


# Поля, которые разрешено обновлять из RGK/recouped XML.
ALLOWED_UPDATE_FIELDS = (
    "contractor_id",
    "delivery_start_date",
    "delivery_end_date",
    "final_price",
    "initial_price",
    "guarantee_amount",
    "okpd_id",
    "auction_name",
    "region_id",
)

# Canonical registry columns accepted from parser output on INSERT.  Parser
# helper/provenance keys must be mapped before this boundary, never promoted
# to SQL identifiers by dict expansion.
COMMON_INSERT_FIELDS = frozenset(
    {
        "contract_number", "tender_link", "start_date", "end_date",
        "delivery_start_date", "delivery_end_date", "auction_name",
        "initial_price", "final_price", "guarantee_amount", "customer_id",
        "contractor_id", "trading_platform_id", "okpd_id",
        "delivery_region", "delivery_address", "region_id", "status_id",
    }
)

INSERT_FIELDS_BY_FZ = {
    "44": COMMON_INSERT_FIELDS | {"customer", "warranty_size"},
    "223": COMMON_INSERT_FIELDS | {"placer", "placer_inn"},
}


def persistence_payload(fz_type: str, parser_fields: dict) -> dict:
    """Map parser output to the explicit canonical registry INSERT contract."""
    allowed = INSERT_FIELDS_BY_FZ[fz_type]
    return {key: value for key, value in parser_fields.items() if key in allowed}
