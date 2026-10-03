"""Targeted tests: PHASE 1 - S7 identity lookup must not resurrect terminal rows.

Run with or without pytest:  python tests/test_contract_identity_lookup.py
No DB needed: a fake DB manager replays the UNION ALL terminal-first lookup.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database_work.contract_registry_locator import ContractRegistryLocator  # noqa: E402
from database_work.registry_tables import (  # noqa: E402
    TABLES_44,
    TABLES_223,
    lookup_order,
)

M44 = "reestr_contract_44_fz"
C44 = "reestr_contract_44_fz_commission_work"
U44 = "reestr_contract_44_fz_unclear"
A44 = "reestr_contract_44_fz_awarded"
Z44 = "reestr_contract_44_fz_completed"
K44 = "reestr_contract_44_fz_unknown"
M223 = "reestr_contract_223_fz"
C223 = "reestr_contract_223_fz_commission_work"
U223 = "reestr_contract_223_fz_unclear"
A223 = "reestr_contract_223_fz_awarded"
Z223 = "reestr_contract_223_fz_completed"

FUTURE = "2030-01-01"
PAST = "2020-01-01"
N = "0171200001926000919"


class _Cursor:
    def __init__(self, store):
        self.store = store
        self.result = None

    def execute(self, query, params):
        number = (params or ("",))[0]
        tables = re.findall(r"FROM\s+(\w+)\s+WHERE\s+contract_number", query)
        fz = re.findall(r"'(\d+)'::text AS fz_type", query)
        self.result = None
        for i, table in enumerate(tables):
            rec = self.store.get(table, {})
            if number in rec:
                self.result = (rec[number], table, fz[i] if i < len(fz) else "44")
                return

    def fetchone(self):
        return self.result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Conn:
    def __init__(self, store):
        self.store = store

    def cursor(self):
        return _Cursor(self.store)

    def rollback(self):
        pass


class _DB:
    def __init__(self, store):
        self.connection = _Conn(store)


def _locator(store):
    return ContractRegistryLocator(_DB(store))


def _found(store, fz, number, end_date=None):
    loc = _locator(store).find_in_fz(fz, number, end_date=end_date)
    return loc.table_name if loc else None


def test_1_44_main_only():
    assert _found({M44: {N: 1}}, "44", N) == M44


def test_2_44_commission_only():
    assert _found({C44: {N: 2}}, "44", N) == C44


def test_3_44_unclear_only():
    assert _found({U44: {N: 3}}, "44", N) == U44


def test_4_44_awarded_only():
    assert _found({A44: {N: 4}}, "44", N) == A44


def test_5_44_completed_only():
    assert _found({Z44: {N: 5}}, "44", N) == Z44


def test_6_44_unknown_only():
    assert _found({K44: {N: 6}}, "44", N) == K44


def test_7_223_main_only():
    assert _found({M223: {N: 7}}, "223", N) == M223


def test_8_223_commission_only():
    assert _found({C223: {N: 8}}, "223", N) == C223


def test_9_223_unclear_only():
    assert _found({U223: {N: 9}}, "223", N) == U223


def test_10_223_awarded_only():
    assert _found({A223: {N: 10}}, "223", N) == A223


def test_11_223_completed_only():
    assert _found({Z223: {N: 11}}, "223", N) == Z223


def test_12_future_end_date_awarded():
    assert _found({A44: {N: 12}}, "44", N, end_date=FUTURE) == A44


def test_13_future_end_date_unclear():
    assert _found({U44: {N: 13}}, "44", N, end_date=FUTURE) == U44


def test_14_future_end_date_completed():
    assert _found({Z44: {N: 14}}, "44", N, end_date=FUTURE) == Z44


def test_15_past_end_date_main():
    assert _found({M44: {N: 15}}, "44", N, end_date=PAST) == M44


def test_16_main_and_completed_pick_completed():
    assert _found({M44: {N: 160}, Z44: {N: 161}}, "44", N, end_date=FUTURE) == Z44


def test_17_main_and_awarded_pick_awarded():
    assert _found({M44: {N: 170}, A44: {N: 171}}, "44", N, end_date=FUTURE) == A44


def test_18_main_and_unclear_pick_unclear():
    assert _found({M44: {N: 180}, U44: {N: 181}}, "44", N, end_date=FUTURE) == U44


def test_19_absent_everywhere_returns_none():
    assert _locator({}).find_by_number(N) is None


def test_20_completed_is_not_mutable_source():
    loc = _locator({Z44: {N: 200}}).find_in_fz("44", N)
    assert loc is not None and loc.table_name == Z44
    assert loc.is_promotable_source is False
    assert (loc is None) is False  # parser inserts only when locator returns None


def test_lookup_order_is_terminal_first_and_complete():
    assert lookup_order(TABLES_44) == [Z44, A44, U44, K44, C44, M44]
    assert lookup_order(TABLES_223) == [Z223, A223, U223, C223, M223]


def test_lookup_tables_independent_of_end_date():
    src = (
        Path(__file__).resolve().parents[1]
        / "database_work"
        / "contract_registry_locator.py"
    ).read_text(encoding="utf-8", errors="replace")
    assert "is_active_tender" not in src
    assert "active_lookup_tables" not in src


def _run_all():
    tests = [
        (k, v) for k, v in sorted(globals().items())
        if k.startswith("test_") and callable(v)
    ]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS {name}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {name}: {exc!r}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(_run_all())
