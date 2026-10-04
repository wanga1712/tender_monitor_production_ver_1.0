"""Targeted: terminal/technical registry tables must never be mutated by RGK.

Regression guarded here: reestr_contract_44_fz_completed has no updated_at
column, so build_batch_update_sql raised UndefinedColumn and killed the
backward parser on every restart.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402

from database_work.registry_tables import lookup_order, tables_for_fz  # noqa: E402
from database_work.rgk_batch_sql import (  # noqa: E402
    ALLOWED_TABLES_44,
    MUTABLE_TABLES_44,
    READ_ONLY_TABLES_44,
    build_batch_update_sql,
    build_registry_lookup_sql,
)

T44 = tables_for_fz("44")


def test_readonly_set_covers_completed_and_unknown():
    assert T44.completed in READ_ONLY_TABLES_44
    assert T44.unknown in READ_ONLY_TABLES_44


def test_lookup_still_covers_completed():
    assert T44.completed in ALLOWED_TABLES_44
    assert T44.completed in build_registry_lookup_sql(T44.completed)


def test_update_sql_rejects_readonly_tables():
    for name in (T44.completed, T44.unknown):
        with pytest.raises(ValueError):
            build_batch_update_sql(name)


def test_update_sql_still_allowed_for_active_tables():
    for name in (T44.main, T44.commission_work, T44.unclear, T44.awarded):
        assert name in MUTABLE_TABLES_44
        assert "updated_at = NOW()" in build_batch_update_sql(name)


def test_lookup_order_stays_terminal_first_and_complete():
    assert lookup_order(T44) == [
        T44.completed, T44.awarded, T44.unclear,
        T44.unknown, T44.commission_work, T44.main,
    ]
