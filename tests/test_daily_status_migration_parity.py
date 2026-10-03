"""Targeted tests: Phase 2 - COUNT / SELECT / INSERT predicate parity.

Run with or without pytest:  python tests/test_daily_status_migration_parity.py
Source-level guarantees (no DB): the canonical engine must expose ONE logical
eligibility predicate shared by COUNT, SELECT candidate ids and INSERT re-check.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

MOD = ROOT / "database_work" / "daily_status_migration.py"
SRC = MOD.read_text(encoding="utf-8", errors="replace")


def test_1_main_to_commission_shared_predicate_used_by_count_select_insert():
    # def + COUNT + SELECT + INSERT
    assert SRC.count("main_to_commission_predicate(") == 4


def test_2_main_to_commission_no_independent_inline_predicate():
    assert SRC.count("end_date <= CURRENT_DATE + INTERVAL '1 day'") == 1


def test_3_unclear_shared_predicate_used_by_count_select_insert():
    assert SRC.count("commission_to_unclear_predicate(") == 4  # def + 3 call sites


def test_4_unclear_no_independent_inline_predicate():
    # old inline commission predicates (COUNT/SELECT) must be gone
    assert SRC.count("AND c.end_date < CURRENT_DATE - INTERVAL '90 days'") == 0
    assert SRC.count("AND end_date < CURRENT_DATE - INTERVAL '90 days'") == 0


def test_5_backfill_guard_present_in_both_predicates():
    assert "BACKFILL_GUARD_START = '2026-03-26'" in SRC
    # the guard must be embedded as a valid SQL literal (not a bare identifier)
    assert "start_date < BACKFILL_GUARD_START" not in SRC
    assert SRC.count("start_date < '{BACKFILL_GUARD_START}'") == 2


def test_6_both_fz_types_use_same_engine():
    assert SRC.count("migrate_from_main_to_commission_work('44')") == 1
    assert SRC.count("migrate_from_main_to_commission_work('223')") == 1
    assert SRC.count("migrate_from_commission_work('44')") == 1
    assert SRC.count("migrate_from_commission_work('223')") == 1


def test_7_predicate_semantics_include_required_conditions():
    import importlib.util

    spec = importlib.util.spec_from_file_location("dsm_pred", MOD)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    main_pred = mod.main_to_commission_predicate("44", "m")
    assert "end_date IS NOT NULL" in main_pred
    assert "end_date <= CURRENT_DATE + INTERVAL '1 day'" in main_pred
    assert "m.start_date < '2026-03-26'" in main_pred
    unclear = mod.commission_to_unclear_predicate("c")
    assert "c.end_date < CURRENT_DATE - INTERVAL '90 days'" in unclear
    assert "c.delivery_start_date IS NULL" in unclear
    assert "c.start_date < '2026-03-26'" in unclear
    unaliased = mod.commission_to_unclear_predicate("")
    assert "c." not in unaliased and "delivery_start_date IS NULL" in unaliased


def test_9_identity_guard_contract_number_not_null():
    import importlib.util

    spec = importlib.util.spec_from_file_location("dsm_pred9", MOD)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert "m.contract_number IS NOT NULL" in mod.main_to_commission_predicate("44", "m")


def test_10_identity_guard_covers_all_status_tables_44():
    import importlib.util

    spec = importlib.util.spec_from_file_location("dsm_pred10", MOD)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    pred = mod.main_to_commission_predicate("44", "m")
    for suffix in ("commission_work", "unknown", "unclear", "awarded", "completed"):
        assert f"reestr_contract_44_fz_{suffix}" in pred
    assert "t.contract_number = m.contract_number" in pred


def test_11_identity_guard_223_set():
    import importlib.util

    spec = importlib.util.spec_from_file_location("dsm_pred11", MOD)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    pred = mod.main_to_commission_predicate("223", "m")
    for suffix in ("commission_work", "unclear", "awarded", "completed"):
        assert f"reestr_contract_223_fz_{suffix}" in pred
    assert "reestr_contract_223_fz_unknown" not in pred


def test_12_no_id_only_dedup_for_transitions():
    assert "id NOT IN (SELECT id FROM" not in SRC
    assert "a.id = c.id" not in SRC
    assert "u.id = c.id" not in SRC


def test_8_no_new_modules_or_engines():
    assert (ROOT / "database_work" / "daily_status_migration.py").exists()
    assert not (ROOT / "database_work" / "daily_status_migration_v2.py").exists()
    assert not (ROOT / "database_work" / "status_transition_engine.py").exists()


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
