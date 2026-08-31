"""Regression tests for preserving the 223-FZ submission window."""

from xml.etree import ElementTree as ET

from database_work.database_operations import DatabaseOperations
from parsing_xml.xml_parser import XMLParser


class _FakeIdFetcher:
    def get_region_id(self, _region_code):
        return 77

    def get_okpd_id(self, _okpd_code):
        return None


class _RecordingCursor:
    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, values):
        self.calls.append((" ".join(query.split()), tuple(values)))


class _FakeConnection:
    status = 0

    def __init__(self):
        self.recording_cursor = _RecordingCursor()
        self.commits = 0

    def cursor(self):
        return self.recording_cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        raise AssertionError("rollback was not expected")


def test_missing_223_submission_dates_stay_none():
    parser = XMLParser.__new__(XMLParser)
    parser.db_id_fetcher = _FakeIdFetcher()
    root = ET.fromstring(
        "<root><purchaseNoticeData><registrationNumber>223TEST</registrationNumber>"
        "</purchaseNoticeData></root>"
    )

    result = parser._parse_common_contract_data(
        root,
        {
            "contract_number": "purchaseNoticeData/registrationNumber",
            "start_date": "submissionStartDateTime",
            "end_date": "submissionCloseDateTime",
            "initial_price": "initialSum",
        },
        region_code="77",
        okpd_code=None,
        customer_id=10,
        platform_id=20,
        tags_file="required_tags_223_fz.json",
    )

    assert result["contract_number"] == "223TEST"
    assert result["start_date"] is None
    assert result["end_date"] is None
    assert result["initial_price"] == 0


def test_awarded_223_update_cannot_overwrite_submission_dates():
    connection = _FakeConnection()
    operations = DatabaseOperations.__new__(DatabaseOperations)
    operations.db_manager = type("FakeManager", (), {"connection": connection})()

    result = operations.update_commission_work_223_full(
        {
            "contract_number": "223TEST",
            "start_date": "2026-09-10",
            "end_date": "2026-12-20",
            "delivery_start_date": "2026-09-10",
            "delivery_end_date": "2026-12-20",
            "contractor_id": 42,
        }
    )

    assert result is True
    assert connection.commits == 1
    assert len(connection.recording_cursor.calls) == 1
    query, values = connection.recording_cursor.calls[0]
    assert "delivery_start_date = %s" in query
    assert "delivery_end_date = %s" in query
    assert " start_date = %s" not in query
    assert " end_date = %s" not in query
    assert values == (
        "223TEST",
        42,
        "2026-09-10",
        "2026-12-20",
        "223TEST",
    )
