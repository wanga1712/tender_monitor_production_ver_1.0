import json
import os
from pathlib import Path
import xml.etree.ElementTree as ET

from parsing_xml.xml_parser import XMLParser
from parsing_xml.okpd_parser import extract_okpd_codes


ROOT = Path(os.getenv("TAGS_ROOT", "/opt/tendermonitor/required_tags"))


def parser_without_database():
    return XMLParser.__new__(XMLParser)


def test_known_document_types_select_exact_profiles():
    parser = parser_without_database()
    root_44 = ET.fromstring(
        '<export xmlns="urn:test"><epNotificationEOK2020 /></export>'
    )
    root_aesmbo = ET.fromstring(
        '<purchaseNoticeAESMBO xmlns="urn:test" />'
    )

    selected_44 = parser._select_schema_mapping(
        str(ROOT / "required_tags_44_fz.json"), root_44
    )
    selected_aesmbo = parser._select_schema_mapping(
        str(ROOT / "required_tags_223_fz.json"), root_aesmbo
    )

    assert selected_44.endswith("required_tags_44_fz_epNotificationEOK2020.json")
    assert selected_aesmbo.endswith("required_tags_223_fz_purchaseNoticeAESMBO.json")


def test_unknown_document_types_keep_legacy_mapping():
    parser = parser_without_database()
    root = ET.fromstring('<export xmlns="urn:test"><unknownNotice /></export>')
    default = str(ROOT / "required_tags_44_fz.json")

    assert parser._detect_document_type(root) is None
    assert parser._select_schema_mapping(default, root) == default


def test_specific_field_semantics_and_legacy_guard():
    profile_44 = json.loads(
        (ROOT / "required_tags_44_fz_epNotificationEOK2020.json").read_text(
            encoding="utf-8"
        )
    )
    profile_aesmbo = json.loads(
        (ROOT / "required_tags_223_fz_purchaseNoticeAESMBO.json").read_text(
            encoding="utf-8"
        )
    )
    legacy_44 = json.loads(
        (ROOT / "required_tags_44_fz.json").read_text(encoding="utf-8")
    )
    legacy_223 = json.loads(
        (ROOT / "required_tags_223_fz.json").read_text(encoding="utf-8")
    )

    assert profile_44["reestr_contract"]["start_date"] == (
        "commonInfo/plannedPublishDate"
    )
    assert profile_aesmbo["reestr_contract"]["start_date"].endswith(
        "publicationDateTime"
    )
    assert profile_aesmbo["reestr_contract"]["end_date"].endswith(
        "submissionCloseDateTime"
    )
    assert profile_aesmbo["reestr_contract"]["initial_price"].endswith("initialSum")
    assert "applSubmisionStartDate" not in profile_aesmbo["reestr_contract"].values()
    assert "deliveryStartDateTime" not in profile_aesmbo["reestr_contract"].values()
    assert legacy_44["reestr_contract"]["start_date"] == "collectingInfo/startDT"
    assert legacy_223["reestr_contract"]["start_date"] == "submissionStartDateTime"


def test_okpd_extraction_preserves_all_source_ordered_codes():
    root = ET.fromstring(
        """
        <export>
          <purchaseObject><OKPD2><OKPDCode>01.01</OKPDCode></OKPD2></purchaseObject>
          <lotItems><lotItem><okpd2><code>02.02</code></okpd2></lotItem></lotItems>
          <purchaseObject><OKPD2><OKPDCode>01.01</OKPDCode></OKPD2></purchaseObject>
        </export>
        """
    )
    assert extract_okpd_codes(root) == ["01.01", "02.02"]
