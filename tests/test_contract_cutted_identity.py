"""Regression: identity extraction for EIS contract XMLs (P0 S7 ingest).

Fixtures mirror REAL production samples captured on S7 (2026-10-06):

* 223-FZ ``contractCutted`` (root children header/extendedType/body) ??? the format
  that failed with "???? ???????????? ?????????? ??????????????????". The DB identity is the purchase
  notice number (reestr_contract_223_fz.contract_number, 11/19 digits).
* 44-FZ ``contract_*`` export (root child contract) ??? the legacy format that must
  keep working.

The contract registry number (contractRegNumber/registrationNumber) is NOT used
as identity: it does not match reestr_contract_* contract_number and must not be
invented as a fallback.
"""
import xml.etree.ElementTree as ET

from parsing_xml.okpd_parser import (
    extract_contract_number,
    extract_contract_number_from_filename,
)
from parsing_xml.rgk_record import extract_contract_number as rgk_extract
from parsing_xml.xml_parser import XMLParser


NEW_223_CUTTED = """<?xml version="1.0" encoding="UTF-8"?>
<ns1:contractCutted xmlns:ns1="http://zakupki.gov.ru/223fz/contract/1">
  <header xmlns="http://zakupki.gov.ru/223fz/types/1">
    <guid>e5f54aa1-6c86-46e5-a6f7-467702a4e877</guid>
    <createDateTime>2026-05-20T11:25:29</createDateTime>
  </header>
  <ns2:extendedType xmlns:ns2="http://zakupki.gov.ru/223fz/contract/1">
    <ns2:contentUid>019E447CB0367F2C896933B5B048814D</ns2:contentUid>
  </ns2:extendedType>
  <ns2:body xmlns:ns2="http://zakupki.gov.ru/223fz/contract/1">
    <ns2:item>
      <ns2:contractData>
        <ns2:registrationNumber>52312159262260000230003</ns2:registrationNumber>
        <ns2:contractRegNumber>52312159262260000230000</ns2:contractRegNumber>
        <ns2:purchaseNoticeNumber>32615792098</ns2:purchaseNoticeNumber>
      </ns2:contractData>
    </ns2:item>
  </ns2:body>
</ns1:contractCutted>
"""

OLD_44_EXPORT = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ns3:export xmlns:ns3="http://zakupki.gov.ru/oos/export/1">
  <ns3:contract schemeVersion="14.3">
    <id>96175684</id>
    <foundation>
      <fcsOrder>
        <order>
          <notificationNumber>0111300060624000107</notificationNumber>
          <lotNumber>1</lotNumber>
        </order>
      </fcsOrder>
    </foundation>
  </ns3:contract>
</ns3:export>
"""

MALFORMED = "<root><header/><body><unknownNumber>abc</unknownNumber></body></root>"


def _root(xml):
    return ET.fromstring(XMLParser.remove_namespaces(xml))


def test_new_223_contractcutted_uses_purchase_notice_number():
    root = _root(NEW_223_CUTTED)
    assert extract_contract_number(root) == "32615792098"
    assert rgk_extract(root) == "32615792098"


def test_contract_registry_number_not_used_as_identity():
    value = extract_contract_number(_root(NEW_223_CUTTED))
    assert value not in ("52312159262260000230003", "52312159262260000230000")


def test_old_44_export_still_parses():
    root = _root(OLD_44_EXPORT)
    assert extract_contract_number(root) == "0111300060624000107"
    assert rgk_extract(root) == "0111300060624000107"


def test_malformed_or_unknown_returns_none_safely():
    root = _root(MALFORMED)
    assert extract_contract_number(root) is None
    assert rgk_extract(root) is None


def test_filename_fallback_only_for_explicitly_supported_prefix():
    assert extract_contract_number_from_filename(
        "contract_3233701852026000005_0_019E444883A9715F9A41228788A6FCCD.xml"
    ) == "3233701852026000005"
    assert extract_contract_number_from_filename(
        "contractCutted_52310067376260001200000_2_019E446F39E0709DB5F90FF962E11497.xml"
    ) is None

