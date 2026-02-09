"""
Клиент для работы с НСИ ЕИС (getNsiRequest).

Первая цель: вытащить справочник ``nsiOpenDataList`` и получить коды регионов ``regionCode``,
не трогая основной оркестратор.

Запуск теста будет через отдельный скрипт в ``tests/test_nsi_regions.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Optional
import time
import xml.etree.ElementTree as ET

import requests

from utils.logger_config import get_logger
from secondary_functions import load_config, load_token


logger = get_logger()


def _now_utc_iso() -> str:
    """Текущее время в UTC в формате ISO 8601."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _strip_ns(tag: str) -> str:
    """Удаляет namespace из имени тега: '{ns}tag' -> 'tag'."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


@dataclass
class NsiRequestConfig:
    url: str
    token: str


class EisNsiClient:
    """
    Простой клиент для вызова getNsiRequest.

    Сейчас используется только для справочника nsiOpenDataList,
    чтобы вытащить regionCode и посмотреть, что реально отдаёт ЕИС.
    """

    def __init__(self, config_path: str = "config.ini", url_override: Optional[str] = None) -> None:
        config = load_config(config_path)
        if not config:
            raise ValueError("Не удалось загрузить config.ini для NsiClient")

        token = load_token(config)
        if not token:
            raise ValueError("Токен не найден для NsiClient (load_token вернул None)")

        # URL можно переопределить через конфиг или аргумент.
        # Для НСИ по альбому (для физ. лиц) используется тот же сервис getDocsIP.
        # Используем тот же endpoint, что и основной EISRequester.
        default_url = "http://localhost:8080/eis-integration/services/getDocsIP"
        config_url = config.get("eis", "nsi_url", fallback=default_url)
        self._cfg = NsiRequestConfig(
            url=url_override or config_url,
            token=token,
        )

    # --- SOAP-строитель и отправка -------------------------------------------------

    def _build_get_nsi_request(self, nsi_code44: str, nsi_kind: str = "all") -> str:
        """
        Формирует SOAP-запрос getNsiRequest по шаблону из альбома ТФФ.
        """
        request_id = _now_utc_iso().replace(":", "").replace("-", "").replace("T", "")  # просто уникальный id
        create_dt = _now_utc_iso()

        # В альбоме в примере xmlns:ws указывает на URL сервиса getDocsIP.
        # Используем тот же неймспейс-URI – этого достаточно для SOAP.
        soap = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:ws="https://int.zakupki.gov.ru/eis-integration/services/getDocsIP">
  <soapenv:Header>
    <individualPerson_token>{self._cfg.token}</individualPerson_token>
  </soapenv:Header>
  <soapenv:Body>
    <ws:getNsiRequest>
      <index>
        <id>{request_id}</id>
        <createDateTime>{create_dt}</createDateTime>
        <mode>PROD</mode>
      </index>
      <selectionParams>
        <nsiCode44>{nsi_code44}</nsiCode44>
        <nsiKind>{nsi_kind}</nsiKind>
      </selectionParams>
    </ws:getNsiRequest>
  </soapenv:Body>
</soapenv:Envelope>
"""
        return soap

    def _post_soap(self, soap_xml: str) -> str:
        """
        Отправка SOAP-запроса.

        В отладочном режиме делаем одну попытку с понятным логированием,
        чтобы не «висеть» на долгих ретраях.
        """
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
        }

        logger.info(f"[NSI] Отправка getNsiRequest на {self._cfg.url}")
        print(f"[NSI] POST -> {self._cfg.url}")

        try:
            resp = requests.post(
                self._cfg.url,
                data=soap_xml.encode("utf-8"),
                headers=headers,
                verify=False,
                timeout=60,
            )
            print(f"[NSI] HTTP статус: {resp.status_code}")
            logger.info(f"[NSI] Ответ getNsiRequest: HTTP {resp.status_code}")
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.RequestException as e:
            print(f"[NSI] Ошибка HTTP: {e}")
            logger.error(f"[NSI] Ошибка HTTP при вызове getNsiRequest: {e}", exc_info=True)
            raise

    # --- Разбор nsiOpenDataList ----------------------------------------------------

    def get_open_data_list(self) -> List[Dict[str, str]]:
        """
        ВРЕМЕННО: Получает ответ getNsiRequest для nsiAllList и выводит структуру ответа.

        В идеале здесь должен быть вызов конкретного НСИ с regionCode, но для начала
        мы дергаем nsiAllList, как в примере из альбома, чтобы увидеть реальную структуру.
        """
        # Пока используем nsiAllList, как в официальном примере.
        soap = self._build_get_nsi_request("nsiAllList", "all")
        raw_xml = self._post_soap(soap)

        # ВРЕМЕННАЯ отладка: показываем в консоли начало ответа,
        # чтобы увидеть реальную структуру XML.
        print("\n================= RAW NSI RESPONSE (начало) =================")
        snippet = raw_xml[:4000]
        print(snippet)
        if len(raw_xml) > len(snippet):
            print(f"\n... (обрезано, полная длина ответа: {len(raw_xml)} символов) ...")
        print("=============================================================\n")

        try:
            root = ET.fromstring(raw_xml)
        except ET.ParseError as e:
            logger.error(f"[NSI] Ошибка парсинга XML ответа nsiOpenDataList: {e}", exc_info=True)
            raise

        items: List[Dict[str, str]] = []

        # Ищем все элементы, внутри которых есть datasetCode/datasetName – это и будут записи справочника
        for elem in root.iter():
            tags = {_strip_ns(child.tag) for child in list(elem)}
            if not {"datasetCode", "datasetName"}.issubset(tags):
                continue

            record: Dict[str, str] = {}
            for child in elem:
                key = _strip_ns(child.tag)
                text = (child.text or "").strip()
                if text:
                    record[key] = text

            if record:
                items.append(record)

        logger.info(f"[NSI] Получено записей nsiOpenDataList: {len(items)}")
        return items

    def get_region_codes_from_open_data(self) -> List[str]:
        """
        Выделяет уникальные regionCode из nsiOpenDataList.
        """
        items = self.get_open_data_list()
        regions = sorted({item.get("regionCode") for item in items if item.get("regionCode")})
        logger.info(f"[NSI] Уникальных regionCode: {len(regions)}")
        return regions


