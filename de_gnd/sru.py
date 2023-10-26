import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from typing import Any


VOCAB_CONFIG = {"place": "Tg", "profession": "Ts"}


def get_value(record, datafield: str):
    try:
        key = record.find("datafield", {"tag": datafield})
        value = key.find("subfield", {"code": "a"})
        return value.get_text()
    except:
        return ""


def get_params(gndId: str, vocab_type: str) -> dict[str, Any]:
    query = f"WOE={gndId} and BBG={vocab_type}*"
    params = {
        "version": "1.1",
        "operation": "searchRetrieve",
        "query": query,
        "maximumRecords": 10,
        "recordSchema": "MARC21-xml",
    }
    return params


def request_data(gndId: str, vocab_type: str) -> str:
    sru_url = "https://services.dnb.de/sru/authorities"
    try:
        response = requests.get(sru_url, params=get_params(gndId, vocab_type))
        response.raise_for_status()
        if response.status_code == 200:
            return response.text
    except requests.exceptions.RequestException as e:
        print(f"API Request Error: {e}")


def process_xml(xml_text):
    root = ET.fromstring(xml_text)
    soup = BeautifulSoup(xml_text, "xml")
    records = soup.find_all("record")
    return records


def extract_title(records, gndId: str, vocab_type: str) -> str:
    for record in records:
        recordId = get_value(record, "024")
        if gndId == recordId:
            if vocab_type == "place":
                return get_value(record, "151")
            else:
                return get_value(record, "150")
    print(f"GND Id: {gndId} not found")
    return ""


def get_title_from_sru_request(gndId: str, vocab_type: str) -> str:
    response = request_data(gndId, VOCAB_CONFIG[vocab_type])
    records = process_xml(response)
    return extract_title(records, gndId, vocab_type)
