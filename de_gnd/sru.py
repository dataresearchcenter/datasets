import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from typing import Any


def get_value(record, datafield: str):
    try:
        key = record.find("datafield", {"tag": datafield})
        value = key.find("subfield", {"code": "a"})
        return value.get_text()
    except:
        return ''


def get_params(gndId: str) -> dict[str, Any]:
    query = f"WOE={gndId} and BBG=Ts*"
    params = {
        "version": "1.1",
        "operation": "searchRetrieve",
        "query": query,
        "maximumRecords": 10,
        "recordSchema": "MARC21-xml",
    }
    return params


def request_data(gndId: str) -> str:
    sru_url = "https://services.dnb.de/sru/authorities"
    try:
        response = requests.get(sru_url, params=get_params(gndId))
        response.raise_for_status()
        if response.status_code == 200:
            return response.text
    except requests.exceptions.RequestException as e:
        print(f"API Request Error: {e}")


def process_xml(xml_text, gndId: str):
    root = ET.fromstring(xml_text)
    soup = BeautifulSoup(xml_text, "xml")
    records = soup.find_all("record")
    for record in records:
        # verify the record based on the gndId 
        recordId = get_value(record, "024")
        if gndId == recordId:
           title = get_value(record, "150")
           return title
    print(f"GND Id: {gndId} not found")
    return ""


def get_title_from_sru_request(gndId: str) -> str:
    response = request_data(gndId)
    return process_xml(response, gndId)
