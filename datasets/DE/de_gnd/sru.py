import httpx
from typing import Any

from anystore.decorators import anycache, error_handler
from bs4 import BeautifulSoup
from anystore.logging import get_logger
from investigraph.settings import Settings


settings = Settings()
CACHE = settings.cache.to_store()
VOCAB_CONFIG = {"place": "Tg", "profession": "Ts"}

log = get_logger(f"investigraph.datasets.de_gnd.{__name__}")


def get_value(record, datafield: str) -> str:
    try:
        key = record.find("datafield", {"tag": datafield})
        value = key.find("subfield", {"code": "a"})
        return value.get_text()
    except Exception:
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


@anycache(store=CACHE)
@error_handler(max_retries=10)
def get_title_from_sru_request(gndId: str, vocab_type: str) -> str:
    value = ""
    sru_url = "https://services.dnb.de/sru/authorities"
    response = httpx.get(sru_url, params=get_params(gndId, vocab_type))
    if response is not None:
        soup = BeautifulSoup(response.text, "xml")
        records = soup.find_all("record")
        for record in records:
            recordId = get_value(record, "024")
            if gndId == recordId:
                if vocab_type == "place":
                    value = get_value(record, "151")
                else:
                    value = get_value(record, "150")
    if not value:
        log.warning(f"GND Id: {gndId} not found")
    return value
