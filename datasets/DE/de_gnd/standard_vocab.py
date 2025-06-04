import httpx
from anystore import anycache
from bs4 import BeautifulSoup
from anystore.logging import get_logger
from investigraph.settings import Settings

settings = Settings()
CACHE = settings.cache.to_store()

log = get_logger(f"investigraph.datasets.de_gnd.{__name__}")


@anycache(store=CACHE, serialization_mode="json")
def get_standard_vocab() -> dict[str, str]:
    url = "https://d-nb.info/standards/vocab/gnd/gnd-sc.html"
    res = httpx.get(url)
    soup = BeautifulSoup(res.text, "html")
    records = soup.find_all("div", class_="card-header")
    vocab = {}
    for record in records:
        title = record.find("h4").text
        gnd_id = record.find("small", class_="text-muted").text
        vocab.update({gnd_id: title})
    return vocab


# @anycache(store=CACHE)
def get_title_from_standard_vocab(gnd_id: str) -> str:
    vocab = get_standard_vocab()
    try:
        return vocab[gnd_id]
    except KeyError:
        log.warning(f"{gnd_id} not in GND standard vocab.")
        return gnd_id
