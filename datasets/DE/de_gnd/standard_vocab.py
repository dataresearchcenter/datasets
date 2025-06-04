from functools import cache
import requests

from bs4 import BeautifulSoup


@cache
def request_standard_vocab() -> str:
    url = "https://d-nb.info/standards/vocab/gnd/gnd-sc.html"
    try:
        response = requests.get(url)
        response.raise_for_status()
        if response.status_code == 200:
            return response.text
    except requests.exceptions.RequestException as e:
        print("Request error:", e)
        return ""


def process_html(html: str):
    soup = BeautifulSoup(html, "html")
    return soup.find_all("div", class_="card-header")


def build_vocab(records) -> dict[str:str]:
    vocab = {}
    for record in records:
        title = record.find("h4").text
        gnd_id = record.find("small", class_="text-muted").text
        vocab.update({gnd_id: title})
    return vocab


@cache
def get_title_from_standard_vocab(gnd_id: str) -> str:
    html = request_standard_vocab()
    records = process_html(html)
    vocab = build_vocab(records)
    try:
        return vocab[gnd_id]
    except KeyError:
        print(f"{gnd_id} not in GND standard vocab.")
        return gnd_id
