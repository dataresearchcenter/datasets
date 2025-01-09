#!/usr/bin/env python
"""
Get all german departments (Ministerien) from the FragDenStaat api
"""

import csv
import sys
from typing import Any, Generator, Literal, Mapping, TypeAlias
from urllib.parse import urlparse

import requests

TEMPLATE = "documents"
COUNTRY = "de"
API_URL = "https://fragdenstaat.de/api/v1/publicbody/?classification_id=118"
FIELDS = (
    "id",
    "foreign_id",
    "name",
    "abbrev",
    "description",
    "url",
    "domain",
    "email",
    "contact",
    "address",
    "wikidata_item",
    "jurisdiction",
    "template",
)
JURISDICTIONS = {
    "Bund": "de",
    "Baden-Württemberg": "de_bw",
    "Bayern": "de_by",
    "Berlin": "de_be",
    "Brandenburg": "de_bb",
    "Bremen": "de_hb",
    "Hamburg": "de_hh",
    "Hessen": "de_he",
    "Mecklenburg-Vorpommern": "de_mv",
    "Niedersachsen": "de_ni",
    "Nordrhein-Westfalen": "de_nw",
    "Rheinland-Pfalz": "de_rp",
    "Saarland": "de_sl",
    "Sachsen": "de_sn",
    "Sachsen-Anhalt": "de_st",
    "Schleswig-Holstein": "de_sh",
    "Thüringen": "de_th",
}


Row: TypeAlias = Mapping[Literal[FIELDS], str]


def get_abbrev(data: dict[str, Any]) -> str:
    abbrev = list(sorted(data["other_names"].split(","), key=len))[-1].lower().strip()
    if 1 < len(abbrev) < 7 and abbrev != data["jurisdiction"]["name"].lower():
        return abbrev
    email_host = "http://" + data["email"].split("@", 1)[1]
    hostname = urlparse(email_host).hostname
    if hostname:
        abbrev = hostname.split(".")[0]
        if abbrev == "www":
            abbrev = hostname.split(".")[1]
    abbrev = abbrev.strip()
    if 1 < len(abbrev) < 7 and abbrev != data["jurisdiction"]["name"].lower():
        return abbrev
    hostname = urlparse(data["url"]).hostname
    if hostname:
        abbrev = hostname.split(".")[0]
        if abbrev == "www":
            abbrev = hostname.split(".")[1]
    return abbrev.strip()


def to_row(data: dict[str, Any]) -> Row:
    row = {k: data[k] for k in FIELDS if k in data}
    jurisdiction = data["jurisdiction"]["name"]
    abbrev = get_abbrev(data)
    return {
        **row,
        "country": COUNTRY,
        "jurisdiction": jurisdiction,
        "abbrev": abbrev,
        "domain": "https://" + urlparse(data["url"]).netloc,
        "foreign_id": JURISDICTIONS[jurisdiction] + "_" + abbrev,
        "template": TEMPLATE,
    }


def parse(url: str) -> Generator[Row, None, None]:
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()
    for record in data["objects"]:
        if record["jurisdiction"]["name"] in JURISDICTIONS:
            yield to_row(record)
    if data["meta"]["next"]:
        yield from parse(data["meta"]["next"])


if __name__ == "__main__":
    writer = csv.DictWriter(sys.stdout, FIELDS)
    writer.writeheader()
    for row in parse(API_URL):
        writer.writerow(row)
