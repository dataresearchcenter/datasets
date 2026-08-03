"""
Extract deals and investors from the Land Matrix JSON API, and the wider legacy
csv export.

The json endpoints carry the structured fields (coded crops, intentions,
statuses, coordinates, investor ids), the legacy export covers the same rows but
roughly 150 columns wide and adds what the list endpoints leave out: contracts,
location names, nature of the deal, prices and data sources. Both describe the
same deals, so their entities merge on the shared ids.

Foreign keys (`country_id`, `loans_currency_id`, currency labels) are resolved
here against the reference endpoints, so that the transform stage stays free of
network access.
"""

import json
from functools import cache
from zipfile import ZipFile

import pandas as pd
from anystore.io import smart_open
from anystore.types import SDict
from investigraph.model.context import SourceContext
from investigraph.types import RecordGenerator

COUNTRIES_URI = "https://landmatrix.org/api/countries/?format=json"
CURRENCIES_URI = "https://landmatrix.org/api/currencies/?format=json"
LEGACY_TABLES = ("deals.csv", "locations.csv", "contracts.csv", "datasources.csv")


@cache
def get_countries() -> dict[int, tuple[str, str]]:
    """Map the Land Matrix country id to its iso code and name."""
    with smart_open(COUNTRIES_URI) as fh:
        data = json.load(fh)
    return {c["id"]: (c["code_alpha2"].lower(), c["name"]) for c in data}


@cache
def get_currencies() -> dict[int, str]:
    with smart_open(CURRENCIES_URI) as fh:
        data = json.load(fh)
    return {c["id"]: c["code"] for c in data}


@cache
def get_currency_codes() -> dict[str, str]:
    """The csv export spells currencies out the way the site does, e.g.
    `US Dollar ($)`, map those back onto their iso code."""
    with smart_open(CURRENCIES_URI) as fh:
        data = json.load(fh)
    return {
        f"{c['name']} ({c['symbol']})" if c["symbol"] else c["name"]: c["code"]
        for c in data
    }


def extract_api(ctx: SourceContext) -> RecordGenerator:
    with ctx.open() as fh:
        data: list[SDict] = json.load(fh)

    countries = get_countries()
    currencies = get_currencies()
    # some involvements name a parent investor that is not part of the public
    # subset, flag them so that the transform doesn't emit a dangling edge
    known = {record["id"] for record in data}
    orphans = 0

    for record in data:
        version = record.get("selected_version") or {}
        country_id = record.get("country_id") or version.get("country_id")
        code, name = countries.get(country_id) or (None, None)
        record["country_code"], record["country_name"] = code, name
        for involvement in version.get("involvements") or []:
            involvement["loans_currency"] = currencies.get(
                involvement.get("loans_currency_id")
            )
            involvement["parent_known"] = involvement.get("parent_investor_id") in known
            orphans += not involvement["parent_known"]
        yield record

    if orphans:
        ctx.log.info("Skipping involvements with a non-public parent", orphans=orphans)


def extract_legacy(ctx: SourceContext) -> RecordGenerator:
    currencies = get_currency_codes()
    with ctx.open() as fh:
        with ZipFile(fh) as archive:
            for table in LEGACY_TABLES:
                with archive.open(table) as fh_table:
                    df = pd.read_csv(
                        fh_table, delimiter=";", dtype=str, low_memory=False
                    ).fillna("")
                for record in df.to_dict("records"):
                    record = {k: v.strip() for k, v in record.items()}
                    record["__table__"] = table
                    if table == "deals.csv":
                        record["purchase_currency"] = currencies.get(
                            record["Purchase price currency"]
                        )
                        record["leasing_currency"] = currencies.get(
                            record["Annual leasing fee currency"]
                        )
                    yield record


def handle(ctx: SourceContext, *args, **kwargs) -> RecordGenerator:
    if ctx.source.name == "legacy":
        yield from extract_legacy(ctx)
    else:
        yield from extract_api(ctx)
