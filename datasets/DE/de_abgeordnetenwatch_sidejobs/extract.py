import logging
import os
import time
from typing import Iterable

from banal import as_bool, ensure_dict
from investigraph import Context
from investigraph.logic import fetch
from investigraph.types import Record, RecordGenerator
from investigraph.util import dict_merge

URL = "https://www.abgeordnetenwatch.de/api/v2/"
TESTING = as_bool(os.environ.get("TESTING"))
MAX_ERRORS = 5
CHUNK_SIZE = 1000

log = logging.getLogger(__name__)


def make_request(*args, **kwargs) -> Record:
    backoff = 5
    errors = 0
    res = None
    while errors < MAX_ERRORS:
        try:
            res = fetch.get(*args, **kwargs)
            return res.json()
        except Exception as e:
            log.warning(
                "API %s [%s], backoff %d sec."
                % (args[0], res.status_code if res is not None else e, backoff)
            )
            errors += 1
            time.sleep(backoff)
            backoff = backoff * 2  # gosh
    raise Exception("Service unavailable for url `{args[0]}`")


def fetch_iter(
    url: str,
    range_start: int = 0,
    chunk_size: int = CHUNK_SIZE,
) -> RecordGenerator:
    params = {"range_start": range_start, "range_end": chunk_size}
    res = make_request(url, params=params)
    yield from res["data"]
    if TESTING:
        return
    if res["meta"]["result"]["total"] > range_start + chunk_size:
        yield from fetch_iter(url, range_start + chunk_size)


def fetch_related_items(endpoint: str, ids: Iterable[int], **params) -> RecordGenerator:
    ids = list(ids)
    for chunk in [ids[i : i + 100] for i in range(0, len(ids), 100)]:
        params = {**params, "id[in]": f'[{",".join(str(i) for i in chunk)}]'}
        res = make_request(URL + endpoint, params=params)
        yield from res["data"]


def handle(ctx: Context, *args, **kwargs):
    sidejobs: dict[int, Record] = {}
    mandates: dict[int, Record] = {}
    politicians: dict[int, Record] = {}
    organizations: dict[int, Record] = {}

    ctx.log.info("Fetching sidejobs ...")
    for ix, sidejob in enumerate(fetch_iter(URL + "sidejobs"), 1):
        sidejobs[sidejob["id"]] = sidejob
        for mandate in sidejob["mandates"]:
            mandates[mandate["id"]] = mandate
        organization = ensure_dict(sidejob.get("sidejob_organization"))
        if organization:
            organizations[organization["id"]] = organization
        if TESTING and ix == 100:
            break
    ctx.log.info(f"Fetched `{len(sidejobs)}` sidejobs.")

    ctx.log.info(f"Fetching `{len(mandates)}` related mandates ...")
    for ix, mandate in enumerate(
        fetch_related_items("candidacies-mandates", mandates.keys(), current_on="all"),
        1,
    ):
        mandates[mandate["id"]] = dict_merge(mandates[mandate["id"]], mandate)
        politicians[mandate["politician"]["id"]] = mandate["politician"]
        if TESTING and ix == 100:
            break

    ctx.log.info(f"Fetching `{len(politicians)}` related politicians ...")
    for ix, politician in enumerate(
        fetch_related_items("politicians", politicians.keys()), 1
    ):
        politicians[politician["id"]] = dict_merge(
            politicians[politician["id"]], politician
        )
        if TESTING and ix == 100:
            break

    ctx.log.info(f"Fetching `{len(organizations)}` related organizations ...")
    for ix, organization in enumerate(
        fetch_related_items("sidejob-organizations", organizations.keys()), 1
    ):
        organizations[organization["id"]] = dict_merge(
            organizations[organization["id"]], organization
        )
        if TESTING and ix == 100:
            break

    yield from politicians.values()
    yield from organizations.values()
    # yield from mandates.values()
    for sidejob in sidejobs.values():
        sidejob["mandates"] = [
            dict_merge(m, mandates[m["id"]]) for m in sidejob["mandates"]
        ]
        yield sidejob
