import logging
import os
import time

from banal import as_bool
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
    url: str, range_start: int = 0, chunk_size: int = CHUNK_SIZE, **params
) -> RecordGenerator:
    params = {**params, "range_start": range_start, "range_end": chunk_size}
    res = make_request(url, params=params)
    yield from res["data"]
    if TESTING:
        return
    if res["meta"]["result"]["total"] > range_start + chunk_size:
        params.pop("range_start")
        params.pop("range_end")
        yield from fetch_iter(url, range_start + chunk_size, **params)


def handle(ctx: Context, *args, **kwargs):

    parliaments: dict[int, Record] = {
        i["id"]: i for i in fetch_iter(URL + "parliaments")
    }
    parliament_periods: dict[int, Record] = {
        i["id"]: i for i in fetch_iter(URL + "parliament-periods")
    }

    ix = 0
    ctx.log.info("Fetching politicians ...")
    for ix, item in enumerate(fetch_iter(URL + "politicians"), 1):
        yield item
        if TESTING and ix == 1000:
            break
    ctx.log.info(f"Fetched `{ix}` politicians.")

    ctx.log.info("Fetching mandates ...")
    for ix, item in enumerate(
        fetch_iter(URL + "candidacies-mandates", current_on="all"), 1
    ):
        item["parliament_period"] = dict_merge(
            item["parliament_period"],
            parliament_periods[item["parliament_period"]["id"]],
        )
        item["parliament_period"]["parliament"] = dict_merge(
            item["parliament_period"]["parliament"],
            parliaments[item["parliament_period"]["parliament"]["id"]],
        )
        yield item
        if TESTING and ix == 1000:
            break
    ctx.log.info(f"Fetched `{ix}` mandates.")
