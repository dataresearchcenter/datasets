import httpx
import logging
import os
import time
from banal import as_bool
from investigraph import SourceContext
from investigraph.types import RecordGenerator

URL = "https://www.marktstammdatenregister.de/MaStR/Akteur/MarktakteurJson/GetOeffentlicheMarktakteure"
PAGE_SIZE = 5000
MAX_ERRORS = 5
TESTING = as_bool(os.environ.get("TESTING"))

log = logging.getLogger(__name__)


def make_request(url: str, params: dict, timeout: int = 120) -> dict:
    backoff = 5
    errors = 0
    res = None
    while errors < MAX_ERRORS:
        try:
            res = httpx.get(url, params=params, timeout=timeout)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            log.warning(
                "API %s [%s], backoff %d sec."
                % (url, res.status_code if res is not None else e, backoff)
            )
            errors += 1
            time.sleep(backoff)
            backoff = backoff * 2
    raise Exception(f"Service unavailable for url `{url}`")


def handle(ctx: SourceContext, *args, **kwargs) -> RecordGenerator:
    page = 1
    total = None
    fetched = 0

    while True:
        params = {
            "sort": "",
            "page": page,
            "pageSize": PAGE_SIZE,
            "group": "",
            "filter": "",
        }

        ctx.log.info(f"Fetching page {page}...")
        response = make_request(URL, params)

        if total is None:
            total = response.get("Total", 0)
            ctx.log.info(f"Total records: {total}")

        data = response.get("Data", [])
        if not data:
            break

        for record in data:
            yield record
            fetched += 1

            if TESTING and fetched >= 100:
                ctx.log.info(f"Testing mode: stopped after {fetched} records")
                return

        ctx.log.info(f"Fetched {fetched} / {total} records")

        if fetched >= total:
            break

        page += 1
        time.sleep(0.5)  # Be nice to the server

    ctx.log.info(f"Extraction complete: {fetched} records")
