import time
from datetime import datetime

import httpx
import orjson
from investigraph.model import SourceContext
from investigraph.types import Record, RecordGenerator

FIRST_YEAR = 2000
MAX_RETRIES = 5


def fetch(ctx: SourceContext, year: str) -> Record:
    # fetch directly via httpx instead of `ctx.open()`: anystore url-decodes
    # the uri, and the literal "::" of a decoded SMW ask query breaks fsspec
    # (it parses it as a filesystem chain). httpx encodes the query param
    # properly, so the plain query text lives in the source `data.query`.
    url = httpx.URL(ctx.source.uri).copy_merge_params(
        {"query": ctx.source.data["query"].format(year=year)}
    )
    backoff = 10
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = httpx.get(url, timeout=60)
            res.raise_for_status()
            return orjson.loads(res.content)
        except httpx.HTTPError as e:
            if attempt == MAX_RETRIES:
                raise
            ctx.log.warning(f"Fetch failed ({e}), retrying in {backoff}s ...")
            time.sleep(backoff)
            backoff *= 2


def handle(ctx: SourceContext, *args, **kwargs) -> RecordGenerator:
    # SMW caps results at 10.000 per query and silently resets offsets beyond
    # ~5k, so pagination can't recover truncated results. Instead, query year
    # by year (~2k records max), framed by guard queries that catch records
    # outside the iterated range ("<"/">" mean <=/>= in SMW).
    first_year = int(ctx.source.data.get("first_year", FIRST_YEAR))
    last_year = datetime.now().year + 1
    years = [
        f"<{first_year - 1}",
        *map(str, range(first_year, last_year + 1)),
        f">{last_year + 1}",
    ]
    total = 0
    for year in years:
        data = fetch(ctx, year)
        if "error" in data:
            raise ValueError(f"SMW api error: {data['error']}")
        if "query-continue-offset" in data:
            raise ValueError(
                f"`[[Jahr::{year}]]` hits the SMW result limit and is "
                "truncated. Split the query further, e.g. by Kategorie."
            )
        results = data["query"].get("results") or {}
        if results and not year.isdigit():
            ctx.log.warning(
                f"`{len(results)}` records outside {first_year}-{last_year}",
                year=year,
            )
        ctx.log.info(f"Fetched `{len(results)}` records", year=year)
        total += len(results)
        yield from results.values()
    ctx.log.info(f"Fetched `{total}` records in total", source=ctx.source.name)
