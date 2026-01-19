"""Shared extraction logic for OCDS datasets.

Streams gzip-compressed JSON lines from Open Contracting Data Standard sources.
"""

import gzip
from collections.abc import Generator

import orjson
import requests

from investigraph.model import SourceContext
from investigraph.types import Record


def handle(ctx: SourceContext) -> Generator[Record, None, None]:
    """Extract records from OCDS gzip JSON lines source.

    Streams the gzip file and yields each JSON line as a record.
    This is more memory-efficient than loading the entire file with pandas.
    """
    uri = ctx.source.uri

    ctx.log.info(f"Streaming OCDS data from {uri}")

    res = requests.get(uri, stream=True)
    res.raise_for_status()

    count = 0
    with gzip.open(res.raw, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue

            try:
                record = orjson.loads(line)
                yield record
                count += 1

                if count % 10000 == 0:
                    ctx.log.info(f"Extracted {count} records")
            except orjson.JSONDecodeError as e:
                ctx.log.warning(f"Failed to parse JSON line: {e}")
                continue

    ctx.log.info(f"Finished extracting {count} records")
