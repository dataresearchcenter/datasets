"""Shared extraction logic for OCDS datasets.

Streams gzip-compressed JSON lines from Open Contracting Data Standard sources.
Uses investigraph's built-in archiving/caching via ctx.open().
"""

import gzip
from collections.abc import Generator

import orjson

from investigraph.model import SourceContext
from investigraph.types import Record


def handle(ctx: SourceContext) -> Generator[Record, None, None]:
    """Extract records from OCDS gzip JSON lines source.

    Uses ctx.open() to let investigraph handle download/caching,
    then decompresses and yields each JSON line as a record.
    """
    ctx.log.info(f"Fetching OCDS data from {ctx.source.uri}")

    count = 0
    with ctx.open() as fh:
        with gzip.open(fh, "rt", encoding="utf-8") as gz:
            for line in gz:
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
