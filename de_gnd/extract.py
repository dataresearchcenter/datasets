import gzip
import os
from typing import Any, Generator

import ijson
from investigraph.model import Context
from investigraph.types import Record

SIZE = int(os.environ.get("GND_TEST_SIZE", 0))


def get_type(record: Record) -> str:
    # TODO: Adjust for multiple corporate types
    if "@type" in record.keys():
        return record["@type"][0].split("#")[-1]


def should_transform(ctx: Context, record: Record) -> bool:
    type_ = get_type(record)
    if type_:
        if ctx.source.name == "legalentity":
            return True
        # skip ids with '/about' as this is metadata instead of a new entity
        if "about" in record["@id"]:
            return False
        return type_ != "Family"
    return False


def handle(ctx: Context, *args, **kwargs) -> Generator[dict[str, Any], None, None]:
    ix = 0
    with gzip.open(ctx.source.uri) as fh:
        for record in ijson.items(fh, "item.item"):
            if should_transform(ctx, record):
                yield record
                ix += 1
                if SIZE and ix > SIZE:
                    break
