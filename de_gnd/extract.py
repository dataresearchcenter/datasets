import gzip
from typing import Any, Generator

import ijson
from investigraph.model import Context


def handle(ctx: Context, *args, **kwargs) -> Generator[dict[str, Any], None, None]:
    with gzip.open(ctx.source.uri) as fh:
        items = ijson.items(fh, "item.item")
        yield from items
