import gzip
import os
from typing import Any, Generator

import ijson
from investigraph.model import Context

SIZE = int(os.environ.get("GND_TEST_SIZE", 0))


def handle(ctx: Context, *args, **kwargs) -> Generator[dict[str, Any], None, None]:
    with gzip.open(ctx.source.uri) as fh:
        items = enumerate(ijson.items(fh, "item.item"))
        for ix, item in items:
            yield item
            if SIZE and ix > SIZE:
                break
