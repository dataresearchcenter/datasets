from typing import Any, Generator

import json
from investigraph.model import Context


def handle(ctx: Context, *args, **kwargs) -> Generator[dict[str, Any], None, None]:
    with open(ctx.source.uri, "r") as fh:
        data = json.load(fh)
        for key, entry in data["results"].items():
            yield entry
