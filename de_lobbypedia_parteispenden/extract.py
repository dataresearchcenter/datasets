import orjson
from typing import Any, Generator
import requests

from investigraph.model import Context, Resolver


def handle(ctx: Context, res: Resolver, **kwargs) -> Generator[dict[str, Any], None, None]:
    content = res.get_content()
    data = orjson.loads(content)["results"]
    for key, entry in data.items():
        yield entry
