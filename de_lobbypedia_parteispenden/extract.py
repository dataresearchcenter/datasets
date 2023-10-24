from typing import Any, Generator
import requests

from investigraph.model import Context


def handle(ctx: Context, *args, **kwargs) -> Generator[dict[str, Any], None, None]:
    res = requests.get(ctx.source.uri) 
    data = res.json()['results']
    for key, entry in data.items():
        yield entry
