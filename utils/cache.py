from datetime import datetime
from typing import Any

import requests
from anystore.decorators import anycache
from banal import is_mapping
from memorious.logic.context import Context


def make_url_cache_key(
    context: Context, url: str | dict[str, Any] | None, *args, **kwargs
) -> str | None:
    if is_mapping(url):
        url = url["url"]
    if not url:
        return

    # don't cache error responses
    res = requests.head(url)
    if not res.ok:
        return

    prefix = context.crawler.name
    url = url.split("//", 1)[1]
    return f"{prefix}/{url}"


@anycache(key_func=make_url_cache_key)
def emit_cached(
    context: Context, data: dict[str, Any], rule: str | None = None
) -> datetime:
    context.emit(rule or "pass", data=data)
    return datetime.now()
