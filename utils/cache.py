from datetime import datetime
from typing import Any

import requests
from anystore.decorators import anycache
from anystore.store import get_store
from banal import is_mapping
from memorious.logic.context import Context
from servicelayer import env

CACHE_PREFIX = env.get("MEMORIOUS_CACHE_PREFIX", "memorious")
USE_CACHE = env.to_bool("MEMORIOUS_CACHE", True)


def make_cache_key(context: Context, key: str) -> str | None:
    if not USE_CACHE or not key:
        return
    prefix = context.crawler.name
    return f"{CACHE_PREFIX}/{prefix}/{key}"


def make_url_cache_key(
    context: Context, url: str | dict[str, Any] | None, *args, **kwargs
) -> str | None:
    if not USE_CACHE:
        return

    if is_mapping(url):
        url = url["url"]
    if not url:
        return

    # don't cache error responses
    res = requests.head(url)
    if not res.ok:
        return

    url = url.split("//", 1)[1]
    return make_cache_key(context, url)


@anycache(key_func=make_url_cache_key)
def emit_cached(
    context: Context, data: dict[str, Any], rule: str | None = None
) -> datetime:
    context.emit(rule or "pass", data=data)
    return datetime.now()


CACHE = get_store()
