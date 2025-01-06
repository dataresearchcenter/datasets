from typing import Any
from urllib.parse import urlparse, urlunparse

import requests
from anystore.store import get_store
from banal import is_mapping
from memorious.logic.context import Context
from servicelayer import env

from utils import Data

CACHE_PREFIX = env.get("MEMORIOUS_CACHE_PREFIX", "memorious")
USE_CACHE = env.to_bool("MEMORIOUS_CACHE", True)
CACHE = get_store()


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


def sanitize_key(url: str) -> str | None:
    """Remove scheme to make path-like key of an url"""
    return urlunparse(["", *urlparse(url)[1:]]).strip("/")


def make_emit_cache_key(context: Context, data: Data) -> str | None:
    cache_key = data.get("emit_cache_key", data.get("url"))
    if not cache_key:
        return
    cache_key = sanitize_key(cache_key)
    return make_cache_key(context, f"emit/{cache_key}")
