from typing import Any
from banal import is_mapping
from memorious.logic.context import Context


def make_url_cache_key(
    context: Context, url: str | dict[str, Any] | None
) -> str | None:
    if is_mapping(url):
        url = url["url"]
    if not url:
        return
    prefix = context.crawler.name
    url = url.split("//", 1)[1]
    return f"{prefix}/{url}"
