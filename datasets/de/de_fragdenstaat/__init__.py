from typing import Any

from anystore.decorators import anycache
from banal import ensure_dict
from memorious.logic.context import Context

from utils.cache import emit_cached, make_url_cache_key

DEFAULT_URL = "https://fragdenstaat.de/api/v1/document"


@anycache(key_func=make_url_cache_key)
def get_publicbody(context: Context, url: str | None) -> dict[str, Any]:
    if url:
        res = context.http.get(url)
        return res.json
    return {}


def reduce_publicbody(data: dict[str, Any]) -> dict[str, Any]:
    if not data:
        return data
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "jurisdiction": ensure_dict(data.get("jurisdiction")).get("name"),
    }


def seed(context, data):
    url = data.get("url") or context.get("url", DEFAULT_URL)
    res = context.http.get(url)

    for document in res.json["objects"]:
        if document.get("foirequest") is not None:
            publicbody = get_publicbody(context, document["publicbody"])
            data = {
                **document,
                "url": document["file_url"],
                "source_url": document["site_url"],
                "publicbody": reduce_publicbody(publicbody),
            }

            if data["url"]:
                emit_cached(context, data)

    if res.json["meta"]["next"] is not None:
        context.recurse(data={"url": res.json["meta"]["next"]})
