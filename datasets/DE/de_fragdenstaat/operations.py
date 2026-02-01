from typing import Any

from banal import ensure_dict
from memorious.logic.context import Context
from memorious.util import make_url_key

DEFAULT_URL = "https://fragdenstaat.de/api/v1/document"


def get_publicbody(context: Context, url: str | None) -> dict[str, Any]:
    if url:
        key = f"fds-bodies/{make_url_key(url)}"
        cached = context.tags.get(key)
        if cached:
            return cached
        res = context.http.get(url)
        context.tags.put(key, res.json)
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
                "foreign_id": document["id"],
            }

            if data["url"]:
                context.emit(data=data)

    if res.json["meta"]["next"] is not None:
        context.recurse(data={"url": res.json["meta"]["next"]})
