from memorious.logic.context import Context

from utils import Data
from utils.operations import cached_emit

DEFAULT_URL = "https://fragdenstaat.de/api/v1/documentcollection"


def seed(context: Context, data: Data):
    url = data.get("url") or context.get("url", DEFAULT_URL)
    res = context.http.get(url)

    for collection in res.json["objects"]:
        for document in collection["documents"]:
            data = {
                **document,
                "collection": collection["title"],
                "collection_id": collection["id"],
                "collection_date": collection["created_at"],
                "url": document["file_url"],
                "source_url": document["site_url"],
            }

            if data["url"]:
                cached_emit(context, data)

    if res.json["meta"]["next"] is not None:
        context.recurse(data={"url": res.json["meta"]["next"]})
