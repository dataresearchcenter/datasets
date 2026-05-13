"""NARA NSDAP membership records crawler.

Iterates a static `urls.json` manifest (one entry per PDF) produced upstream
from the NARA Catalog API and emits each record into the fetch pipeline.

Source json from this repo: https://github.com/R3ITOSv87/nara-crawler
"""

import json
from pathlib import Path

from anystore.types import SDict
from memorious.logic.context import Context


def seed(context: Context, data: SDict):
    source = Path(context.params["source"])
    if not source.is_absolute():
        source = Path(__file__).parent / source

    publisher = context.crawler.config.publisher.model_dump(mode="json")

    with source.open() as fh:
        manifest = json.load(fh)

    for item in manifest["items"]:
        na_id = item["naId"]
        context.emit(
            data={
                "url": item["pdf_url"],
                "file_name": item["filename"],
                "title": item["title"],
                "foreign_id": na_id,
                "emit_cache_key": na_id,
                "na_id": na_id,
                "publisher": publisher,
            }
        )
