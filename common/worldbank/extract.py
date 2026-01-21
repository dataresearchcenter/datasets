"""Shared extraction logic for World Bank Finances One datasets."""

from collections.abc import Generator
from urllib.parse import parse_qs, urlparse

import requests

from investigraph.model import SourceContext
from investigraph.types import Record

PAGE_SIZE = 1000  # API maximum


def handle(ctx: SourceContext) -> Generator[Record, None, None]:
    """Extract records from World Bank Finances One API with pagination.

    The source URI should contain datasetId and resourceId parameters:
    https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice?datasetId=XXX&resourceId=YYY&type=json
    """
    # Parse datasetId and resourceId from source URI
    parsed = urlparse(ctx.source.uri)
    params = parse_qs(parsed.query)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

    dataset_id = params.get("datasetId", [""])[0]
    resource_id = params.get("resourceId", [""])[0]

    if not dataset_id or not resource_id:
        ctx.log.error("Missing datasetId or resourceId in source URI")
        return

    skip = 0
    total = None

    while True:
        request_params = {
            "datasetId": dataset_id,
            "resourceId": resource_id,
            "type": "json",
            "top": PAGE_SIZE,
            "skip": skip,
        }

        res = requests.get(base_url, params=request_params)
        res.raise_for_status()
        data = res.json()

        # Get total count from first response
        if total is None:
            total = data.get("count", 0)
            ctx.log.info(f"Total records: {total}")

        records = data.get("data", [])
        if not records:
            break

        for record in records:
            yield record

        skip += len(records)

        # Log progress every 1000 records
        if skip % PAGE_SIZE == 0:
            ctx.log.info(f"Extracted {skip}/{total} records")

        # Stop if we've fetched all records
        if skip >= total:
            break
