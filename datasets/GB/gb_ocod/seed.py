"""Seed stage for GB OCOD dataset.

Fetches the download URL from the HM Land Registry API and yields a Source.
"""

import os
from typing import Generator

import requests
from investigraph.model import DatasetContext, Source


def handle(ctx: DatasetContext) -> Generator[Source, None, None]:
    """Fetch the actual download URL from the API and yield as Source."""
    uri = ctx.config.seed.uri
    headers = {
        "Authorization": os.getenv("GB_OCOD_KEY"),
        "Accept": "application/json",
    }

    # Get dataset metadata
    ctx.log.info("Fetching dataset metadata", url=uri)
    res = requests.get(uri, headers=headers)
    res.raise_for_status()
    data = res.json()

    # Find the "Full File" resource
    resources = data.get("result", {}).get("resources", [])
    full_file = next((r for r in resources if r.get("name") == "Full File"), None)
    if not full_file:
        ctx.log.error("No 'Full File' resource found")
        return

    resource_name = full_file["file_name"]

    # Get the download URL for the resource
    resource_res = requests.get(f"{uri}/{resource_name}", headers=headers)
    resource_res.raise_for_status()
    download_url = resource_res.json().get("result", {}).get("download_url")

    if not download_url:
        ctx.log.error("No download URL found", resource=resource_name)
        return

    ctx.log.info("Found download URL", url=download_url)
    yield Source(uri=download_url, name=resource_name)
