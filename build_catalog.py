from banal import ensure_list
import httpx
import fnmatch
from typing import Annotated, Any, Generator

from anystore.io import smart_write
from anystore.logging import configure_logging
from ftmq.model import Catalog as BaseCatalog, Dataset as BaseDataset
from structlog import get_logger
import typer


class Dataset(BaseDataset):
    """Extended Dataset with aleph_url field."""

    aleph_url: str | None = None

configure_logging()
log = get_logger("datasets.build_catalog")
ALEPH_API = "https://search.openaleph.org/api/2/collections"

# Global lookup for Aleph collections: {foreign_id: ui_url}
_aleph_lookup: dict[str, str] | None = None


def _fetch_aleph_collections() -> dict[str, str]:
    """Fetch all collections from Aleph with pagination and build lookup."""
    lookup = {}
    offset = 0
    limit = 100

    log.info("Fetching Aleph collections...")

    while True:
        params = {
            "exclude:category": "casefile",
            "limit": limit,
            "offset": offset,
        }
        try:
            res = httpx.get(ALEPH_API, params=params, timeout=30)
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            log.error("Failed to fetch Aleph collections", error=str(e))
            break

        results = ensure_list(data.get("results", []))
        if not results:
            break

        for collection in results:
            foreign_id = collection.get("foreign_id")
            links = collection.get("links") or {}
            ui_url = links.get("ui")
            if foreign_id and ui_url:
                lookup[foreign_id] = ui_url

        total = data.get("total", 0)
        offset += limit

        if offset >= total:
            break

    log.info("Fetched Aleph collections", count=len(lookup))
    return lookup


def get_aleph_url(foreign_id: str) -> str | None:
    """Look up Aleph URL for a dataset by foreign_id."""
    global _aleph_lookup
    if _aleph_lookup is None:
        _aleph_lookup = _fetch_aleph_collections()
    return _aleph_lookup.get(foreign_id)


class Catalog(BaseCatalog):
    include_datasets: list[str] = []
    exclude_datasets: list[str] = []
    patch_metadata: dict[str, Any] = {}

    def patch_dataset(self, ds: BaseDataset) -> Dataset:
        prefix = self.patch_metadata.get("dataset_prefix")
        if prefix is not None and ds.name not in self.patch_metadata.get(
            "dataset_prefix_ignore", []
        ):
            if not ds.name.startswith(prefix):
                ds.name = f"{prefix}_{ds.name}"
        return Dataset(
            **{
                **ds.model_dump(),
                "aleph_url": get_aleph_url(ds.name),
                **self.patch_metadata,
            }
        )

    def get_datasets(self) -> Generator[Dataset, None, None]:
        for dataset in self.datasets:
            if self.include_datasets and not any(
                (fnmatch.fnmatch(dataset.name, m) for m in self.include_datasets)
            ):
                continue
            if self.exclude_datasets and any(
                (fnmatch.fnmatch(dataset.name, m) for m in self.exclude_datasets)
            ):
                continue
            yield self.patch_dataset(dataset)


class MultiCatalog(Catalog):
    include_catalogs: list[Catalog] = []

    def get_datasets(self) -> Generator[Dataset, None, None]:
        yield from super().get_datasets()
        for catalog in self.include_catalogs:
            yield from catalog.get_datasets()

    def serialize(self) -> str:
        import json

        seen = set()
        datasets = []
        for dataset in self.get_datasets():
            if dataset.name not in seen:
                datasets.append(dataset.model_dump(mode="json"))
                seen.add(dataset.name)

        return json.dumps({"name": self.name, "datasets": datasets})


def main(
    in_uri: Annotated[str, typer.Option("-i")] = "-",
    out_uri: Annotated[str, typer.Option("-o")] = "-",
):
    """
    Build a catalog from datasets metadata and write it to anywhere from stdout
    (default) to any uri `anystore` can handle.
    """
    catalog = MultiCatalog._from_uri(in_uri)
    data = catalog.serialize()
    smart_write(out_uri, data.encode())


if __name__ == "__main__":
    typer.run(main)
