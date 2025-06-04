from banal import ensure_list
import httpx
import fnmatch
from typing import Annotated, Any, Generator

from anystore.decorators import anycache
from anystore.io import smart_write
from anystore.logging import configure_logging
from ftmq.model import Catalog as BaseCatalog, Dataset
from structlog import get_logger
import typer

configure_logging()
log = get_logger("datasets.build_catalog")
ALEPH_URL = "https://search.openaleph.org/api/2/collections?exclude:category=casefile&facet=countries&facet=category&facet_size:category=1000&facet_size:countries=1000&facet_total:category=true&facet_total:countries=true&limit=30&q={foreign_id}&sort=created_at:desc"


@anycache
def get_aleph_url(foreign_id: str) -> str | None:
    res = httpx.get(ALEPH_URL.format(foreign_id=foreign_id))
    if res.status_code == 200:
        data = res.json()
        for collection in ensure_list(data["results"]):
            if collection["foreign_id"] == foreign_id:
                log.info("Found Aleph collection", foreign_id=foreign_id)
                return collection["links"]["ui"]


class Catalog(BaseCatalog):
    include_datasets: list[str] = []
    exclude_datasets: list[str] = []
    patch_metadata: dict[str, Any] = {}

    def patch_dataset(self, ds: Dataset) -> Dataset:
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
    include_catalogs: list[Catalog]

    def get_datasets(self) -> Generator[Dataset, None, None]:
        yield from super().get_datasets()
        for catalog in self.include_catalogs:
            yield from catalog.get_datasets()

    def serialize(self) -> str:
        seen = set()
        catalog = Catalog(name=self.name)
        for dataset in self.get_datasets():
            if dataset.name not in seen:
                catalog.datasets.append(dataset)
                seen.add(dataset.name)

        return catalog.model_dump_json()


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
