import json
from datetime import datetime
from typing import Any, Dict, Generator, Optional, Union

import requests
import yaml
from nomenklatura.dataset import DataCatalog
from nomenklatura.util import PathLike, datetime_iso
from zavod.dataset import ZavodDataset


def flatten_catalog(url: str) -> Generator[Dict[str, Any], None, None]:
    try:
        resp = requests.get(url)
        catalog_data = resp.json()
        for dataset in catalog_data.get("datasets", []):
            if dataset.get("type") != "collection":
                yield dataset
    except Exception as exc:
        print("ERROR [%s]: %s" % (url, exc))


def build_catalog(catalog_in: PathLike, fname: str):
    print("Catalog: %s" % catalog_in)
    seen = set()
    with open(catalog_in, "r") as fh:
        catalog_in_data = yaml.safe_load(fh)
    catalog = DataCatalog(ZavodDataset, {})
    catalog.updated_at = datetime_iso(datetime.utcnow())
    for ds_data in catalog_in_data["datasets"]:
        include_url: Optional[str] = ds_data.pop("include", None)
        if include_url is not None:
            try:
                resp = requests.get(include_url)
                ds_data = resp.json()
                name = ds_data["name"]
                if name not in seen:
                    ds = catalog.make_dataset(ds_data)
                    print("Dataset: %r" % ds)
                    seen.add(name)
                else:
                    print("Dataset `%s` already in catalog." % name)
            except Exception as exc:
                print("ERROR [%s]: %s" % (include_url, exc))
        include_catalog: Optional[Union[str, Dict[str, Any]]] = ds_data.pop(
            "include_catalog", None
        )
        if include_catalog is not None:
            if isinstance(include_catalog, str):
                include_catalog_url = include_catalog
                exclude = []
            else:
                include_catalog_url = include_catalog.pop("url")
                exclude = include_catalog.pop("exclude", [])

            for ds_data in flatten_catalog(include_catalog_url):
                name = ds_data["name"]
                if name not in seen and name not in exclude:
                    ds = catalog.make_dataset(ds_data)
                    print("Dataset: %r" % ds)
                    seen.add(name)
                else:
                    print("Dataset `%s` already in catalog or excluded." % name)

    with open(fname, "w") as fh:
        json.dump(catalog.to_dict(), fh)


if __name__ == "__main__":
    build_catalog("catalog.in.yml", fname="catalog.json")
    build_catalog("catalog.all.yml", fname="catalog.all.json")
    build_catalog("catalog.investigraph.yml", fname="investigraph.json")
