from anystore.io import DoesNotExist, smart_read
from servicelayer import env
from typing import Any, Generator
from structlog import get_logger
import yaml
from anystore import get_store, smart_write

log = get_logger(__name__)


DATASETS = get_store("datasets", serialization_mode="raw")
LEAKRFC_URI = env.get("LEAKRFC_URI", "s3://investigativecommons")
PUBLIC_URL = "https://data.investigativecommons.org"


def make_config_uri(dataset: str) -> str:
    return f"{LEAKRFC_URI}/{dataset}/.leakrfc/config.yml"


def make_index_uri(dataset: str) -> str:
    return f"{PUBLIC_URL}/{dataset}/.leakrfc/index.json"


def get_datasets() -> Generator[dict[str, Any], None, None]:
    """Iterate through all local dataset configs"""
    for key in DATASETS.iterate_keys(glob="**/config.yml"):
        dataset = DATASETS.get(key, deserialization_func=yaml.safe_load)
        yield dataset


def get_dataset(name: str) -> dict[str, Any]:
    """Lookup a local dataset config by its name (foreign_id)"""
    for dataset in get_datasets():
        if dataset["name"] == name:
            return dataset
    raise DoesNotExist(name)


def push_dataset_configs(name: str | None = None) -> None:
    """Push a local dataset config (or all) to the remote storage"""
    for key in DATASETS.iterate_keys(glob="**/config.yml"):
        dataset = DATASETS.get(key, deserialization_func=yaml.safe_load)
        if name and dataset["name"] == name or name is None:
            uri = make_config_uri(dataset["name"])
            smart_write(uri, DATASETS.get(key))
            log.info("Upload complete.", dataset=dataset["name"], uri=uri)


def make_catalog(in_uri: str) -> str:
    """Make the catalog.yml with all the datasets referenced"""
    catalog = yaml.safe_load(smart_read(in_uri))
    catalog["datasets"] = []
    for dataset in get_datasets():
        catalog["datasets"].append({"from_uri": make_index_uri(dataset["name"])})
    return yaml.safe_dump(catalog)
