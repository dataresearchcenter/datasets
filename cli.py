from typing import Annotated, Optional
from anystore.io import smart_write
import typer
from utils.catalog import make_catalog, push_dataset_configs

cli = typer.Typer(no_args_is_help=True)


@cli.command("make-catalog")
def cli_make_catalog(
    in_uri: Annotated[
        Optional[str], typer.Option("-i", help="Input yaml uri")
    ] = "catalog.yml",
    out_uri: Annotated[Optional[str], typer.Option("-o", help="Output json uri")] = "-",
):
    """Build the catalog.yml"""
    in_uri = in_uri or "-"
    out_uri = out_uri or "-"
    data = make_catalog(in_uri)
    smart_write(out_uri, data.encode())


@cli.command("push-config")
def cli_push_config(
    dataset: Annotated[Optional[str], typer.Option("-d", help="Dataset name")] = None,
):
    """Push a dataset config or all configs to the remote storage"""
    push_dataset_configs(dataset)
