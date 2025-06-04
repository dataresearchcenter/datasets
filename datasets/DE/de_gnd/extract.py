import gzip

import ijson
from investigraph.model import SourceContext
from investigraph.types import Record, RecordGenerator


def get_type(record: Record) -> str | None:
    # TODO: Adjust for multiple corporate types
    if "@type" in record.keys():
        return record["@type"][0].split("#")[-1]


def should_transform(ctx: SourceContext, record: Record) -> bool:
    type_ = get_type(record)
    if type_:
        if ctx.source.name == "legalentity":
            return True
        # skip ids with '/about' as this is metadata instead of a new entity
        if "about" in record["@id"]:
            return False
        return type_ != "Family"
    return False


def handle(ctx: SourceContext, *args, **kwargs) -> RecordGenerator:
    with ctx.open() as gzfh:
        with gzip.open(gzfh) as fh:
            for record in ijson.items(fh, "item.item"):
                if should_transform(ctx, record):
                    yield record
