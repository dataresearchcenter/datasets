from banal import clean_dict
import pandas as pd
from zipfile import ZipFile
from investigraph.model import SourceContext
from investigraph.types import RecordGenerator

SOURCES = ("investors.csv", "involvements.csv")


def handle(ctx: SourceContext, *args, **kwargs) -> RecordGenerator:
    with ctx.open() as fh:
        with ZipFile(fh) as zf:
            for name in zf.namelist():
                if name in SOURCES:
                    with zf.open(name) as f:
                        df = (
                            pd.read_csv(f, delimiter=";")
                            .fillna("")
                            .map(str)
                            .map(lambda x: x.strip())
                        )
                        df["__source_name__"] = name
                        for _, record in df.iterrows():
                            yield clean_dict(record)
