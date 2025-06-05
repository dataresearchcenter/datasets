from io import BytesIO
from pathlib import Path
from typing import Any, Generator
from zipfile import ZipFile

import pandas as pd
from investigraph.model import Context, Resolver


def handle(ctx: Context, res: Resolver) -> Generator[dict[str, Any], None, None]:
    content = res.get_content()
    z = ZipFile(BytesIO(content))
    for f in z.filelist:
        if f.filename.endswith(".csv"):
            df = pd.read_csv(z.open(f.filename), delimiter=";", dtype=str)
            df["_type"] = Path(f.filename).stem
            df = df.fillna("")
            for _, row in df.iterrows():
                yield dict(row)
