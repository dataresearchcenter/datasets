import os
import re
from base64 import b64encode
from fnmatch import fnmatch
from typing import Any, Generator

import requests
from anystore.util import clean_dict
from investigraph.model import Source, SourceContext
from runpandarun import Playbook

AUTH = tuple(os.environ["DATA_BASIC_AUTH"].split(":"))
STORAGE_OPTIONS = {
    "Authorization": b"Basic %s" % b64encode(os.environ["DATA_BASIC_AUTH"].encode())
}
PAT = re.compile(r"a href=\"(?P<url>.*\.csv\.gz)\"")
BASE_URL = "https://data.farmsubsidy.org/cleaned/"


def get_play(url: str) -> Playbook:
    return Playbook(
        read={
            "options": {"storage_options": STORAGE_OPTIONS},
            "uri": url,
            "handler": "read_csv",
        }
    )


def seed(ctx: SourceContext, *args, **kwargs) -> Generator[Source, None, None]:
    for glob in ctx.config.seed.glob:
        uri = glob.rsplit("/", 1)[0]
        index = requests.get(uri, auth=AUTH)
        for url in PAT.findall(index.text):
            url = BASE_URL + url
            if fnmatch(url, glob):
                yield Source(uri=url, pandas=get_play(url))


def handle(
    ctx: SourceContext, *args, **kwargs
) -> Generator[dict[str, Any], None, None]:
    df = ctx.source.pandas.run()
    df = df.fillna("").map(str)
    df["recipient_id"] = ctx.config.dataset.prefix + "-" + df["recipient_id"]
    df["pk"] = ctx.config.dataset.prefix + "-" + df["pk"]
    for _, row in df.iterrows():
        yield clean_dict(dict(row))
