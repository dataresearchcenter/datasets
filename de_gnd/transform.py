from typing import Any, Generator

from investigraph.model import Context
from nomenklatura.entity import CE


def handle(ctx: Context, record: dict[str, Any], ix: int) -> Generator[CE, None, None]:
    if ctx.source.name == "legalentity":
        # create LegalEntity
        import ipdb

        ipdb.set_trace()
    elif ctx.source.name == "person":
        pass
