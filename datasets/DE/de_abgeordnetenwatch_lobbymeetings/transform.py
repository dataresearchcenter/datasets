from typing import Any, Generator

from investigraph.model import Context
from nomenklatura.entity import CE


def handle(ctx: Context, record: dict[str, Any], ix: int) -> Generator[CE, None, None]:
    pass
