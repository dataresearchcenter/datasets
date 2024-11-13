from investigraph.model import Context, Resolver
from investigraph.types import RecordGenerator
import orjson


def handle(ctx: Context, res: Resolver, **kwargs) -> RecordGenerator:
    content = res.get_content()
    data = orjson.loads(content)["results"]
    yield from data.values()
