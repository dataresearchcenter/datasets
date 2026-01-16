from ftmq.types import CEGenerator
from ftmq.util import get_country_code
from investigraph.model.context import SourceContext, TaskContext
from investigraph.types import Record
from nomenklatura.entity import CE
from normality import normalize

URL = "https://landmatrix.org/api/legacy_export/?filters=[]&subset=PUBLIC&format=csv"


def make_investor(
    ctx: TaskContext, lmi_id: str, name: str, schema: str | None = "LegalEntity"
) -> CE:
    proxy = ctx.make_proxy(schema or "LegalEntity")
    proxy.id = ctx.make_slug(lmi_id)
    proxy.add("name", name)
    return proxy


def parse_investor(ctx: TaskContext, record: dict[str, str]):
    proxy = make_investor(ctx, record.pop("Investor ID"), record.pop("Name"))
    country = get_country_code(record.pop("Country of registration/origin"))
    proxy.add("country", country)
    proxy.add("jurisdiction", country)
    proxy.add("legalForm", record.pop("Classification"))
    proxy.add("website", record.pop("Investor homepage"))
    url = record.pop("Opencorporates link")  # it's not only open corporates
    if "opencorporates" in url:
        proxy.add("opencorporatesUrl", url)
        proxy.add("idNumber", url.split("/")[-1])
    else:
        proxy.add("sourceUrl", url)

    ctx.emit(proxy)


def parse_involvement(ctx: TaskContext, record: dict[str, str]):
    owner = make_investor(
        ctx,
        record.pop("Investor ID Upstream"),
        record.pop("Investor Name Upstream"),
    )
    asset = make_investor(
        ctx,
        record.pop("Investor ID Downstream"),
        record.pop("Investor Name Downstream"),
        "Company",
    )

    ctx.emit(owner)
    ctx.emit(asset)

    date = record.pop("Loan date")
    role = record.pop("Relation type")
    otype = record.pop("Investment type")
    rel = ctx.make_proxy("Ownership")
    rel.id = ctx.make_id(
        "ownership", owner.id, asset.id, date, normalize(role), normalize(otype)
    )
    rel.add("owner", owner)
    rel.add("asset", asset)
    rel.add("date", date)
    rel.add("role", role)
    rel.add("ownershipType", otype)
    rel.add("percentage", record.pop("Ownership share"))
    rel.add("sharesValue", record.pop("Loan amount"))
    rel.add("sharesCurrency", record.pop("Loan currency"))
    rel.add("summary", record.pop("Comment"))

    ctx.emit(rel)


HANDLERS = {
    "investors.csv": parse_investor,
    "involvements.csv": parse_involvement,
}


def handle(ctx: Sourcectx, record: Record, *args, **kwargs) -> CEGenerator:
    ctx = ctx.task()
    handler = HANDLERS[record["__source__name"]]
    handler(ctx, record)
    yield from ctx
