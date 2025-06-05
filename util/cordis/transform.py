from ftmq.types import CE, CEGenerator, SDict
from ftmq.util import make_fingerprint, get_country_code, make_proxy, to_numeric
from investigraph.model import Context


EU = make_proxy(
    {
        "id": "eu",
        "schema": "PublicBody",
        "properties": {"name": ["European Union"], "weakAlias": ["EU"]},
    }
)


def ensure_publisher(ctx: Context, proxy: CE) -> CE:
    proxy.add("publisher", ctx.config.dataset.publisher.name)
    proxy.add("publisherUrl", ctx.config.dataset.publisher.url)
    return proxy


def make_project(ctx: Context, record: SDict, ident: str | None = "projectID") -> CE:
    project = ctx.make("Project")
    ident = record.pop(ident)
    project.id = ctx.make_slug(ident)
    project.add("projectId", ident)
    return project


def parse_euroscivoc(ctx: Context, record: SDict):
    project = make_project(ctx, record)
    project.add("keywords", record.pop("euroSciVocPath").split("/"))

    ctx.emit(project)


def parse_legalbasis(ctx: Context, record: SDict):
    project = make_project(ctx, record)
    program, name = record.pop("legalBasis"), record.pop("title")
    project.add("program", f"{program} - {name}")

    ctx.emit(project)


def parse_urls(ctx: Context, record: SDict):
    project = make_project(ctx, record)
    project.add("sourceUrl", record.pop("physUrl"))

    ctx.emit(project)


def parse_project(ctx: Context, record: SDict):
    project = make_project(ctx, record, "id")
    acronym, name = record.pop("acronym"), record.pop("title")
    project.add("name", f"{acronym} - {name}")
    project.add("weakAlias", acronym)
    project.add("status", record.pop("status"))
    project.add("startDate", record.pop("startDate"))
    project.add("endDate", record.pop("endDate"))
    amount = to_numeric(record.pop("totalCost"))
    project.add("amount", amount)
    project.add("amountEur", amount)
    project.add("currency", "EUR")
    project.add("program", record.pop("frameworkProgramme"))
    project.add("program", record.pop("legalBasis"))
    project.add("recordId", record.pop("rcn"))
    project.add("summary", record.pop("objective"))
    project.add("modifiedAt", record.pop("contentUpdateDate"))
    project.add("doi", record.pop("grantDoi"))
    project = ensure_publisher(ctx, project)

    ctx.emit(project)

    amount = to_numeric(record.pop("ecMaxContribution"))
    if amount and int(amount) > 0:
        eu = EU
        payment = ctx.make("Payment")
        payment.id = ctx.make_id("payment", eu.id, project.id)
        payment.add("amount", amount)
        payment.add("amountEur", amount)
        payment.add("currency", "EUR")
        payment.add("startDate", project.get("startDate"))
        payment.add("endDate", project.get("endDate"))
        payment.add("programme", project.get("program"))
        payment.add("purpose", f'Max. EC project contribution for "{project.caption}"')
        payment.add("project", project)
        payment.add("payer", eu)
        payment = ensure_publisher(ctx, payment)

        ctx.emit(eu)
        ctx.emit(payment)


SCHEMAS = {
    "PRC": ("Company", None),
    "HES": ("Organization", "higher education institute"),
    "PUB": ("PublicBody", None),
    "REC": ("Organization", "research institute"),
}


def parse_organization(ctx: Context, record: SDict):
    schema, legal_form = SCHEMAS.get(record.pop("activityType"), ("LegalEntity", None))
    proxy = ctx.make(schema)
    country = record.pop("country").lower()
    if country == "el":
        country = "gr"
    if country == "uk":
        country = "gb"
    country = get_country_code(country)
    name = record.pop("name")
    vat_id, org_id = record.pop("organisationID"), record.pop("vatNumber")
    if vat_id:
        if country and not vat_id.lower().startswith(country):
            vat_id = f"{country.upper()}{vat_id}"
        proxy.id = ctx.make_slug(vat_id)
        proxy.add("taxNumber", vat_id)
        proxy.add("vatCode", vat_id)
    elif org_id:
        proxy.id = ctx.make_slug("organization", org_id)
    else:
        proxy.id = ctx.make_id(country or "xx", make_fingerprint(name))

    proxy.add("country", country)
    proxy.add("legalForm", legal_form)
    proxy.add("name", name)
    proxy.add("weakAlias", record.pop("shortName"))
    # FIXME parse address
    proxy.add("website", record.pop("organizationURL"))

    project = make_project(ctx, record)

    participation = ctx.make("ProjectParticipant")
    participation.id = ctx.make_id(project.id, proxy.id)
    participation.add("project", project)
    participation.add("participant", proxy)
    participation.add("role", record.pop("role"))
    participation.add("modifiedAt", record.pop("contentUpdateDate"))

    amount = to_numeric(record.pop("ecContribution"))
    payment = ctx.make("Payment")
    payment.id = ctx.make_id("payment", participation.id)
    payment.add("payer", EU)
    payment.add("beneficiary", proxy)
    payment.add("project", project)
    payment.add("amount", amount)
    payment.add("amountEur", amount)
    payment.add("currency", "EUR")

    ctx.emit(proxy)
    ctx.emit(payment)
    ctx.emit(project)
    ctx.emit(participation)


HANDLERS = {
    "euroSciVoc": parse_euroscivoc,
    "webLink": parse_urls,
    "legalBasis": parse_legalbasis,
    "project": parse_project,
    "organization": parse_organization,
}


def handle(ctx: Context, record: SDict, ix: int) -> CEGenerator:
    ctx = ctx.task()
    handler = HANDLERS.get(record.pop("_type"))
    if handler:
        handler(ctx, record)

    return ctx
