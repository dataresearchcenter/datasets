from followthemoney.proxy import make_entity_id
from investigraph import Context
from investigraph.types import CE, CEGenerator, Record
from investigraph.util import clean_name as n
from investigraph.util import data_checksum
from investigraph.util import fingerprint as fp
from investigraph.util import join_text, string_id


def make_address(ctx: Context, record: Record) -> CE:
    proxy = ctx.make("Address")
    street = n(record.pop("beneficiary_street"))
    city = n(record.pop("beneficiary_city"))
    postalCode = n(record.pop("beneficiary_postcode"))
    country = n(record.pop("beneficiary_country"))
    full = join_text(street, postalCode, city, country, sep=", ")
    proxy.id = f"addr-{make_entity_id(full)}"
    proxy.add("full", full)
    proxy.add("street", street)
    proxy.add("postalCode", postalCode)
    proxy.add("city", city)
    proxy.add("country", country)
    return proxy


def make_project(ctx: Context, record: Record) -> CE | None:
    proxy = ctx.make("Project")
    ident = record.pop("project_identifier")
    name = record.pop("project_name")
    if "Information is not available" in (name, ident):
        return
    if n(ident):
        proxy.id = ctx.make_slug("project", string_id(ident))
        proxy.add("name", ident)
    elif n(name):
        proxy.id = ctx.make_slug("project", string_id(name))
        proxy.add("name", name)
    else:
        return

    proxy.add("startDate", record["project_startDate"])
    proxy.add("endDate", record["project_endDate"])
    proxy.add("date", record["date"])
    proxy.add("program", record.pop("program"))
    return proxy


def make_payer(ctx: Context, record: Record) -> CE | None:
    name = record.pop("payer")
    if fp(name):
        proxy = ctx.make("PublicBody", name=name, country="eu")
        proxy.id = ctx.make_id(fp(name))
        return proxy


def make_payment(ctx: Context, record: Record) -> CE:
    proxy = ctx.make("Payment")
    amount = record.pop("payment_amount")
    proxy.add("amountEur", amount)
    proxy.add("amount", amount)
    proxy.add("currency", "EUR")
    proxy.add("startDate", record["project_startDate"])
    proxy.add("endDate", record["project_endDate"])
    proxy.add("date", record["date"])
    proxy.add("recordId", record.pop("payment_recordId"))
    return proxy


def make_project_participation(
    ctx: Context,
    participant: CE,
    project: CE,
    record: Record,
    role: str | None = None,
) -> CE:
    proxy = ctx.make("ProjectParticipant")
    proxy.id = ctx.make_id(project.id, participant.id)
    proxy.add("participant", participant)
    proxy.add("project", project)
    proxy.add("startDate", project.first("startDate"))
    proxy.add("endDate", project.first("endDate"))
    if role is None:
        role = "coordinator" if record["beneficiary_role"] == "Yes" else None
    proxy.add("role", role)
    return proxy


def make_beneficiary(ctx: Context, record: Record) -> CE:
    beneficiary_type = record.pop("beneficiary_type")
    name = record.pop("beneficiary_name")
    ident = make_entity_id(fp(name))
    assert ident is not None

    if "NATURAL PERSON" in name:
        proxy = ctx.make_proxy("Person")
        proxy.id = ctx.make_slug("person", data_checksum(record))
    elif beneficiary_type.lower() == "private persons":
        proxy = ctx.make_proxy("Person")
    elif beneficiary_type.lower() == "private companies":
        proxy = ctx.make_proxy("Company")
    elif beneficiary_type.lower() == "public bodies":
        proxy = ctx.make_proxy("PublicBody")
    elif beneficiary_type.lower() == "third states":
        proxy = ctx.make_proxy("PublicBody")
    elif (
        "agencies" in beneficiary_type.lower()
        or "organisations" in beneficiary_type.lower()
    ):
        proxy = ctx.make_proxy("Organization")
    else:
        proxy = ctx.make_proxy("LegalEntity")

    if fp(record["beneficiary_vatCode"]):
        ident = record.pop("beneficiary_vatCode")
        proxy.add("vatCode", ident)

    if proxy.id is None:
        proxy.id = ctx.make_slug(ident)

    proxy.add("legalForm", beneficiary_type)
    proxy.add("name", name)
    return proxy


def handle(ctx: Context, record: Record, ix: int) -> CEGenerator:
    # exclude empty beneficiary names
    if fp(record["beneficiary_name"]):
        beneficiary = make_beneficiary(ctx, record)
        address = make_address(ctx, record)
        project = make_project(ctx, record)
        payer = make_payer(ctx, record)
        payment = make_payment(ctx, record)

        beneficiary.add("country", address.first("country"))
        beneficiary.add("address", address.caption)
        beneficiary.add("addressEntity", address)

        yield beneficiary
        yield address

        payment.add("beneficiary", beneficiary)

        if project is not None:
            yield make_project_participation(ctx, beneficiary, project, record)

            payment.id = ctx.make_id(
                "payment", project.id, beneficiary.id, payment.first("recordId")  # noqa
            )
            payment.add("project", project)
            payment.add("purpose", project.caption)
            yield project
        else:
            payment.id = ctx.make_id(
                "payment", beneficiary.id, payment.first("date")
            )  # noqa

        if payer is not None:
            payment.add("payer", payer)
            yield payer

            if project is not None:
                yield make_project_participation(
                    ctx, payer, project, record, role="Responsible department"
                )
        yield payment
