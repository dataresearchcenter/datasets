from followthemoney import EntityProxy
from ftmq.util import make_fingerprint_id
from investigraph.model import SourceContext
from investigraph.types import RecordGenerator, Record
from investigraph.util import clean_name as n
from investigraph.util import make_data_checksum
from investigraph.util import make_fingerprint as fp
from investigraph.util import join_text, make_string_id
from followthemoney.types import registry


def make_address(ctx: SourceContext, record: Record) -> EntityProxy:
    proxy = ctx.make_entity("Address")
    street = n(record.pop("beneficiary_street"))
    city = n(record.pop("beneficiary_city"))
    postalCode = n(record.pop("beneficiary_postcode"))
    country = n(record.pop("beneficiary_country"))
    full = join_text(street, postalCode, city, country, sep=", ")
    proxy.id = f"addr-{make_fingerprint_id(full)}"
    proxy.add("full", full)
    proxy.add("street", street)
    proxy.add("postalCode", postalCode)
    proxy.add("city", city)
    proxy.add("country", country)
    return proxy


def make_project(ctx: SourceContext, record: Record) -> EntityProxy | None:
    proxy = ctx.make_entity("Project")
    ident = record.pop("project_identifier")
    name = record.pop("project_name")
    if "Information is not available" in (name, ident):
        return
    if n(ident):
        proxy.id = ctx.make_slug("project", make_string_id(ident))
        proxy.add("name", ident)
    elif n(name):
        proxy.id = ctx.make_slug("project", make_string_id(name))
        proxy.add("name", name)
    else:
        return

    proxy.add("startDate", record["project_startDate"])
    proxy.add("endDate", record["project_endDate"])
    proxy.add("date", record["date"])
    proxy.add("program", record.pop("program"))
    return proxy


def make_payer(ctx: SourceContext, record: Record) -> EntityProxy | None:
    name = record.pop("payer")
    if fp(name):
        id_ = ctx.make_id(fp(name))
        proxy = ctx.make_entity("PublicBody", id_)
        proxy.add("name", name)
        proxy.add("country", "eu")
        return proxy


def round_two_decimals_comma(ctx: SourceContext, amount: str) -> str:
    if amount:
        try:
            number_obj = registry.number
            float_amount = number_obj.to_number(amount)
            rounded = round(float_amount, 2)
            amount = f"{rounded:,.2f}"
            # amount = str(round(float_amount, 2))
        except Exception as e:
            ctx.log.warn(f"Unable to convert to float: {str(e)} : {amount}")
            pass
    return amount


def round_two_decimals(ctx: SourceContext, amount: str) -> str:
    if amount:
        try:
            number_obj = registry.number
            float_amount = number_obj.to_number(amount)
            amount = str(round(float_amount, 2))
        except Exception as e:
            ctx.log.error(f"Unable to convert to float: {str(e)}")
            pass
    return amount


def make_payment(
    ctx: SourceContext, record: Record, beneficiary: EntityProxy
) -> EntityProxy:
    proxy = ctx.make_entity("Payment")
    redacted = False
    proxy.id = ctx.make_id("payment", beneficiary.id, make_data_checksum(record))
    # amount = Beneficiary’s contracted amount -> commitment consumed amount
    # amount = record.pop("payment_amount")
    amount = record.pop("commitment_consumed_amount")
    if amount is None or amount == "*****":
        if amount == "*****":
            redacted = True
        amount = "0"

    amount = round_two_decimals_comma(ctx, amount)
    if redacted:
        amount = "REDACTED"

    proxy.add("amountEur", amount)
    proxy.add("amount", amount)
    proxy.add("currency", "EUR")
    proxy.add("startDate", record["project_startDate"])
    proxy.add("endDate", record["project_endDate"])
    proxy.add("date", record["date"])
    proxy.add("recordId", record.pop("payment_recordId"))
    # added other values in description
    contracted_amount = record.get("contracted_amount") or 0
    commitment_amount = record.get("commitment_contracted_amount") or 0
    commitment_total = record.get("commitment_total_amount") or 0
    description = []
    if contracted_amount == "*****":
        description.append("Beneficiary’s contracted amount (EUR): REDACTED")
    else:
        description.append(
            f"Beneficiary’s contracted amount (EUR): {round_two_decimals_comma(ctx, contracted_amount)}"
        )
    if commitment_amount == "*****":
        description.append("Commitment contracted amount (EUR): REDACTED")
    else:
        description.append(
            f"Commitment contracted amount (EUR): {round_two_decimals_comma(ctx, commitment_amount)}"
        )
    if commitment_total == "*****":
        description.append("Commitment total amount (EUR): REDACTED")
    else:
        description.append(
            f"Commitment total amount (EUR): {round_two_decimals_comma(ctx, commitment_total)}"
        )

    description.append(f"Commitment consumed amount (EUR): {amount}")

    # description = [
    #     f'Beneficiary’s contracted amount (EUR): {round_two_decimals_comma(ctx, contracted_amount)}',
    #     f'Commitment contracted amount (EUR): {round_two_decimals_comma(ctx, commitment_amount)}',
    #     f'Commitment total amount (EUR): {round_two_decimals_comma(ctx, commitment_total)}',
    #     f"Commitment consumed amount (EUR): {amount}",
    # ]
    proxy.add("description", description)
    return proxy


def make_project_participation(
    ctx: SourceContext,
    participant: EntityProxy,
    project: EntityProxy,
    record: Record,
    role: str | None = None,
) -> EntityProxy:
    proxy = ctx.make_entity("ProjectParticipant")
    proxy.id = ctx.make_id(project.id, participant.id)
    proxy.add("participant", participant)
    proxy.add("project", project)
    proxy.add("startDate", project.first("startDate"))
    proxy.add("endDate", project.first("endDate"))
    if role is None:
        role = "coordinator" if record["beneficiary_role"] == "Yes" else None
    proxy.add("role", role)
    return proxy


def make_beneficiary(ctx: SourceContext, record: Record) -> EntityProxy:
    beneficiary_type = record.pop("beneficiary_type")
    name = record.pop("beneficiary_name")
    ident = make_fingerprint_id(name)
    assert ident is not None

    if "NATURAL PERSON" in name:
        proxy = ctx.make_entity("Person")
        proxy.id = ctx.make_slug("person", make_data_checksum(record))
    elif beneficiary_type.lower() == "private persons":
        proxy = ctx.make_entity("Person")
        proxy.id = ctx.make_slug("person", make_data_checksum(record))
    elif beneficiary_type.lower() == "private companies":
        proxy = ctx.make_entity("Company")
    elif beneficiary_type.lower() == "public bodies":
        proxy = ctx.make_entity("PublicBody")
    elif beneficiary_type.lower() == "third states":
        proxy = ctx.make_entity("PublicBody")
    elif (
        "agencies" in beneficiary_type.lower()
        or "organisations" in beneficiary_type.lower()
    ):
        proxy = ctx.make_entity("Organization")
    else:
        proxy = ctx.make_entity("LegalEntity")

    if proxy.id is None:
        proxy.id = ctx.make_slug(ident)

    if fp(record["beneficiary_vatCode"]):
        ident = record.pop("beneficiary_vatCode")
        proxy.id = ctx.make_slug(ident.upper())
        proxy.add("vatCode", ident)
    elif record["beneficiary_vatCode"] == "*****":
        proxy.id = ctx.make_slug(ident)
        proxy.add("vatCode", ident)

    proxy.add("legalForm", beneficiary_type)
    proxy.add("name", name)
    return proxy


def handle(ctx: SourceContext, record: Record, ix: int) -> RecordGenerator:
    # exclude empty beneficiary names
    if fp(record["beneficiary_name"]):
        beneficiary = make_beneficiary(ctx, record)
        address = make_address(ctx, record)
        project = make_project(ctx, record)
        payer = make_payer(ctx, record)
        payment = make_payment(ctx, record, beneficiary)

        beneficiary.add("country", address.first("country"))
        beneficiary.add("address", address.caption)
        beneficiary.add("addressEntity", address)

        yield beneficiary
        yield address

        payment.add("beneficiary", beneficiary)

        if project is not None:
            yield make_project_participation(ctx, beneficiary, project, record)

            payment.add("project", project)
            payment.add("purpose", project.caption)
            yield project

        if payer is not None:
            payment.add("payer", payer)
            yield payer

            if project is not None:
                yield make_project_participation(
                    ctx, payer, project, record, role="Responsible department"
                )
        yield payment
    elif record["beneficiary_name"] == "*****":
        # with open("redacted.txt", "a") as f:
        #     f.write(f"{record}\n")
        checksum = make_data_checksum(record)
        prefix = checksum[:12]
        # ctx.log.error("redacted record")
        record["beneficiary_name"] = f"REDACTED_{prefix}"
        beneficiary = make_beneficiary(ctx, record)
        address = make_address(ctx, record)
        project = make_project(ctx, record)
        payer = make_payer(ctx, record)
        payment = make_payment(ctx, record, beneficiary)

        beneficiary.add("country", address.first("country"))
        beneficiary.add("address", address.caption)
        beneficiary.add("addressEntity", address)

        yield beneficiary
        yield address

        payment.add("beneficiary", beneficiary)

        if project is not None:
            yield make_project_participation(ctx, beneficiary, project, record)

            payment.add("project", project)
            payment.add("purpose", project.caption)
            yield project

        if payer is not None:
            payment.add("payer", payer)
            yield payer

            if project is not None:
                yield make_project_participation(
                    ctx, payer, project, record, role="Responsible department"
                )
        yield payment
