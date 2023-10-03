import re
from enum import Enum
from typing import Any

from followthemoney.util import make_entity_id
from ftmq.util import get_country_code
from investigraph import Context
from investigraph.types import CE, CEGenerator
from investigraph.util import fingerprint as fp


class EntityType(Enum):
    SIDEJOB = "Sidejob"
    POLITICIAN = "Politician"
    ORGANIZATION = "Organization"
    PARTY = "Party"


def extract_year(value: str) -> str | None:
    if value:
        m = re.match(r".*(\d{4}).*", str(value))
        if m is not None:
            for year in m.groups():
                return year


def make_link(
    context: Context, politician: CE, organization: CE, data: dict[str, Any]
) -> CE:
    rel = context.make("UnknownLink")
    rel.id = context.make_slug("sidejob", data.pop("id"))

    label = data.pop("label")
    rel.add("subject", politician)
    rel.add("object", organization)
    rel.add("role", label)

    return rel


def make_directorship(
    context: Context, director: CE, organization: CE, data: dict[str, Any]
) -> CE:
    rel = context.make("Directorship")
    rel.id = context.make_slug("sidejob", data.pop("id"))

    label = data.pop("label")
    jobTitleExtra = data.pop("job_title_extra")
    additionalInformation = data.pop("additional_information")
    dataChangeDate = data.pop("data_change_date")
    rel.add("director", director)
    rel.add("organization", organization)
    rel.add("role", label)
    rel.add("description", jobTitleExtra)
    rel.add("modifiedAt", dataChangeDate)
    rel.add("summary", additionalInformation)

    return rel


def make_membership(
    context: Context, member: CE, organization: CE, data: dict[str, Any]
) -> CE:
    rel = context.make("Membership")
    rel.id = context.make_slug("membership", make_entity_id(member.id, organization.id))

    if data:
        rel.id = context.make_slug("sidejob", data.pop("id"))
        label = data.pop("label")
        description = data.pop("job_title_extra")
        rel.add("role", label)
        rel.add("date", extract_year(description))
        rel.add("description", description)
        rel.add("modifiedAt", data.pop("data_change_date"))
        rel.add("summary", data.pop("additional_information"))
    rel.add("member", member)
    rel.add("organization", organization)

    return rel


def make_address(context: Context, data: dict[str, Any]) -> CE:
    proxy = context.make("Address")
    country = data["field_country"].pop("label")
    proxy.add("country", country)
    if data["field_city"]:
        city = data["field_city"].pop("label")
        proxy.add("city", city)
        proxy.add("full", ", ".join((city, country)))
    else:
        proxy.add("full", country)
    proxy.id = context.make_slug(
        get_country_code(country) or "xx",
        make_entity_id(fp(proxy.caption)),
        prefix="addr",
    )
    return proxy


def make_politician(context: Context, data: dict[str, Any]) -> CE:
    proxy = context.make("Person")
    proxy.id = context.make_slug("person", data.pop("id"))

    title = data.pop("field_title", None)
    firstName = data.pop("first_name")
    lastName = data.pop("last_name")
    if title is not None:
        proxy.add("name", f"{title} {firstName} {lastName}")
        proxy.add("title", title)
    else:
        proxy.add("name", f"{firstName} {lastName}")

    proxy.add("topics", "role.pep")
    proxy.add("firstName", firstName)
    proxy.add("lastName", lastName)
    proxy.add("phone", data.pop("phoneNumber", None))
    for email in data.pop("organizationMemberEmails", []):
        proxy.add("email", email)
    return proxy


def make_organization(
    context: Context, data: dict[str, Any], entity_type: EntityType
) -> CE:
    proxy = context.make("Organization")
    proxy.id = context.make_slug("organization", data.pop("id"))

    proxy.add("name", data.pop("label"))

    if entity_type == EntityType.PARTY:
        proxy.add("topics", "pol.party")

    if "field_country" in data and data["field_country"]:
        addr = make_address(context, data)
        context.emit(addr)
        proxy.add("addressEntity", addr)
        proxy.add("address", addr.caption)

    if data.get("topics"):
        # TODO: Use other property to save topics (Dip21)?
        proxy.add("keywords", [k.get("label") for k in data["topics"]])

    return proxy


def parse_sidejob(context: Context, record: dict[str, Any]):
    politician = parse_record(
        context, record["mandates"][0]["politician"], EntityType.POLITICIAN
    )
    if record["sidejob_organization"]:
        organization = parse_record(
            context, record["sidejob_organization"], EntityType.ORGANIZATION
        )

        label = record["label"]
        # Choose proxy type by record label.
        # TODO: Find a more reliable way.
        if label.startswith("Mitglied"):
            proxy = make_membership(context, politician, organization, record)
        elif label.startswith(
            (
                "Fraktionsvorsitzender",
                "Vorsitzende",
                "Vorsitzender",
                "Vorstand",
                "Stellv.",
                "Erste Vorsitzende",
                "Erster Vorsitzender",
            )
        ):
            proxy = make_directorship(context, politician, organization, record)
        else:
            # TODO: Use suitable type for other (this doesn't fit in general, e.g. for "Beteiligung" or "Reisekosten")
            proxy = make_link(context, politician, organization, record)
        # TODO: Find other suitable models. What about Interval, Other link or Payment?

        # TODO: Where to save income?
        # proxy.add("notes", record.pop("income_level", "Unbekanntes Einkommen"))
    else:
        # TODO: Handle sidejobs with no organization.
        proxy = None
    return proxy


def parse_politician(context: Context, record: dict[str, Any]):
    proxy = make_politician(context, record)
    party = make_organization(context, record["party"], EntityType.PARTY)
    membership = make_membership(context, proxy, party, None)
    context.emit(membership)
    return proxy


def parse_record(context: Context, record: dict[str, Any], entity_type: EntityType):
    if entity_type == EntityType.SIDEJOB:
        proxy = parse_sidejob(context, record)
    elif entity_type == EntityType.POLITICIAN:
        proxy = parse_politician(context, record)
    elif entity_type == EntityType.ORGANIZATION or entity_type == EntityType.PARTY:
        proxy = make_organization(context, record, entity_type)

    if proxy:
        if not proxy.id and record["id"]:
            proxy.id = context.make_slug(record["id"])
        proxy.add("sourceUrl", record.pop("api_url"))
        # TODO: publisher?
        # TODO: publisherUrl?
        context.emit(proxy)
    return proxy


def handle(ctx: Context, record: dict[str, Any], ix: int) -> CEGenerator:
    tx = ctx.task()
    parse_record(tx, record, EntityType.SIDEJOB)
    return tx
