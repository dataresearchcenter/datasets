import re
from dateparser import parse as _parse_date
from enum import StrEnum
from typing import Any, Tuple

from followthemoney.util import make_entity_id
from ftmq.util import get_country_code
from investigraph import Context
from investigraph.types import CE, CEGenerator, Record
from investigraph.util import join_text, make_fingerprint_id


class EntityType(StrEnum):
    SIDEJOB = "sidejob"
    POLITICIAN = "politician"
    ORGANIZATION = "sidejob_organization"
    MANDATE = "candidacy_mandate"
    PARTY = "party"


def parse_date(value: str | None) -> str | None:
    if not value:
        return
    value = value.strip()
    if len(value) == 4:
        return value
    date = _parse_date(value, locales=["de"])
    if date is not None:
        date = date.isoformat()
        # only month
        if sum(c.isdigit() for c in value) == 4:
            return date[:7]
        return date[:10]


def extract_date_range(value: str) -> Tuple[str | None, str | None]:
    if value:
        # 'ab 29.11.2023'
        # 'ab Dezember 2022'
        # 'ab 2022'
        m = re.match(r".*(ab|von)\s([\w\s]+)\).*", str(value))
        if m is not None:
            return parse_date(m.groups()[1]), None

        # 'bis 29.11.2023'
        # 'bis April 2022'
        # 'bis 2022'
        m = re.match(r".*bis\s([\w\s]+)\).*", str(value))
        if m is not None:
            return None, parse_date(m.groups()[0])

        # 'Einkommen im Jahr 2024'
        m = re.match(r".*(\d{4}).*", str(value))
        if m is not None:
            for year in m.groups():
                return year, year
        # '(Bundestag 2021 - 2025)''
        m = re.match(r".*(\d{4})\s?-\s?(\d{4}).*", str(value))
        if m is not None:
            return m.groups()[0], m.groups()[1]
    return None, None


def extract_date_ranges(*values: str) -> Tuple[str | None, str | None]:
    for value in values:
        res = extract_date_range(value)
        if res[0] or res[1] is not None:
            return res
    return None, None


def make_link(context: Context, politician: CE, organization: CE, data: Record) -> CE:
    rel = context.make("UnknownLink")
    rel.id = context.make_slug("sidejob", data.pop("id"))

    label = data.pop("label")
    rel.add("subject", politician)
    rel.add("object", organization)
    rel.add("role", label)
    return rel


def make_directorship(
    context: Context, director: CE, organization: CE, data: Record
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


def make_membership(context: Context, member: CE, organization: CE, data: Record) -> CE:
    rel = context.make("Membership")
    rel.id = context.make_slug("membership", make_entity_id(member.id, organization.id))

    if data:
        rel.id = context.make_slug("sidejob", data.pop("id"))
        label = data.pop("label")
        description = data.pop("job_title_extra")
        rel.add("role", label)
        rel.add("description", description)
        rel.add("modifiedAt", data.pop("data_change_date"))
        rel.add("summary", data.pop("additional_information"))
    rel.add("member", member)
    rel.add("organization", organization)

    return rel


def make_address(context: Context, data: Record) -> CE:
    country = data["field_country"].pop("label")
    city = None
    if data.get("field_city"):
        city = data.pop("field_city")["label"]
    full = join_text(city, country, sep=", ")
    proxy = context.make("Address")
    proxy.id = context.make_slug(
        get_country_code(country) or "xx",
        make_fingerprint_id(full),
        prefix="addr",
    )
    proxy.add("full", full)
    proxy.add("country", country)
    proxy.add("city", city)
    return proxy


def make_politician(context: Context, data: Record) -> CE:
    proxy = context.make("Person")
    proxy.id = context.make_slug("person", data.pop("id"))
    title = data.get("field_title")
    firstName = data.get("first_name")
    lastName = data.get("last_name")
    proxy.add("name", join_text(title, firstName, lastName))
    proxy.add("title", title)
    proxy.add("topics", "role.pep")
    proxy.add("firstName", firstName)
    proxy.add("lastName", lastName)
    proxy.add("phone", data.get("phoneNumber"))
    for email in data.pop("organizationMemberEmails", []):
        proxy.add("email", email)
    proxy.add("sourceUrl", data.get("api_url"))
    proxy.add("gender", data.get("sex"))
    proxy.add("birthDate", data.get("year_of_birth"))
    proxy.add("education", data.get("education"))
    proxy.add("wikidataId", data.get("qid_wikidata"))
    return proxy


def make_organization(context: Context, data: Record, entity_type: EntityType) -> CE:
    proxy = context.make("Organization")
    proxy.id = context.make_slug("organization", data.pop("id"))
    proxy.add("name", data.pop("label"))
    proxy.add("sourceUrl", data.pop("api_url"))

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


def parse_sidejob(context: Context, record: Record) -> CEGenerator:
    for mandate in record["mandates"]:
        politician_data = mandate.get("politician")
        if politician_data:
            politician = make_politician(context, politician_data)
            yield politician
            if record["sidejob_organization"]:
                if record["sidejob_organization"].get("id"):
                    organization = make_organization(
                        context, record["sidejob_organization"], EntityType.ORGANIZATION
                    )
                    yield organization

                    label = record["label"]
                    start_date, end_date = extract_date_ranges(
                        label, record["job_title_extra"], mandate["label"]
                    )
                    # Choose proxy type by record label.
                    # TODO: Find a more reliable way.
                    if label.startswith("Mitglied"):
                        proxy = make_membership(
                            context, politician, organization, record
                        )
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
                        proxy = make_directorship(
                            context, politician, organization, record
                        )
                    else:
                        # TODO: Use suitable type for other (this doesn't fit in general, e.g. for "Beteiligung" or "Reisekosten")
                        proxy = make_link(context, politician, organization, record)
                    # TODO: Find other suitable models. What about Interval, Other link or Payment?

                    # TODO: Where to save income?
                    income_level = record.get("income_level") or "Unbekanntes Einkommen"
                    proxy.add("indexText", f"income_level#{income_level}")
                    income = record.get("income")
                    if income:
                        proxy.add("indexText", f"income#{income}")
                    proxy.add("startDate", start_date)
                    proxy.add("endDate", end_date)
                    proxy.add("sourceUrl", record.pop("api_url"))
                    yield proxy
                else:
                    context.log.warning("No ID for `sidejob_organization`")
    # TODO: Handle sidejobs with no organization.
    return


def parse_politician(context: Context, record: Record) -> CEGenerator:
    proxy = make_politician(context, record)
    party = make_organization(context, record["party"], EntityType.PARTY)
    yield proxy
    yield party
    yield make_membership(context, proxy, party, {})


def handle(ctx: Context, record: dict[str, Any], ix: int) -> CEGenerator:
    tx = ctx.task()
    if record["entity_type"] == EntityType.POLITICIAN:
        yield from parse_politician(tx, record)
    if record["entity_type"] == EntityType.ORGANIZATION:
        yield make_organization(tx, record, EntityType.ORGANIZATION)
    if record["entity_type"] == EntityType.SIDEJOB:
        yield from parse_sidejob(tx, record)
    yield from tx
