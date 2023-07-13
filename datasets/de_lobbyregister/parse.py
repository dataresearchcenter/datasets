from enum import Enum
from typing import Any

import orjson
from fingerprints import generate as fp
from followthemoney.util import make_entity_id
from nomenklatura.entity import CE
from zavod import Zavod, init_context

URL = "https://www.lobbyregister.bundestag.de/sucheDetailJson?sort=REGISTRATION_DESC"


class EntityType(Enum):
    NATURAL = "Person"
    ORGANIZATION = "Organization"


def make_address(context: Zavod, data) -> CE:
    proxy = context.make("Address")
    city = data["city"]
    country = data["country"]["code"]
    proxy.add("city", city)
    proxy.add("country", country)
    if data["type"] == "FOREIGN":
        street = data["internationalAdditional1"]
        proxy.add("full", ", ".join((street, city, country)))
    elif data["type"] == "POSTBOX":
        zipCode = data["zipCode"]
        proxy.add("postalCode", zipCode)
    else:
        street = data["street"] + " " + data["streetNumber"]
        zipCode = data["zipCode"]
        proxy.add("full", ", ".join((street, zipCode, city, country)))
        proxy.add("postalCode", zipCode)
    proxy.id = context.make_slug("addr", make_entity_id(fp(proxy.caption)))
    return proxy


def make_person(context: Zavod, org_ident: str, data: dict[str, Any]) -> CE:
    proxy = context.make("Person")
    title = data.pop("academicDegreeBefore", None)
    firstName = data.pop("commonFirstName")
    lastName = data.pop("lastName")
    if title is not None:
        proxy.add("name", f"{title} {firstName} {lastName}")
        proxy.add("title", title)
    else:
        proxy.add("name", f"{firstName} {lastName}")

    proxy.add("firstName", firstName)
    proxy.add("lastName", lastName)
    proxy.add("phone", data.pop("phoneNumber", None))
    for email in data.pop("organizationMemberEmails", []):
        proxy.add("email", email)
    ident = make_entity_id(fp(proxy.caption), org_ident)
    proxy.id = context.make_slug("person", ident)
    return proxy


def make_representation(
    context: Zavod, agent: CE, client: CE, role: str | None = "Auftraggeber"
) -> CE:
    rel = context.make("Representation")
    ident = make_entity_id(client.id, agent.id)
    rel.id = context.make_slug("representation", ident)
    rel.add("client", client)
    rel.add("agent", agent)
    rel.add("role", role)
    return rel


def wrangle_organisazion(context: Zavod, proxy: CE, data: dict[str, Any]) -> CE:
    proxy.add("name", data.pop("name"))
    proxy.add("phone", data.pop("phoneNumber"))

    address_proxy = make_address(context, data.pop("address"))
    context.emit(address_proxy)

    proxy.add("addressEntity", address_proxy)
    proxy.add("address", address_proxy.caption)

    legalForm = data.pop("legalForm")
    proxy.add("legalForm", legalForm.pop("code_de"))
    proxy.add("summary", legalForm.get("legalFormText"))

    for email in data.pop("organizationEmails"):
        proxy.add("email", email)
    for url in data.pop("websites"):
        proxy.add("website", url)

    for person_data in data.pop("legalRepresentatives", []):
        person = make_person(context, proxy.id, person_data)
        context.emit(person)

        rel = make_representation(context, person, proxy, person_data.pop("function"))
        context.emit(rel)

    for person_data in data.pop("namedEmployees", []):
        person = make_person(context, proxy.id, person_data)
        context.emit(person)

        rel = context.make("Employment")
        rel.id = context.make_slug("employment", make_entity_id(person.id, proxy.id))
        rel.add("employer", proxy)
        rel.add("employee", person)
        context.emit(rel)

    for name in data.pop("membershipEntries", []):
        org = context.make("Organization")
        org.add("name", name)
        org.id = context.make_slug("org", make_entity_id(fp(name)))
        context.emit(org)

        rel = context.make("Membership")
        rel.id = context.make_slug("membership", make_entity_id(proxy.id, org.id))
        rel.add("organization", org)
        rel.add("member", proxy)
        context.emit(rel)

    return proxy


def parse_record(context: Zavod, record: dict[str, Any]):
    registerId = record.pop("registerNumber")
    record = record.pop("registerEntryDetail")
    proxy_data = record.pop("lobbyistIdentity")

    proxy_type = EntityType[proxy_data.pop("identity")]

    if proxy_type == EntityType.NATURAL:
        proxy = make_person(context, "", proxy_data)
        proxy.id = context.make_slug(registerId)
    else:
        proxy = context.make(proxy_type.value)
        proxy.id = context.make_slug(registerId)
        proxy = wrangle_organisazion(context, proxy, proxy_data)
        proxy.add("keywords", [k.get("de") for k in record.pop("fieldsOfInterest")])

    proxy.add("sourceUrl", record.pop("detailsPageUrl"))
    proxy.add(
        "status",
        "active" if record["account"]["activeLobbyist"] else "inactive",  # noqa
    )
    proxy.add("description", record.pop("activityDescription"))
    activity = record.pop("activity")

    notes = activity.get("de", activity.get("text"))
    proxy.add("notes", notes)

    context.emit(proxy)

    for item in record.pop("donators"):
        payer = context.make("LegalEntity")
        payer.add("name", item["name"])
        payer.add("address", item.get("location"))
        payer.id = context.make_slug(
            "donator", make_entity_id("donator", proxy.id, payer.caption)
        )
        context.emit(payer)

        payment = context.make("Payment")
        payment.add("payer", payer)
        payment.add("beneficiary", proxy)
        payment.add("purpose", item.pop("description"))
        payment.add("programme", item.pop("categoryType"))
        payment.add("startDate", item.pop("fiscalYearStart"))
        payment.add("endDate", item.pop("fiscalYearend", None))
        payment.add("amountEur", item["donationEuro"]["from"])
        payment.add("amountEur", item["donationEuro"]["to"])
        payment.id = context.make_slug("payment", make_entity_id(payer.id, proxy.id))
        context.emit(payment)

    for data in record.pop("clientOrganizations"):
        org = context.make("Organization")
        name = data["name"]
        ident = make_entity_id(fp(name))
        org.id = context.make_slug("org", ident)
        org = wrangle_organisazion(context, org, data)
        context.emit(org)

        rel = make_representation(context, proxy, org)
        context.emit(rel)

    for data in record.get("clientPersons"):
        person = make_person(context, proxy.id, data)
        context.emit(person)

        rel = make_representation(context, proxy, person)
        context.emit(rel)


def parse(context: Zavod):
    data_fp = context.fetch_resource("source.json", URL)
    with open(data_fp) as f:
        data = orjson.loads(f.read())
        ix = 0
        for ix, record in enumerate(data["results"]):
            parse_record(context, record)
            if ix and ix % 1_000 == 0:
                context.log.info("Parse record %d ..." % ix)
        if ix:
            context.log.info("Parsed %d records." % (ix + 1), fp=data_fp.name)


if __name__ == "__main__":
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
