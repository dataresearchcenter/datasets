from enum import Enum
from typing import Any

import orjson, copy
from fingerprints import generate as fp
from followthemoney.util import make_entity_id
from nomenklatura.entity import CE
from zavod import Zavod, init_context

URL = "https://www.abgeordnetenwatch.de/api/v2"
class EntityType(Enum):
    SIDEJOB = "Sidejob"
    POLITICIAN = "Politician"
    ORGANIZATION = "Organization"


def make_employment(
    context: Zavod, employer: CE, employee: CE, data: dict[str, Any]
) -> CE:
    rel = context.make("Employment")
    label = data.pop("label")
    ident = make_entity_id(employer.id, employee.id, label)
    rel.id = context.make_slug("employment", ident)
    rel.add("employer", employer)
    rel.add("employee", employee)
    rel.add("role", label)
    return rel


def make_directorship(
    context: Zavod, director: CE, organization: CE, data: dict[str, Any]
) -> CE:
    rel = context.make("Directorship")
    label = data.pop("label")
    jobTitleExtra = data.pop("job_title_extra")
    additionalInformation = data.pop("additional_information")
    dataChangeDate = data.pop("data_change_date")
    ident = make_entity_id(director.id, organization.id, label)
    rel.id = context.make_slug("directorship", ident)
    rel.add("director", director)
    rel.add("organization", organization)
    rel.add("role", label)
    rel.add("description", jobTitleExtra)
    rel.add("modifiedAt", dataChangeDate)
    rel.add("summary", additionalInformation)
    return rel


def make_membership(
    context: Zavod, member: CE, organization: CE, data: dict[str, Any]
) -> CE:
    rel = context.make("Membership")
    label = data.pop("label")
    jobTitleExtra = data.pop("job_title_extra")
    additionalInformation = data.pop("additional_information")
    dataChangeDate = data.pop("data_change_date")
    ident = make_entity_id(member.id, organization.id, label)
    rel.id = context.make_slug("directorship", ident)
    rel.add("member", member)
    rel.add("organization", organization)
    rel.add("role", label)
    rel.add("description", jobTitleExtra)
    rel.add("modifiedAt", dataChangeDate)
    rel.add("summary", additionalInformation)
    return rel

# TODO: This is copied from lobbyregister. Adjust if needed or remove.
def make_address(context: Zavod, data) -> CE:
    proxy = context.make("Address")
    city = data["field_city"].pop("label")
    country = data["field_country"].pop("label")
    proxy.add("city", city)
    proxy.add("country", country)
    proxy.add("full", ", ".join((city, country)))
    proxy.id = context.make_slug("addr", make_entity_id(fp(proxy.caption)))
    return proxy


def make_person(context: Zavod, org_ident: str, data: dict[str, Any]) -> CE:
    proxy = context.make("Person")
    title = data.pop("field_title", None)
    firstName = data.pop("first_name")
    lastName = data.pop("last_name")
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

def make_organization(context: Zavod, data: dict[str, Any]) -> CE:
    proxy = context.make("Organization")
    proxy.add("name", data.pop("label"))

    if (data['field_country']):
        addr = make_address(context, data)
        context.emit(addr)
        proxy.add("addressEntity", addr)
        proxy.add("address", addr.caption)

    topics = data.pop("field_topics")
    if (topics):
        # TODO: Use other property to save topics (Dip21)?
        proxy.add("keywords", [k.get("label") for k in topics])

    return proxy


# TODO: This is copied from lobbyregister. Adjust if needed or remove.
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


# TODO: This is copied from lobbyregister. Adjust if needed or remove.
def wrangle_organization(context: Zavod, proxy: CE, data: dict[str, Any]) -> CE:
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


def parse_sidejob(context: Zavod, record: dict[str, Any]):
    proxy_data = record

    politician = parse_record(context, record["mandates"][0]['politician'], EntityType.POLITICIAN)
    organization = parse_record(context, record['sidejob_organization'], EntityType.ORGANIZATION)

    label = record["label"]

    # Choose proxy type by record label. 
    # TODO: Find a more reliable way. 
    if label.startswith("Mitglied"):
        proxy = make_membership(context, politician, organization, proxy_data)
    elif label.startswith(("Vorstand", "Stellv.")):
        proxy = make_directorship(context, politician, organization, proxy_data)
    else:
        # TODO: Use suitable type for other (this doesn't fit in general, e.g. for "Reisekosten")
        proxy = make_employment(context, organization, politician, proxy_data)

    # TODO: Where to save income?
    # proxy.add("notes", record.pop("income_level", "Unbekanntes Einkommen"))

    return proxy
    

def parse_politician(context: Zavod, record: dict[str, Any]):
    proxy_data = record
    proxy = make_person(context, "", proxy_data)
    proxy.add("notes", "Politiker") # TODO: Is there a better way to mark politicians?

    # TODO: Make party \o/

    return proxy


def parse_organization(context: Zavod, record: dict[str, Any]):
    proxy_data = record
    proxy = make_organization(context, proxy_data)

    return proxy


def parse_record(context: Zavod, record: dict[str, Any], type: str):
    if type == EntityType.SIDEJOB:
        proxy = parse_sidejob(context, record)
    elif type == EntityType.POLITICIAN:
        proxy = parse_politician(context, record)
    elif type == EntityType.ORGANIZATION:
        proxy = parse_organization(context, record)

    proxy.id = context.make_slug(record.pop("id"))
    proxy.add("sourceUrl", record.pop("api_url"))
    # TODO: publisher?
    # TODO: publisherUrl?
    context.emit(proxy)
    return proxy


# TODO Remove
def parse_record_old(context: Zavod, record: dict[str, Any]):
    registerId = record.pop("registerNumber")
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
    # TODO: This won't collect all entities. Use options and loop to fetch all.
    sideJobsData_fp = context.fetch_resource("source_sidejobs.json", URL + "/sidejobs")
    # The API doesn't return related data. 
    # To get the relevant entities and minimize the number of requests,
    # we collect their IDs, load them and merge the data before parsing.
    relatedMandateIds = set()
    relatedOrganizationIds = set()
    with open(sideJobsData_fp) as f:
        sideJobsData = orjson.loads(f.read())
        for sideJob in sideJobsData["data"]:
            for mandate in sideJob["mandates"]:
                relatedMandateIds.add(mandate["id"])
            relatedOrganizationIds.add(sideJob["sidejob_organization"]["id"])

    if (len(relatedMandateIds) > 0):
        mandates_url = URL + '/candidacies-mandates?current_on=all&id[in]=[' + ','.join(str(mid) for mid in relatedMandateIds) + ']'
        mandatesData_fp = context.fetch_resource("source_mandates.json", mandates_url)
        relatedPoliticianIds = set()
        with open(mandatesData_fp) as f:
            mandatesData = orjson.loads(f.read())
            for mandate in mandatesData["data"]:
                relatedPoliticianIds.add(mandate['politician']['id'])

        if (len(relatedPoliticianIds) > 0):
            politicians_url = URL + '/politicians?id[in]=[' + ','.join(str(pid) for pid in relatedPoliticianIds) + ']'
            politiciansData_fp = context.fetch_resource("source_politicians.json", politicians_url)
            with open(politiciansData_fp) as f:
                politiciansData = orjson.loads(f.read())
                # Attach politician to mandate record.
                for mandatesRecord in mandatesData["data"]:
                    for politiciansRecord in politiciansData["data"]:
                        if mandatesRecord["politician"]["id"] == politiciansRecord["id"]:
                            mandatesRecord["politician"] = copy.deepcopy(politiciansRecord)
                            break;

    if (len(relatedOrganizationIds) > 0):
        # Load organizations for all sidejobs.
        organizations_url = URL + '/sidejob-organizations?id[in]=[' + ','.join(str(mid) for mid in relatedOrganizationIds) + ']'
        data_fp = context.fetch_resource("source_sidejob_organizations.json", organizations_url)
        with open(data_fp) as f:
            organizationsData = orjson.loads(f.read())

    ix = 0
    for ix, sideJobRecord in enumerate(sideJobsData["data"]):
        if organizationsData:
            # Attach organization to sidejob record.
            for organizationRecord in organizationsData["data"]:
                if sideJobRecord["sidejob_organization"]["id"] == organizationRecord["id"]:
                    sideJobRecord["sidejob_organization"] = copy.deepcopy(organizationRecord)
                    break;
        if mandatesData:
            # Attach mandates including its politician to sidejob record.
            im = 0
            for im, mandate in enumerate(sideJobRecord['mandates']):
                for mandateRecord in mandatesData["data"]:
                    if mandate["id"] == mandateRecord["id"]:
                        sideJobRecord["mandates"][im] = copy.deepcopy(mandateRecord)
                        break;
        parse_record(context, sideJobRecord, EntityType.SIDEJOB)
        if ix and ix % 1_000 == 0:
            context.log.info("Parse sidejob record %d ..." % ix)
    if ix:
        context.log.info("Parsed %d sidejob records." % (ix + 1), fp=data_fp.name)

def fetchAll(path):
    range_start = 0
    dataAll = []
    while (range_start >= 0):
        # TODO json file name should be unique or according to path and options.
        data_fp = context.fetch_resource("source.json", URL + path)
        with open(data_fp) as f:
            data = orjson.loads(f.read())
            meta = data["meta"]

            total = int(meta["result"]["total"])
            limit = int(meta["result"]["range_end"])
            if (total > (range_start + limit)):
                range_start = range_start + limit
            else:
                range_start = -1

            dataAll += data["data"]

    return dataAll


if __name__ == "__main__":
    # TODO: Had to change path to metadata.yml to make vscode debugging work. Fix this in launch.json
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
