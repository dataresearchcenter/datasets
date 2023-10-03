from enum import Enum
import os
from typing import Any

import orjson, copy, urllib
from fingerprints import generate as fp
from followthemoney.util import make_entity_id
from nomenklatura.entity import CE
from os.path import dirname, join
from zavod import Zavod, init_context

URL = "https://www.abgeordnetenwatch.de/api/v2"
class EntityType(Enum):
    SIDEJOB = "Sidejob"
    POLITICIAN = "Politician"
    ORGANIZATION = "Organization"
    PARTY = "Party"


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
    rel.add("member", member)
    rel.add("organization", organization)
    if (data):
        label = data.pop("label")
        rel.add("role", label)
        rel.add("description", data.pop("job_title_extra"))
        rel.add("modifiedAt", data.pop("data_change_date"))
        rel.add("summary", data.pop("additional_information"))
        ident = make_entity_id(member.id, organization.id, label)
    else:
        ident = make_entity_id(member.id, organization.id)

    rel.id = context.make_slug("membership", ident)
    return rel

def make_address(context: Zavod, data) -> CE:
    proxy = context.make("Address")
    country = data["field_country"].pop("label")
    proxy.add("country", country)
    if (data["field_city"]):
        city = data["field_city"].pop("label")
        proxy.add("city", city)
        proxy.add("full", ", ".join((city, country)))
    else:
        proxy.add("full", country)
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
    ident = make_entity_id(data['id'], fp(proxy.caption), org_ident)
    proxy.id = context.make_slug("person", ident)
    return proxy


def make_organization(context: Zavod, data: dict[str, Any]) -> CE:
    proxy = context.make("Organization")
    proxy.add("name", data.pop("label"))

    if ("field_country" in data and data["field_country"]):
        addr = make_address(context, data)
        context.emit(addr)
        proxy.add("addressEntity", addr)
        proxy.add("address", addr.caption)

    if ("topics" in data and data["topics"]):
        # TODO: Use other property to save topics (Dip21)?
        proxy.add("keywords", [k.get("label") for k in data['topics']])

    ident = make_entity_id(data['id'], fp(proxy.caption))
    proxy.id = context.make_slug("organization", ident)
    return proxy


def parse_sidejob(context: Zavod, record: dict[str, Any]):
    proxy_data = record

    politician = parse_record(context, record["mandates"][0]["politician"], EntityType.POLITICIAN)
    if record["sidejob_organization"]:
        organization = parse_record(context, record["sidejob_organization"], EntityType.ORGANIZATION)

        label = record["label"]
        # Choose proxy type by record label. 
        # TODO: Find a more reliable way. 
        if label.startswith("Mitglied"):
            proxy = make_membership(context, politician, organization, proxy_data)
        elif label.startswith(("Fraktionsvorsitzender", "Vorsitzende", "Vorsitzender", "Vorstand", "Stellv.", "Erste Vorsitzende", "Erster Vorsitzender")):
            proxy = make_directorship(context, politician, organization, proxy_data)
        else:
            # TODO: Use suitable type for other (this doesn't fit in general, e.g. for "Beteiligung" or "Reisekosten")
            proxy = make_employment(context, organization, politician, proxy_data)
        # TODO: Find other suitable models. What about Interval, Other link or Payment?

        # TODO: Where to save income?
        # proxy.add("notes", record.pop("income_level", "Unbekanntes Einkommen"))
    else:
        # TODO: Handle sidejobs with no organization.
        proxy = None
    return proxy


def parse_politician(context: Zavod, record: dict[str, Any]):
    proxy_data = record
    proxy = make_person(context, "", proxy_data)
    proxy.add("notes", "Politiker") # TODO: Is there a better way to mark politicians?
    # TODO: Is there a better way to make party \o/?
    party = parse_record(context, proxy_data["party"], EntityType.PARTY)
    membership = make_membership(context, proxy, party, None)
    context.emit(membership)
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
    elif type == EntityType.ORGANIZATION or type == EntityType.PARTY:
        proxy = parse_organization(context, record)

    if (proxy):
        if (not proxy.id and record['id']):
            proxy.id = context.make_slug(record['id'])
        proxy.add("sourceUrl", record.pop("api_url"))
        # TODO: publisher?
        # TODO: publisherUrl?
        context.emit(proxy)
    return proxy


def parse(context: Zavod):
    sideJobsData = fetchAll(context, "/sidejobs", {"range_end": 1000})
    # The API doesn't return related data. 
    # To get the relevant entities and minimize the number of requests,
    # we collect their IDs, load them and merge the data before parsing.
    relatedMandateIds = set()
    relatedOrganizationIds = set()
    for sideJob in sideJobsData:
        for mandate in sideJob["mandates"]:
            relatedMandateIds.add(mandate["id"])
        if sideJob["sidejob_organization"]:
            relatedOrganizationIds.add(sideJob["sidejob_organization"]["id"])

    if (len(relatedMandateIds) > 0):
        mandatesData = fetchByIds(context, "/candidacies-mandates", relatedMandateIds, {"current_on": "all"})
        relatedPoliticianIds = set()
        for mandate in mandatesData:
            relatedPoliticianIds.add(mandate["politician"]["id"])

        if (len(relatedPoliticianIds) > 0):
            politiciansData = fetchByIds(context, "/politicians", relatedPoliticianIds)
            for mandatesRecord in mandatesData:
                for politiciansRecord in politiciansData:
                    if mandatesRecord["politician"]["id"] == politiciansRecord["id"]:
                        mandatesRecord["politician"] = copy.deepcopy(politiciansRecord)
                        break;

    if (len(relatedOrganizationIds) > 0):
        organizationsData = fetchByIds(context, "/sidejob-organizations", relatedOrganizationIds)

    ix = 0
    for ix, sideJobRecord in enumerate(sideJobsData):
        if sideJobRecord["sidejob_organization"] and organizationsData:
            # Attach organization to sidejob record.
            for organizationRecord in organizationsData:
                if sideJobRecord["sidejob_organization"]["id"] == organizationRecord["id"]:
                    sideJobRecord["sidejob_organization"] = copy.deepcopy(organizationRecord)
                    break;
        if mandatesData:
            # Attach mandates including its politician to sidejob record.
            im = 0
            for im, mandate in enumerate(sideJobRecord["mandates"]):
                for mandateRecord in mandatesData:
                    if mandate["id"] == mandateRecord["id"]:
                        sideJobRecord["mandates"][im] = copy.deepcopy(mandateRecord)
                        break;
        parse_record(context, sideJobRecord, EntityType.SIDEJOB)
        if ix and ix % 1_000 == 0:
            context.log.info("Parse sidejob record %d ..." % ix)
    if ix:
        context.log.info("Parsed %d sidejob records." % (ix + 1))


def fetchByIds(context: Zavod, path, ids = {}, queryParams = {}):
    # Fetch by IDs using chunks/batch to prevent too long URLs or bad gateway response.
    chunkSize = 150 # maximum number of IDs to request without failure
    data = []
    for idChunk in [list(ids)[i:i+chunkSize] for i in range(0, len(ids), chunkSize)]:
        queryParams["id[in]"] = "[" + ",".join(str(id) for id in idChunk) + "]"
        data += fetchAll(context, path, queryParams)
    return data


def fetchAll(context: Zavod, path, queryParams = {}):
    ix = 0
    range_start =  0
    range_end = 1000 # aka limit, default is 100, max. is 1.000
    dataAll = []

    # Fetch all entities with multiple requests (AW API has limit per request)
    while (range_start >= 0):
        ix+1
        queryParams["range_start"] = range_start
        queryParams["range_end"] = range_end
        query = urllib.parse.urlencode(queryParams)
        queryHash = hash(query)
        sourceFileSuffix = "_" + "".join(x for x in path if x.isalnum()) + "_" + str(queryHash)
        data_fp = context.fetch_resource("source" + sourceFileSuffix + ".json", URL + path + "?" + query)
        with open(data_fp) as f:
            data = orjson.loads(f.read())
            meta = data["meta"]

            total = int(meta["result"]["total"])
            range_end = int(meta["result"]["range_end"])
            if (total > (range_start + range_end)):
                range_start = range_start + range_end
            else:
                range_start = -1

            dataAll += data["data"]

    return dataAll


if __name__ == "__main__":
    print(os.path.abspath('.'))
    with init_context("metadata.yml") as context:
        context.export_metadata("export/index.json")
        parse(context)
