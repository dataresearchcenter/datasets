from enum import Enum

from banal import ensure_dict
from followthemoney.util import make_entity_id
from ftmq.util import clean_string, make_fingerprint, get_country_code
from investigraph.exceptions import DataError
from investigraph.model import SourceContext, TaskContext
from investigraph.types import CEGenerator, Record
from investigraph.util import join_text, make_fingerprint_id
from nomenklatura.entity import CE


class EntityType(Enum):
    NATURAL = "Person"
    ORGANIZATION = "Organization"


DEFAULT_COUNTRY = {"code": "de"}


def make_address(context: TaskContext, data: Record) -> CE:
    proxy = context.make_proxy("Address")
    city = data["city"]
    country = data.pop("country", DEFAULT_COUNTRY)["code"]
    zipCode = None
    street = None
    extras = []
    if data["type"] == "FOREIGN":
        street = data["internationalAdditional1"]
    elif data["type"] == "POSTBOX":
        zipCode = data["zipCode"]
    else:
        street = join_text(data.get("street"), data.get("streetNumber"))
        extras = [data.get("nationalAdditional1"), data.get("nationalAdditional1")]
        zipCode = data.get("zipCode")
    full = join_text(street, *extras, zipCode, city, sep=", ")
    proxy.id = context.make_slug(
        get_country_code(country), make_fingerprint_id(full), prefix="addr"
    )
    proxy.add("full", full)
    proxy.add("postalCode", zipCode)
    proxy.add("city", city)
    proxy.add("country", country)
    proxy.add("remarks", extras)
    return proxy


def make_person(context: TaskContext, org_ident: str, data: Record) -> CE | None:
    proxy = context.make_proxy("Person")
    title = data.pop("academicDegreeBefore", None)
    firstName = data.pop("firstName", data.get("commonFirstName"))
    lastName = data.pop("lastName", None)
    name = join_text(title, firstName, lastName)
    if name is None:
        context.log.warning("No person names")
        return
    ident = make_entity_id(make_fingerprint(name), org_ident)
    proxy.id = context.make_slug("person", ident)
    proxy.add("name", name)
    proxy.add("title", title)
    proxy.add("firstName", firstName)
    proxy.add("lastName", lastName)
    if data.get("recentGovernmentFunctionPresent"):
        proxy.add("topics", "gov")
    contact = data.pop("contactDetails", None)
    if contact:
        proxy.add("phone", contact.get("phoneNumber"))
        proxy.add("email", [e["email"] for e in contact.get("emails", [])])
    proxy.add("phone", data.pop("phoneNumber", None))
    for email in data.pop("organizationMemberEmails", []):
        proxy.add("email", email)
    return proxy


def make_representation(
    context: TaskContext, agent: CE, client: CE, role: str | None = "Auftraggeber"
) -> CE:
    rel = context.make_proxy("Representation")
    ident = make_entity_id(client.id, agent.id)
    rel.id = context.make_slug("representation", ident)
    rel.add("client", client)
    rel.add("agent", agent)
    rel.add("role", role)
    return rel


def init_organization(
    context: TaskContext, data: Record, schema: str | None = "Organization"
) -> CE:
    proxy = context.make_proxy(schema)
    if data.get("referenceName") and data.get("referenceDetailsPageUrl"):
        data["name"] = data.pop("referenceName")
        ident = data["referenceDetailsPageUrl"].split("/")[-1]
        proxy.id = context.make_slug(ident)
    elif data.get("name") or data.get("referenceName"):
        data["name"] = data.get("name", data.get("referenceName"))
        ident = make_fingerprint_id(data["name"])
        proxy.id = context.make_slug("org", ident)
    else:
        raise DataError("Invalid data for `init_organization`")

    return proxy


def make_organization(context: TaskContext, proxy: CE, data: Record) -> CE:
    proxy.add("name", data.get("name"))
    legalForm = data.get("legalForm", {})
    proxy.add("legalForm", legalForm.get("de"))
    proxy.add("summary", legalForm.get("legalFormText"))

    if "adress" in data:
        address_proxy = make_address(context, data.pop("address"))
        context.emit(address_proxy)
        proxy.add("addressEntity", address_proxy)
        proxy.add("address", address_proxy.caption)

    if data.get("capitalCityRepresentationPresent"):
        capital_address = make_address(
            context, data["capitalCityRepresentation"].pop("address")
        )
        context.emit(capital_address)
        proxy.add("addressEntity", capital_address)
        proxy.add("address", capital_address.caption)
        contact = data["capitalCityRepresentation"].pop("contactDetails")
        proxy.add("phone", contact.get("phoneNumber"))
        proxy.add("email", contact.get("email"))
        proxy.add("website", contact.get("website"))

    contact = data.pop("contactDetails", None)
    if contact:
        proxy.add("phone", contact.pop("phoneNumber", None))
        for email in contact.pop("emails", []):
            proxy.add("email", email["email"])
        for website in contact.pop("websites", []):
            proxy.add("website", website["website"])

    for person_data in data.pop("legalRepresentatives", []):
        person = make_person(context, proxy.id, person_data)
        if person:
            person.add("topics", "role.lobby")
            context.emit(person)
            rel = make_representation(
                context, person, proxy, person_data.pop("function")
            )
            context.emit(rel)

    for person_data in data.pop("entrustedPersons", []):
        person = make_person(context, proxy.id, person_data)
        if person:
            person.add("topics", "role.lobby")
            context.emit(person)
            role = person_data.pop("function", "entrusted_person")
            rel = make_representation(context, person, proxy, role)
            context.emit(rel)

    for person_data in data.pop("namedEmployees", []):
        person = make_person(context, proxy.id, person_data)
        if person:
            context.emit(person)
            rel = context.make_proxy("Employment")
            rel.id = context.make_slug(
                "employment", make_entity_id(person.id, proxy.id)
            )
            rel.add("employer", proxy)
            rel.add("employee", person)
            context.emit(rel)

    for membership in data.pop("memberships", []):
        org = context.make_proxy("Organization")
        name = membership.pop("membership")
        org.id = context.make_slug("org", make_fingerprint_id(name))
        org.add("name", name)
        context.emit(org)

        rel = context.make_proxy("Membership")
        rel.id = context.make_slug("membership", make_entity_id(proxy.id, org.id))
        rel.add("organization", org)
        rel.add("member", proxy)
        context.emit(rel)

    return proxy


def make_ministry(context: TaskContext, data: Record) -> CE:
    ident = data.pop("shortTitle")
    proxy = context.make_proxy("PublicBody")
    proxy.id = context.make_slug(ident)
    proxy.add("name", data.pop("title"))
    proxy.add("weakAlias", ident)
    proxy.add("topics", "gov.executive")
    proxy.add("website", data.get("url"))
    return proxy


def make_law(context: TaskContext, data: Record, project: CE) -> CE:
    proxy = context.make_proxy("Article")
    title = data.pop("shortTitle")
    proxy.id = context.make_slug("law", title)
    proxy.add("title", title)
    proxy.add("summary", data.pop("title"))
    proxy.add("sourceUrl", data.pop("url"))
    rel = context.make_proxy("Documentation")
    rel.id = context.make_id("affected-law", project.id, proxy.id)
    rel.add("document", proxy)
    rel.add("entity", project)
    rel.add("role", "affected_law")
    return proxy


def make_bill(context: TaskContext, data: Record, project: CE, org: CE) -> CE:
    proxy = context.make_proxy("Project")
    title = data.pop("title", data.get("customTitle"))
    if title is None:
        raise DataError("No title for `make_bill`")
    proxy.id = context.make_id("draft-bill", title)
    proxy.add("name", title)
    proxy.add("date", data.pop("publicationDate", data.get("customDate")))

    rel = context.make_proxy("UnknownLink")
    rel.id = context.make_id("draft-bill", project.id, proxy.id)
    rel.add("subject", project)
    rel.add("object", proxy)
    rel.add("role", "Gesetzesentwurf")
    context.emit(rel)

    participant = context.make_proxy("ProjectParticipant")
    participant.id = context.make_id("draft-bill-participant", org.id, proxy.id)
    participant.add("participant", org)
    participant.add("project", proxy)
    participant.add("role", "Gesetzesentwurf")
    context.emit(participant)

    for ministry in data.pop("leadingMinistries"):
        participant = make_ministry(context, ministry)
        context.emit(participant)
        rel = context.make_proxy("ProjectParticipant")
        rel.id = context.make_id("bill-participant", proxy.id, participant.id)
        rel.add("project", proxy)
        rel.add("participant", participant)
        rel.add("sourceUrl", ministry.get("draftBillProjectUrl"))
        rel.add("sourceUrl", ministry.get("draftBillDocumentUrl"))
        context.emit(rel)


def make_project(context: TaskContext, data: Record, org: CE) -> CE:
    proxy = context.make_proxy("Project")
    ident = data.pop("regulatoryProjectNumber")
    proxy.id = context.make_slug(ident)
    proxy.add("projectId", ident)
    proxy.add("name", data.get("title", data.get("regulatoryProjectTitle")))
    proxy.add("description", clean_string(data.get("description")))
    proxy.add("keywords", [i["de"] for i in data.get("fieldsOfInterest", [])])
    proxy.add("sourceUrl", data.get("pdfUrl"))

    rel = context.make_proxy("ProjectParticipant")
    rel.id = context.make_id("participant", proxy.id, org.id)
    rel.add("project", proxy)
    rel.add("participant", org)
    context.emit(rel)

    for law in data.pop("affectedLaws", []):
        context.emit(make_law(context, law, proxy))

    if data.pop("draftBillPresent", []):
        context.emit(make_bill(context, data.pop("draftBill"), proxy, org))

    if data.get("printedMattersPresent"):
        for matter in data["printedMatters"]:
            doc = context.make_proxy("Document")
            foreign_id = matter.pop("printingNumber")
            doc.id = context.make_slug("document", foreign_id)
            doc.add("title", matter.get("title"))
            doc.add("publisher", matter.pop("issuer"))
            url = matter.get("documentUrl")
            if url:
                doc.add("sourceUrl", url)
                doc.add("fileName", url.split("/")[-1])
            context.emit(doc)
            rel = context.make_proxy("Documentation")
            rel.id = context.make_id("matter", proxy.id, doc.id)
            rel.add("document", doc)
            rel.add("entity", proxy)
            rel.add("role", "printed_matter")
            context.emit(rel)
            for ministry in matter.pop("leadingMinistries"):
                ministry = make_ministry(context, ministry)
                context.emit(ministry)
                rel = context.make_proxy("Documentation")
                rel.id = context.make_id("matter", doc.id, ministry.id)
                rel.add("document", doc)
                rel.add("entity", ministry)
                rel.add("role", "leading_ministry")
                context.emit(rel)

    return proxy


def make_contract(context: TaskContext, data: Record, org: CE) -> CE:
    proxy = context.make_proxy("Contract")
    description = clean_string(data.pop("description"))
    proxy.id = context.make_id("contract", org.id, description)
    proxy.add("title", description)
    proxy.add("keywords", [i["de"] for i in data.pop("fieldsOfInterest")])

    for client in data["clients"]["clientOrganizations"]:
        authority = init_organization(context, client)
        authority = make_organization(context, authority, client)
        proxy.add("authority", authority)
        context.emit(authority)
    if data["clients"]["clientPersons"]:
        for person in data["clients"]["clientPersons"]:
            person = make_person(context, org.id, person)
            proxy.add("authority", person)
            context.emit(person)

    contractors = data.pop("contractors")
    suppliers = []
    for contractor in contractors["contractorOrganizations"]:
        supplier = init_organization(context, contractor)
        suppliers.append(make_organization(context, supplier, contractor))
    for contractor in contractors["contractorPersons"]:
        supplier = make_person(context, org.id, contractor)
        if "companyName" in contractor:
            company_data = {"name": contractor.pop("companyName")}
            comp = init_organization(context, company_data, "Company")
            comp = make_organization(context, org, company_data)
            context.emit(comp)
            rel = context.make_proxy("Employment")
            rel.id = context.make_id("contractor-employment", comp.id, supplier.id)
            rel.add("employer", comp)
            rel.add("employee", supplier)
            context.emit(rel)
            suppliers.append(comp)
        suppliers.append(supplier)
    for contractor in contractors["entrustedPersons"]:
        suppliers.append(make_person(context, org.id, contractor))

    for supplier in suppliers:
        if supplier:
            award = context.make_proxy("ContractAward")
            award.id = context.make_id("award", proxy.id, supplier.id)
            award.add("contract", proxy)
            award.add("supplier", supplier)
            context.emit(supplier)
            context.emit(award)

    return proxy


def make_statement(context: TaskContext, data: Record, org: CE) -> CE:
    project = make_project(context, data, org)
    context.emit(project)

    dates = [i["sendingDate"] for i in data["recipientGroups"]]
    proxy = context.make_proxy("Article")
    proxy.id = context.make_id("statement", project.id, org.id, *dates)
    proxy.add("title", f"Stellungnahme von {org.caption} zu {project.caption}")
    proxy.add("summary", data.pop("text")["text"])
    proxy.add("publishedAt", dates)

    rel = context.make_proxy("Documentation")
    rel.id = context.make_id("project-statement", project.id, proxy.id)
    rel.add("document", proxy)
    rel.add("entity", project)
    rel.add("date", dates)
    rel.add("role", "Stellungnahme")
    context.emit(rel)

    for group in data.pop("recipientGroups"):
        for recipient in group.pop("recipients"):
            org = context.make_proxy("PublicBody")
            org.id = context.make_id("statement-recipient", recipient["code"])
            org.add("name", recipient["de"])
            org.add("topics", "gov")
            context.emit(org)
            rel = context.make_proxy("Documentation")
            rel.id = context.make_id(
                "statement-recipient-rel", proxy.id, org.id, *dates
            )
            rel.add("document", proxy)
            rel.add("entity", org)
            rel.add("role", "Empfänger von Stellungnahme")
            rel.add("date", dates)
            context.emit(rel)

    return proxy


def parse_record(context: TaskContext, data: Record):
    registerId = data.pop("registerNumber")
    record = data.pop("registerEntryDetails")
    proxy_data = data.pop("lobbyistIdentity")

    proxy_type = EntityType[proxy_data.pop("identity")]

    if proxy_type == EntityType.NATURAL:
        proxy = make_person(context, "", proxy_data)
        if proxy is None:
            context.log.warning("No person data")
            return
        proxy.id = context.make_slug(registerId)
    else:
        proxy = context.make_proxy(proxy_type.value)
        proxy.id = context.make_slug(registerId)
        proxy = make_organization(context, proxy, proxy_data)

    activities = data.pop("activitiesAndInterests")
    proxy.add("idNumber", registerId)
    proxy.add("summary", activities.pop("activity"))
    proxy.add("description", clean_string(activities.pop("activityDescription")))
    proxy.add("keywords", [i["de"] for i in activities.pop("fieldsOfInterest")])
    proxy.add(
        "notes", [i["de"] for i in activities.pop("typesOfExercisingLobbyWork", [])]
    )
    proxy.add("sourceUrl", record.pop("detailsPageUrl"))
    proxy.add("sourceUrl", record.pop("pdfUrl"))
    proxy.add(
        "status",
        "active" if data["accountDetails"]["activeLobbyist"] else "inactive",  # noqa
    )

    context.emit(proxy)

    if data["donators"].get("donatorsInformationPresent"):
        start_date = data["donators"].get("relatedFiscalYearStart")
        end_date = data["donators"].get("relatedFiscalYearEnd")
        for item in data["donators"].pop("donators"):
            payer = context.make_proxy("LegalEntity")
            name = item.pop("name")
            payer.id = context.make_slug(
                "donator",
                make_entity_id("donator", proxy.id, make_fingerprint(name)),
            )
            payer.add("name", name)
            payer.add("address", item.get("location"))
            context.emit(payer)

            payment = context.make_proxy("Payment")
            payment.id = context.make_id(
                "payment", payer.id, proxy.id, start_date, end_date
            )
            payment.add("payer", payer)
            payment.add("beneficiary", proxy)
            payment.add("purpose", clean_string(item.pop("description")))
            payment.add("startDate", start_date)
            payment.add("endDate", end_date)
            amounts = item.pop("donationEuro")
            payment.add("amountEur", amounts["from"])
            payment.add("amountEur", amounts["to"])
            context.emit(payment)

    if data["clientIdentity"] and data["clientIdentity"].get("clientsPresent"):
        clients = data.pop("clientIdentity")
        for client in clients.pop("clientOrganizations", []):
            org = init_organization(context, client)
            org = make_organization(context, org, client)
            context.emit(org)
            rel = make_representation(context, proxy, org)
            context.emit(rel)

        for client in clients.pop("clientPersons", []):
            person = make_person(context, proxy.id, client)
            if person:
                context.emit(person)
                rel = make_representation(context, proxy, person)
                context.emit(rel)

    if data["contracts"] and data["contracts"].pop("contractsPresent"):
        for contract in data.pop("contracts").pop("contracts"):
            context.emit(make_contract(context, contract, proxy))

    if data["statements"] and data["statements"].pop("statementsPresent"):
        for statement in data.pop("statements").pop("statements"):
            context.emit(make_statement(context, statement, proxy))

    for project in ensure_dict(data.pop("regulatoryProjects")).pop(
        "regulatoryProjects", []
    ):
        context.emit(make_project(context, project, proxy))

    for payment in data.pop("publicAllowances").pop("publicAllowances", []):
        payer = context.make_proxy("PublicBody")
        payer_name = payment.pop("name")
        payer.id = context.make_slug(payer_name)
        if payer.id:
            payer.add("name", payer_name)
            payer.add("legalForm", payment.get("type", {}).get("de"))
            context.emit(payer)
            description = clean_string(payment.pop("description"))
            payment = context.make_proxy("Payment")
            payment.id = context.make_id(
                "payment", payer.id, proxy.id, make_fingerprint(description)
            )
            payment.add("payer", payer)
            payment.add("beneficiary", proxy)
            payment.add("purpose", description)
            payment.add("programme", "Öffentliche Zuwendungen")
            amounts = payment.pop("publicAllowanceEuro")
            if amounts:
                payment.add("amountEur", [amounts["from"], amounts["to"]])
            context.emit(payment)


def handle(ctx: SourceContext, record: Record, ix: int) -> CEGenerator:
    tx = ctx.task()
    parse_record(tx, record)
    yield from tx
