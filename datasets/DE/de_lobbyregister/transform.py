import os
from enum import Enum

from banal import ensure_dict
from followthemoney import E
from followthemoney.util import make_entity_id
from ftmq.types import Entities
from ftmq.util import clean_string, get_country_code, make_fingerprint
from investigraph.exceptions import DataError
from investigraph.model import SourceContext, TaskContext
from investigraph.types import Record
from investigraph.util import join_text, make_fingerprint_id


class EntityType(Enum):
    NATURAL = "Person"
    ORGANIZATION = "Organization"


def format_euro_range(amounts: dict | None) -> str | None:
    """Format a Euro amount range as a string."""
    if not amounts:
        return None
    from_amt = amounts.get("from")
    to_amt = amounts.get("to")
    if from_amt is None and to_amt is None:
        return None
    if from_amt == to_amt:
        return f"{from_amt:,} EUR".replace(",", ".")
    return f"{from_amt:,} - {to_amt:,} EUR".replace(",", ".")


DEFAULT_COUNTRY = {"code": "de"}


def clean_path(value: str, limit: int | None = None) -> str:
    """A single archive path component - no separators, no runaway length."""
    value = " ".join(value.replace("/", "-").replace("\\", "-").split())
    if limit is not None and len(value) > limit:
        # back off to the last word boundary instead of cutting mid-word
        value = value[:limit].rsplit(" ", 1)[0]
    return value.strip(" .-,")


def lobbyist_folders(org: E) -> list[str]:
    """Archive folders for everything belonging to one register entry."""
    return ["Lobbyisten", join_text(org.first("idNumber"), "-", org.caption)]


def archive_document(
    context: TaskContext, proxy: E, url: str, title: str, folders: list[str]
) -> None:
    """Archive a referenced file and add what the lakehouse knows about it.

    Does nothing without a `LAKEHOUSE_URI` - there is nowhere to put the file
    then, and the entity keeps its `sourceUrl` either way.
    """
    if not os.environ.get("LAKEHOUSE_URI"):
        return
    suffix = url.rsplit("/", 1)[-1].rsplit(".", 1)
    name = f"{clean_path(title, 120)}.{suffix[-1] if len(suffix) > 1 else 'pdf'}"
    path = "/".join([*(clean_path(f) for f in folders), name])
    try:
        file = context.fetch(
            url,
            cache_key=proxy.id,
            id=proxy.id,
            key=path,
            name=name,
            title=title,
            sourceUrl=url,
        )
    except Exception as exc:
        context.log.warning("Cannot archive document", url=url, error=str(exc))
        return
    proxy.add("contentHash", file.checksum)
    proxy.add("fileSize", file.size)
    proxy.add("mimeType", file.mimetype)
    proxy.add("parent", file.parent)
    for folder in file.make_parents():
        context.emit(folder)


def make_document(
    context: TaskContext,
    entity: E,
    url: str,
    *,
    role: str,
    title: str,
    folders: list[str],
    key: str | None = None,
    dates: list[str] | None = None,
) -> E:
    """Emit a `Document` for a pdf the register references, tied to `entity`.

    The document is keyed by `key` where the source offers something more
    stable than the url (which changes whenever a file is republished), so an
    updated file replaces the old one instead of piling up next to it. Where a
    `LAKEHOUSE_URI` is configured the file is archived under `folders` on top
    of that, and the resulting checksum and folder are added to the same entity.
    """
    proxy = context.make_entity("Document")
    proxy.id = context.make_id(role, entity.id, key or url)
    proxy.add("title", title)
    proxy.add("sourceUrl", url)
    proxy.add("fileName", url.split("/")[-1])
    proxy.add("mimeType", "application/pdf")
    proxy.add("date", dates)

    archive_document(context, proxy, url, title, folders)
    context.emit(proxy)

    rel = context.make_entity("Documentation")
    rel.id = context.make_id("documentation", entity.id, proxy.id)
    rel.add("document", proxy)
    rel.add("entity", entity)
    rel.add("role", role)
    rel.add("date", dates)
    context.emit(rel)
    return proxy


def make_address(context: TaskContext, data: Record) -> E:
    proxy = context.make_entity("Address")
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
        extras = [data.get("nationalAdditional1"), data.get("nationalAdditional2")]
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


def make_person(context: TaskContext, org_ident: str, data: Record) -> E | None:
    proxy = context.make_entity("Person")
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

    # Extract revolving door / government function details
    if data.get("recentGovernmentFunctionPresent"):
        proxy.add("topics", "role.pep")
        gov_func = data.get("recentGovernmentFunction", {})
        func_type = gov_func.get("type", {})
        func_type_code = func_type.get("code") if isinstance(func_type, dict) else None
        # func_type_name = func_type.get("de") if isinstance(func_type, dict) else None

        # Extract position name and organization based on function type
        position_name = None
        org_entity = None  # Will hold the PublicBody entity
        if gov_func.get("houseOfRepresentatives"):
            hr = gov_func["houseOfRepresentatives"]
            func = hr.get("function")
            if isinstance(func, dict):
                position_name = func.get("de")
            if hr.get("functionPosition"):
                position_name = (
                    f"{position_name} ({hr['functionPosition']})"
                    if position_name
                    else hr["functionPosition"]
                )
            # Create PublicBody for Bundestag
            org_entity = context.make_entity("PublicBody")
            org_entity.id = context.make_slug("org", "bundestag")
            org_entity.add("name", "Deutscher Bundestag")
            org_entity.add("country", "de")
            org_entity.add("topics", "gov.national")
            org_entity.add("website", "https://www.bundestag.de")
            context.emit(org_entity)
        elif gov_func.get("federalGovernment"):
            fg = gov_func["federalGovernment"]
            func = fg.get("function")
            if isinstance(func, dict):
                position_name = func.get("de")
            ministry = fg.get("ministry")
            if ministry and isinstance(ministry, dict):
                ministry_title = ministry.get("title")
                ministry_short = ministry.get("shortTitle")
                if ministry_title:
                    if position_name:
                        position_name = f"{position_name}, {ministry_title}"
                    # Create PublicBody for the specific ministry
                    org_entity = context.make_entity("PublicBody")
                    org_entity.id = context.make_slug(
                        "org", ministry_short or make_fingerprint(ministry_title)
                    )
                    org_entity.add("name", ministry_title)
                    org_entity.add("weakAlias", ministry_short)
                    org_entity.add("country", "de")
                    org_entity.add("topics", "gov.executive")
                    org_entity.add("website", ministry.get("url"))
                    context.emit(org_entity)
            if not org_entity:
                # Fallback to general Bundesregierung
                org_entity = context.make_entity("PublicBody")
                org_entity.id = context.make_slug("org", "bundesregierung")
                org_entity.add("name", "Bundesregierung")
                org_entity.add("country", "de")
                org_entity.add("topics", "gov.executive")
                org_entity.add("website", "https://www.bundesregierung.de")
                context.emit(org_entity)
        elif gov_func.get("federalAdministration"):
            fa = gov_func["federalAdministration"]
            func = fa.get("function") if isinstance(fa, dict) else None
            if isinstance(func, dict):
                position_name = func.get("de")
            # Create PublicBody for Bundesverwaltung
            org_entity = context.make_entity("PublicBody")
            org_entity.id = context.make_slug("org", "bundesverwaltung")
            org_entity.add("name", "Bundesverwaltung")
            org_entity.add("country", "de")
            org_entity.add("topics", "gov.executive")
            context.emit(org_entity)

        # Create Position and Occupancy entities
        if position_name:
            # Create Position entity
            position = context.make_entity("Position")
            position.id = context.make_id(
                "position", func_type_code or "gov", make_fingerprint(position_name)
            )
            position.add("name", position_name)
            position.add("country", "de")
            if org_entity:
                position.add("organization", org_entity)
            if func_type_code == "HOUSE_OF_REPRESENTATIVES":
                position.add("topics", "gov.national")
            elif func_type_code == "FEDERAL_GOVERNMENT":
                position.add("topics", "gov.executive")
            elif func_type_code == "FEDERAL_ADMINISTRATION":
                position.add("topics", "gov.executive")
            context.emit(position)

            # Create Occupancy entity linking person to position
            occupancy = context.make_entity("Occupancy")
            occupancy.id = context.make_id("occupancy", proxy.id, position.id)
            occupancy.add("holder", proxy)
            occupancy.add("post", position)
            end_date = gov_func.get("endDate")
            if end_date:
                occupancy.add("endDate", end_date)
            if not gov_func.get("ended", False):
                occupancy.add("status", "current")
            context.emit(occupancy)

    contact = data.pop("contactDetails", None)
    if contact:
        proxy.add("phone", contact.get("phoneNumber"))
        proxy.add("email", [e["email"] for e in contact.get("emails", [])])
    proxy.add("phone", data.pop("phoneNumber", None))
    for email in data.pop("organizationMemberEmails", []):
        proxy.add("email", email)
    return proxy


def make_representation(
    context: TaskContext, agent: E, client: E, role: str | None = "Auftraggeber"
) -> E:
    rel = context.make_entity("Representation")
    ident = make_entity_id(client.id, agent.id)
    rel.id = context.make_slug("representation", ident)
    rel.add("client", client)
    rel.add("agent", agent)
    rel.add("role", role)
    return rel


def init_organization(
    context: TaskContext, data: Record, schema: str | None = "Organization"
) -> E:
    proxy = context.make_entity(schema)
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


def make_organization(context: TaskContext, proxy: E, data: Record) -> E:
    proxy.add("name", data.get("name"))
    legalForm = data.get("legalForm", {})
    proxy.add("legalForm", legalForm.get("de"))
    proxy.add("summary", legalForm.get("legalFormText"))

    if "address" in data:
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
            rel = context.make_entity("Employment")
            rel.id = context.make_slug(
                "employment", make_entity_id(person.id, proxy.id)
            )
            rel.add("employer", proxy)
            rel.add("employee", person)
            context.emit(rel)

    for membership in data.pop("memberships", []):
        org = context.make_entity("Organization")
        name = membership.pop("membership")
        org.id = context.make_slug("org", make_fingerprint_id(name))
        org.add("name", name)
        context.emit(org)

        rel = context.make_entity("Membership")
        rel.id = context.make_slug("membership", make_entity_id(proxy.id, org.id))
        rel.add("organization", org)
        rel.add("member", proxy)
        context.emit(rel)

    return proxy


def make_ministry(context: TaskContext, data: Record) -> E:
    ident = data.pop("shortTitle")
    proxy = context.make_entity("PublicBody")
    proxy.id = context.make_slug(
        "org", ident
    )  # Add "org" prefix for namespace consistency
    proxy.add("name", data.pop("title"))
    proxy.add("weakAlias", ident)
    proxy.add("country", "de")
    proxy.add("topics", "gov.executive")
    proxy.add("website", data.get("url"))
    return proxy


def make_law(context: TaskContext, data: Record, project: E) -> E:
    proxy = context.make_entity("Article")
    title = data.pop("shortTitle")
    proxy.id = context.make_slug("law", title)
    proxy.add("title", title)
    proxy.add("summary", data.pop("title"))
    proxy.add("sourceUrl", data.pop("url"))
    rel = context.make_entity("Documentation")
    rel.id = context.make_id("affected-law", project.id, proxy.id)
    rel.add("document", proxy)
    rel.add("entity", project)
    rel.add("role", "affected_law")
    return proxy


def make_bill(context: TaskContext, data: Record, project: E, org: E) -> E:
    proxy = context.make_entity("Project")
    title = data.pop("title", data.get("customTitle"))
    if title is None:
        raise DataError("No title for `make_bill`")
    # Include project.id to ensure uniqueness - same bill title for different
    # projects = different IDs
    proxy.id = context.make_id("draft-bill", project.id, title)
    proxy.add("name", title)
    proxy.add("date", data.pop("publicationDate", data.get("customDate")))

    rel = context.make_entity("UnknownLink")
    rel.id = context.make_id("draft-bill", project.id, proxy.id)
    rel.add("subject", project)
    rel.add("object", proxy)
    rel.add("role", "Gesetzesentwurf")
    context.emit(rel)

    participant = context.make_entity("ProjectParticipant")
    participant.id = context.make_id("draft-bill-participant", org.id, proxy.id)
    participant.add("participant", org)
    participant.add("project", proxy)
    participant.add("role", "Gesetzesentwurf")
    context.emit(participant)

    for ministry in data.pop("leadingMinistries"):
        # `make_ministry` pops the titles, so read the urls first
        project_url = ministry.get("draftBillProjectUrl")
        document_url = ministry.get("draftBillDocumentUrl")
        participant = make_ministry(context, ministry)
        context.emit(participant)
        rel = context.make_entity("ProjectParticipant")
        rel.id = context.make_id("bill-participant", proxy.id, participant.id)
        rel.add("project", proxy)
        rel.add("participant", participant)
        rel.add("sourceUrl", project_url)
        rel.add("sourceUrl", document_url)
        context.emit(rel)

        if document_url:
            make_document(
                context,
                proxy,
                document_url,
                role="Referentenentwurf",
                title=join_text("Referentenentwurf:", title),
                folders=[
                    "Referentenentwürfe",
                    participant.first("weakAlias") or participant.caption,
                ],
            )

    return proxy


def make_project(context: TaskContext, data: Record, org: E) -> E:
    proxy = context.make_entity("Project")
    ident = data.pop("regulatoryProjectNumber")
    proxy.id = context.make_slug(ident)
    proxy.add("projectId", ident)
    proxy.add("name", data.get("title", data.get("regulatoryProjectTitle")))
    proxy.add("description", clean_string(data.get("description")))
    proxy.add("keywords", [i["de"] for i in data.get("fieldsOfInterest", [])])
    # a `pdfUrl` here belongs to the statement this project was built from, not
    # to the project - `make_statement` documents it on the statement itself

    rel = context.make_entity("ProjectParticipant")
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
            doc = context.make_entity("Document")
            foreign_id = matter.pop("printingNumber")
            # keyed by the printing number so the same paper stays one entity
            # across all the projects referencing it
            doc.id = context.make_slug("document", foreign_id)
            doc.add("title", matter.get("title"))
            issuer = matter.pop("issuer")
            doc.add("publisher", issuer)
            url = matter.get("documentUrl")
            if url:
                doc.add("sourceUrl", url)
                doc.add("fileName", url.split("/")[-1])
                archive_document(
                    context,
                    doc,
                    url,
                    join_text("Drucksache", foreign_id, "-", matter.get("title")),
                    ["Drucksachen", issuer or "Unbekannt"],
                )
            context.emit(doc)
            rel = context.make_entity("Documentation")
            rel.id = context.make_id("matter", proxy.id, doc.id)
            rel.add("document", doc)
            rel.add("entity", proxy)
            rel.add("role", "printed_matter")
            context.emit(rel)
            for ministry in matter.pop("leadingMinistries"):
                ministry = make_ministry(context, ministry)
                context.emit(ministry)
                rel = context.make_entity("Documentation")
                rel.id = context.make_id("matter", doc.id, ministry.id)
                rel.add("document", doc)
                rel.add("entity", ministry)
                rel.add("role", "leading_ministry")
                context.emit(rel)

    return proxy


def make_contract(context: TaskContext, data: Record, org: E) -> E:
    proxy = context.make_entity("Contract")
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
            rel = context.make_entity("Employment")
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
            award = context.make_entity("ContractAward")
            award.id = context.make_id("award", proxy.id, supplier.id)
            award.add("contract", proxy)
            award.add("supplier", supplier)
            context.emit(supplier)
            context.emit(award)

    return proxy


def make_statement(context: TaskContext, data: Record, org: E) -> E:
    project = make_project(context, data, org)
    context.emit(project)

    dates = [i["sendingDate"] for i in data["recipientGroups"]]
    proxy = context.make_entity("Article")
    proxy.id = context.make_id("statement", project.id, org.id, *dates)
    proxy.add("title", f"Stellungnahme von {org.caption} zu {project.caption}")
    text = data.pop("text", None)
    if isinstance(text, dict):
        text = text.get("text")
    proxy.add("bodyText", text)
    proxy.add("publishedAt", dates)

    rel = context.make_entity("Documentation")
    rel.id = context.make_id("project-statement", project.id, proxy.id)
    rel.add("document", proxy)
    rel.add("entity", project)
    rel.add("date", dates)
    rel.add("role", "Stellungnahme")
    context.emit(rel)

    # the statement as it was handed in
    statement_url = data.get("pdfUrl")
    if statement_url:
        make_document(
            context,
            proxy,
            statement_url,
            role="Stellungnahme",
            title=join_text("Stellungnahme", org.caption, "zu", project.caption),
            folders=[*lobbyist_folders(org), "Stellungnahmen"],
            dates=dates,
        )

    for group in data.pop("recipientGroups"):
        recipients = group.pop("recipients", {})
        # Handle parliament recipients (have code/de/en structure)
        for recipient in recipients.get("parliament", []):
            recipient_org = context.make_entity("PublicBody")
            # Use make_slug with "org" prefix for consistency - allows merging
            # with other PublicBody refs
            recipient_org.id = context.make_slug("org", recipient["code"].lower())
            recipient_org.add("name", recipient["de"])
            recipient_org.add("country", "de")
            recipient_org.add("topics", "gov.national")
            context.emit(recipient_org)
            rel = context.make_entity("Documentation")
            rel.id = context.make_id(
                "statement-recipient-rel", proxy.id, recipient_org.id, *dates
            )
            rel.add("document", proxy)
            rel.add("entity", recipient_org)
            rel.add("role", "Empfänger von Stellungnahme")
            rel.add("date", dates)
            context.emit(rel)
        # Handle federalGovernment recipients (have department structure)
        for recipient in recipients.get("federalGovernment", []):
            dept = recipient.get("department", {})
            if not dept:
                continue
            recipient_org = context.make_entity("PublicBody")
            # Use make_slug with "org" prefix - same pattern as make_ministry for deduplication
            recipient_org.id = context.make_slug("org", dept["shortTitle"])
            recipient_org.add("name", dept["title"])
            recipient_org.add("weakAlias", dept["shortTitle"])
            recipient_org.add("country", "de")
            recipient_org.add("website", dept.get("url"))
            recipient_org.add("topics", "gov.executive")
            context.emit(recipient_org)
            rel = context.make_entity("Documentation")
            rel.id = context.make_id(
                "statement-recipient-rel", proxy.id, recipient_org.id, *dates
            )
            rel.add("document", proxy)
            rel.add("entity", recipient_org)
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
        proxy = context.make_entity(proxy_type.value)
        proxy.id = context.make_slug(registerId)
        proxy = make_organization(context, proxy, proxy_data)

    activities = data.pop("activitiesAndInterests")
    proxy.add("idNumber", registerId)
    proxy.add("summary", activities.pop("activity", {}).get("de"))
    proxy.add("description", clean_string(activities.pop("activityDescription")))
    proxy.add("keywords", [i["de"] for i in activities.pop("fieldsOfInterest")])
    proxy.add(
        "notes", [i["de"] for i in activities.pop("typesOfExercisingLobbyWork", [])]
    )
    proxy.add("sourceUrl", record.pop("detailsPageUrl"))
    entry_url = record.pop("pdfUrl")
    proxy.add("sourceUrl", entry_url)
    proxy.add(
        "status",
        "active" if data["accountDetails"]["activeLobbyist"] else "inactive",  # noqa
    )

    # Add first publication date (when first registered in lobby register)
    first_pub = data["accountDetails"].get("firstPublicationDate")
    if first_pub:
        # Extract just the date part from ISO datetime
        pub_date = first_pub.split("T")[0] if "T" in first_pub else first_pub
        proxy.add("notes", f"First registered: {pub_date}")

    # Add financial data as notes
    employees = data.get("employeesInvolvedInLobbying", {})
    if employees.get("employeeFTE") is not None:
        proxy.add("notes", f"Lobbying FTE: {employees['employeeFTE']}")

    expenses = data.get("financialExpenses", {})
    expense_range = format_euro_range(expenses.get("financialExpensesEuro"))
    if expense_range:
        proxy.add("notes", f"Lobbying expenses: {expense_range}")

    funding = data.get("mainFundingSources", {})
    funding_sources = [s.get("de") for s in funding.get("mainFundingSources", [])]
    if funding_sources:
        proxy.add(
            "notes", f"Funding sources: {', '.join(filter(None, funding_sources))}"
        )

    membership = data.get("membershipFees", {})
    membership_range = format_euro_range(membership.get("totalMembershipFees"))
    if membership_range:
        proxy.add("notes", f"Membership fees: {membership_range}")

    context.emit(proxy)

    # the register entry itself, republished as a pdf on every version
    entry = record.get("registerEntryId")
    version = record.get("version")
    if entry_url:
        make_document(
            context,
            proxy,
            entry_url,
            role="Registereintrag",
            title=join_text("Registereintrag", registerId, "-", proxy.caption),
            key=f"{entry}-{version}",
            folders=[*lobbyist_folders(proxy), "Registereintrag"],
            dates=[(record.get("validFromDate") or "").split("T")[0] or None],
        )

    # Add annual report as Document entity
    annual = data.get("annualReports", {})
    report_url = annual.get("annualReportPdfUrl")
    fiscal_start = annual.get("lastFiscalYearStart")
    fiscal_end = annual.get("lastFiscalYearEnd")
    if report_url:
        make_document(
            context,
            proxy,
            report_url,
            role="Jahresabschluss",
            title=join_text("Jahresabschluss", proxy.caption),
            # fiscal year dates outlive the url, which changes on every upload
            key=fiscal_start or fiscal_end,
            folders=[*lobbyist_folders(proxy), "Jahresabschluss"],
            dates=[d for d in (fiscal_start, fiscal_end) if d],
        )

    conduct = data.get("codeOfConduct") or {}
    conduct_url = conduct.get("codeOfConductPdfUrl")
    if conduct_url:
        make_document(
            context,
            proxy,
            conduct_url,
            role="Verhaltenskodex",
            title=join_text("Verhaltenskodex", proxy.caption),
            folders=[*lobbyist_folders(proxy), "Verhaltenskodex"],
        )

    if data["donators"].get("donatorsInformationPresent"):
        start_date = data["donators"].get("relatedFiscalYearStart")
        end_date = data["donators"].get("relatedFiscalYearEnd")
        for item in data["donators"].pop("donators"):
            payer = context.make_entity("LegalEntity")
            name = item.pop("name")
            payer.id = context.make_slug(
                "donator",
                make_entity_id("donator", proxy.id, make_fingerprint(name)),
            )
            payer.add("name", name)
            payer.add("address", item.get("location"))
            context.emit(payer)

            payment = context.make_entity("Payment")
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

    if data.get("clientIdentity") and data["clientIdentity"].get("clientsPresent"):
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

    if data.get("contracts") and data["contracts"].pop("contractsPresent"):
        for contract in data.pop("contracts").pop("contracts"):
            context.emit(make_contract(context, contract, proxy))

    if data.get("statements") and data["statements"].pop("statementsPresent"):
        for statement in data.pop("statements").pop("statements"):
            context.emit(make_statement(context, statement, proxy))

    for project in ensure_dict(data.pop("regulatoryProjects")).pop(
        "regulatoryProjects", []
    ):
        context.emit(make_project(context, project, proxy))

    for allowance in data.pop("publicAllowances").pop("publicAllowances", []):
        payer = context.make_entity("PublicBody")
        payer_name = allowance.pop("name")
        # Use fingerprint_id for stability (fixed-length hash) and "org" prefix for namespace
        payer.id = context.make_slug("org", make_fingerprint_id(payer_name))
        if payer.id:
            payer.add("name", payer_name)
            payer.add("country", "de")
            payer.add("legalForm", allowance.get("type", {}).get("de"))
            context.emit(payer)
            description = clean_string(allowance.pop("description"))
            amounts = allowance.get("publicAllowanceEuro")
            payment = context.make_entity("Payment")
            payment.id = context.make_id(
                "payment", payer.id, proxy.id, make_fingerprint(description)
            )
            payment.add("payer", payer)
            payment.add("beneficiary", proxy)
            payment.add("purpose", description)
            payment.add("programme", "Öffentliche Zuwendungen")
            if amounts:
                payment.add("amountEur", [amounts["from"], amounts["to"]])
            context.emit(payment)


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    tx = ctx.task()
    parse_record(tx, record)
    yield from tx
