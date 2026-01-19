"""
Transform OCDS 1.1.5 data to FollowTheMoney entities.

This is a generic transformer that can be used across any OCDS dataset.
Use it by calling handle() with an investigraph context.
"""

import logging
from banal import is_mapping, ensure_list
from investigraph.helpers.addresses import make_address

log = logging.getLogger(__name__)

# Map OCDS identifier schemes to FTM properties
IDENTIFIER_PROPS = {
    "TRADE_REGISTER": "registrationNumber",
    "AU-ABN": "registrationNumber",
    "PY-PGN": "classification",
    "TAX_ID": "vatCode",
    "AM-TIN": "taxNumber",
    "ORGANIZATION_ID": "classification",
    "STATISTICAL": "classification",
    "National-ID": "registrationNumber",
}
DEFAULT_IDENTIFIER_PROP = "registrationNumber"

# Field name mappings for flexible extraction
NAME_FIELDS = ["name", "legalName", "entityName", "businessName", "title"]
DESCRIPTION_FIELDS = ["description", "summary"]


def clean_date(date):
    """Extract ISO date from datetime string."""
    if date and "T" in str(date):
        date, _ = str(date).split("T", 1)
    return date


def get_field_value(data, fields):
    """Extract value from first matching field."""
    if not is_mapping(data):
        return None
    for field in ensure_list(fields):
        value = data.get(field)
        if value:
            return value
    return None


def determine_org_schema(party_data):
    """Determine appropriate FTM schema based on OCDS party roles."""
    roles = ensure_list(party_data.get("roles", []))
    if "buyer" in roles or "procuringEntity" in roles:
        return "PublicBody"
    return "LegalEntity"


def make_organization(ctx, party_data):
    """Create an Organization entity from OCDS party data.

    Returns:
        Tuple of (organization entity, identification entity or None, address entity or None, contact entity or None, representation entity or None)
    """
    if not is_mapping(party_data):
        return None, None, None, None, None

    # Determine schema
    schema = determine_org_schema(party_data)
    org = ctx.make_entity(schema)

    # Generate ID from OCDS party ID
    party_id = party_data.get("id")
    if not party_id:
        return None, None, None, None, None

    org.id = ctx.make_slug("party", party_id)

    # Add name
    name = get_field_value(party_data, NAME_FIELDS)
    org.add("name", name)

    # Add identifier and create Identification entity
    identification = None
    identifier_data = party_data.get("identifier", {})
    if is_mapping(identifier_data):
        scheme = identifier_data.get("scheme")
        id_value = identifier_data.get("id")

        # Add to organization
        if id_value:
            prop = IDENTIFIER_PROPS.get(scheme, DEFAULT_IDENTIFIER_PROP)
            org.add(prop, id_value)

        # Create Identification entity
        if id_value and scheme:
            identification = ctx.make_entity("Identification")
            country = None

            # Get country from address first
            address_data = party_data.get("address", {})
            if is_mapping(address_data):
                country = (
                    address_data.get("countryCode")
                    or address_data.get("countryCode3")
                    or address_data.get("countryName")
                )

            # Generate ID
            if country:
                identification.id = ctx.make_slug("id", country, scheme, id_value)
            else:
                identification.id = ctx.make_slug("id", scheme, id_value)

            identification.add("number", id_value)
            identification.add("type", scheme)
            identification.add("holder", org)
            identification.add("country", country)
            identification.add("authority", identifier_data.get("uri"))

    # Create Address entity
    address_entity = None
    address_data = party_data.get("address", {})
    if is_mapping(address_data):
        address_entity = make_address(
            ctx,
            street=address_data.get("streetAddress"),
            city=address_data.get("locality"),
            postal_code=address_data.get("postalCode"),
            region=address_data.get("region"),
            country_code=address_data.get("countryCode"),
            country=address_data.get("countryName"),
        )

        if address_entity:
            org.add("addressEntity", address_entity)
            org.add("address", address_entity.first("full"))
            org.add("country", address_data.get("countryCode"))

    # Create contact person and Representation if contactPoint has name
    contact = None
    representation = None
    contact_data = party_data.get("contactPoint", {})
    if is_mapping(contact_data):
        contact_name = contact_data.get("name")

        # Add contact info to organization
        org.add("email", contact_data.get("email"))
        org.add("phone", contact_data.get("telephone") or contact_data.get("phone"))
        org.add("website", party_data.get("details", {}).get("url"))

        # Create contact person entity if name is present
        if contact_name:
            contact = ctx.make_entity("LegalEntity")
            email = contact_data.get("email")
            phone = contact_data.get("telephone") or contact_data.get("phone")

            if email:
                contact.id = ctx.make_id("contact", email)
            elif phone:
                contact.id = ctx.make_id("contact", phone)
            else:
                contact.id = ctx.make_id("contact", org.id, contact_name)

            contact.add("name", contact_name)
            contact.add("email", email)
            contact.add("phone", phone)
            contact.add("country", org.first("country"))

            # Create Representation relationship
            representation = ctx.make_entity("Representation")
            representation.id = ctx.make_id("contact", org.id, contact.id)
            representation.add("agent", contact)
            representation.add("client", org)
            representation.add("role", "Contact")

    return org, identification, address_entity, contact, representation


def make_call_for_tenders(ctx, ocid, tender_data, buyer_entity):
    """Create CallForTenders entity from OCDS tender data."""
    if not is_mapping(tender_data):
        return None

    cft = ctx.make_entity("CallForTenders")
    cft.id = ctx.make_slug("tender", ocid)

    # Add basic info
    title = get_field_value(tender_data, NAME_FIELDS)
    description = get_field_value(tender_data, DESCRIPTION_FIELDS)
    cft.add("title", title)
    cft.add("description", description)
    cft.add("authority", buyer_entity)

    # Add submission URL
    cft.add("sourceUrl", tender_data.get("submissionMethodDetails"))

    # Add dates from tender period
    tender_period = tender_data.get("tenderPeriod", {})
    if is_mapping(tender_period):
        cft.add("publicationDate", clean_date(tender_period.get("startDate")))
        cft.add("submissionDeadline", clean_date(tender_period.get("endDate")))

    # Add CPV codes from tender items
    for item in ensure_list(tender_data.get("items", [])):
        if is_mapping(item):
            classification = item.get("classification", {})
            if is_mapping(classification) and classification.get("scheme") == "CPV":
                cft.add("cpvCode", classification.get("id"))

    return cft


def make_contract(ctx, ocid, contract_id, contract_data, call_for_tenders):
    """Create Contract entity from OCDS contract data."""
    contract = ctx.make_entity("Contract")
    contract.id = ctx.make_slug("contract", ocid, contract_id)

    # Add basic info
    title = get_field_value(contract_data, NAME_FIELDS)
    description = get_field_value(contract_data, DESCRIPTION_FIELDS)
    contract.add("title", title)
    contract.add("description", description)
    contract.add("contractDate", clean_date(contract_data.get("dateSigned")))
    contract.add("status", contract_data.get("status"))

    # Add value
    value_data = contract_data.get("value", {})
    if is_mapping(value_data):
        contract.add("amount", value_data.get("amount"))
        contract.add("currency", value_data.get("currency"))

    return contract


def make_contract_award(
    ctx, ocid, award_id, award_data, contract_entity, call_for_tenders
):
    """Create ContractAward entity from OCDS award data."""
    award = ctx.make_entity("ContractAward")
    award.id = ctx.make_slug("award", ocid, award_id)

    # Link to contract and tender
    award.add("contract", contract_entity)
    if call_for_tenders:
        award.add("callForTenders", call_for_tenders)

    # Add basic info
    title = get_field_value(award_data, NAME_FIELDS)
    description = get_field_value(award_data, DESCRIPTION_FIELDS)
    award.add("role", title)
    award.add("summary", description)
    award.add("date", clean_date(award_data.get("date") or award_data.get("awardDate")))
    award.add("status", award_data.get("status"))

    # Add value
    value_data = award_data.get("value", {})
    if is_mapping(value_data):
        award.add("amount", value_data.get("amount"))
        award.add("currency", value_data.get("currency"))

    # Add contract period from award
    period_data = award_data.get("contractPeriod", {})
    if is_mapping(period_data):
        award.add("startDate", clean_date(period_data.get("startDate")))
        award.add("endDate", clean_date(period_data.get("endDate")))

    # Add CPV codes from award items
    for item in ensure_list(award_data.get("items", [])):
        if is_mapping(item):
            classification = item.get("classification", {})
            if is_mapping(classification) and classification.get("scheme") == "CPV":
                award.add("cpvCode", classification.get("id"))

    return award


def handle(ctx, record, ix):
    """Transform OCDS release to FollowTheMoney entities.

    Args:
        ctx: investigraph SourceContext
        record: OCDS release (dict)
        ix: record index

    Yields:
        FollowTheMoney entities
    """
    if not is_mapping(record):
        return

    ocid = record.get("ocid")
    if not ocid:
        ctx.log.warning("No OCID in record", record=ix)
        return
    if ocid.startswith("ocds-"):
        ocid = ocid[5:]

    # Extract parties and build lookup
    parties = ensure_list(record.get("parties", []))
    party_lookup = {p.get("id"): p for p in parties if is_mapping(p)}

    # Create organizations from parties
    org_entities = {}  # party_id -> organization entity
    for party_id, party_data in party_lookup.items():
        org, identification, address, contact, representation = make_organization(
            ctx, party_data
        )

        if org:
            org_entities[party_id] = org
            yield org

            if identification:
                yield identification
            if address:
                yield address
            if contact:
                yield contact
            if representation:
                yield representation

    # Get buyer
    buyer_entity = None
    buyer_data = record.get("buyer", {})
    if is_mapping(buyer_data):
        buyer_id = buyer_data.get("id")
        buyer_entity = org_entities.get(buyer_id)

    # Create CallForTenders from tender
    # Note: buyer_entity can be None for minimal records
    call_for_tenders = None
    tender_data = record.get("tender", {})
    if is_mapping(tender_data):
        call_for_tenders = make_call_for_tenders(ctx, ocid, tender_data, buyer_entity)
        if call_for_tenders:
            yield call_for_tenders

    # Process contracts
    contracts_data = ensure_list(record.get("contracts", []))
    contract_entities = {}  # contract_id -> Contract entity

    for contract_data in contracts_data:
        if not is_mapping(contract_data):
            continue

        contract_id = contract_data.get("id")
        if not contract_id:
            continue

        contract = make_contract(
            ctx, ocid, contract_id, contract_data, call_for_tenders
        )
        contract_entities[contract_id] = contract

        # Link buyer
        if buyer_entity:
            contract.add("authority", buyer_entity)

        yield contract

    # Process awards
    awards_data = ensure_list(record.get("awards", []))
    award_entities = {}  # award_id -> ContractAward entity

    for award_data in awards_data:
        if not is_mapping(award_data):
            continue

        award_id = award_data.get("id")
        if not award_id:
            continue

        # Find matching contract
        contract = None
        for contract_data in contracts_data:
            if is_mapping(contract_data) and contract_data.get("awardID") == award_id:
                contract = contract_entities.get(contract_data.get("id"))

                # Merge contract value/period into award if missing
                if contract and not award_data.get("value"):
                    award_data["value"] = contract_data.get("value")
                if contract and not award_data.get("contractPeriod"):
                    award_data["contractPeriod"] = contract_data.get("period")
                break

        # Create award
        award = make_contract_award(
            ctx, ocid, award_id, award_data, contract, call_for_tenders
        )
        award_entities[award_id] = award

        # Link suppliers
        for supplier_ref in ensure_list(award_data.get("suppliers", [])):
            if is_mapping(supplier_ref):
                supplier_id = supplier_ref.get("id")
                supplier = org_entities.get(supplier_id)
                if supplier:
                    award.add("supplier", supplier)

        yield award
