from ftmq.types import Entities
from ftmq.util import get_country_code
from investigraph.model import SourceContext
from investigraph.types import Record

from util.worldbank import parse_date


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    """Transform World Bank investment project contract award to FTM entities."""
    contract_number = record.get("wb_contract_number")
    if not contract_number:
        ctx.log.warning("Missing wb_contract_number", row=ix)
        return

    # Create Project entity for the development project
    project_id = record.get("project_id")
    if project_id:
        project = ctx.make_entity("Project")
        project.id = ctx.make_slug("project", project_id)
        project.add("projectId", project_id)
        project.add("name", record.get("project_name"))

        # Borrower country
        borrower_country = record.get("borrower_country_code")
        if borrower_country:
            project.add("country", get_country_code(borrower_country))

        # Keywords from global practice
        practices = record.get("project_global_practice")
        if practices:
            project.add("keywords", [p.strip() for p in practices.split(";")])

        # Region as note
        region = record.get("region")
        if region:
            project.add("notes", f"Region: {region}")

        project.add("publisher", "World Bank Group")

        yield project

    # Create Contract entity
    contract = ctx.make_entity("Contract")
    contract.id = ctx.make_slug("contract", contract_number)
    contract.add("procedureNumber", contract_number)
    contract.add("title", record.get("contract_description"))
    contract.add("contractDate", parse_date(record.get("contract_signing_date")))

    # Link to project
    if project_id:
        contract.add("project", ctx.make_slug("project", project_id))

    # Amount
    amount = record.get("supplier_contract_amount_usd")
    if amount:
        contract.add("amountUsd", amount)

    # Procurement details as keywords and notes
    keywords = []
    if record.get("procurement_category"):
        keywords.append(record["procurement_category"])
    contract.add("keywords", keywords)

    # Method and review type
    method = record.get("procurement_method")
    if method:
        contract.add("method", method)

    notes = []
    review_type = record.get("review_type")
    if review_type:
        notes.append(f"Review type: {review_type}")
    borrower_ref = record.get("borrower_contract_reference_number")
    if borrower_ref:
        notes.append(f"Borrower ref: {borrower_ref}")
    fiscal_year = record.get("fiscal_year")
    if fiscal_year:
        notes.append(f"Fiscal year: {fiscal_year}")
    contract.add("notes", notes)

    contract.add("publisher", "World Bank Group")

    yield contract

    # Create Company entity for supplier
    supplier_name = record.get("supplier")
    supplier_id = record.get("supplier_id")
    supplier_country = record.get("supplier_country_code")

    if supplier_name and supplier_id:
        supplier = ctx.make_entity("Company")
        # Use supplier_id for stable ID (official World Bank supplier identifier)
        supplier.id = ctx.make_slug("supplier", supplier_id)
        supplier.add("name", supplier_name)
        supplier.add("registrationNumber", supplier_id)
        if supplier_country:
            supplier.add("country", get_country_code(supplier_country))

        yield supplier

        # Create ContractAward linking contract to supplier
        award = ctx.make_entity("ContractAward")
        award.id = ctx.make_id("award", contract.id, supplier.id)
        award.add("contract", contract)
        award.add("supplier", supplier)
        award.add("date", parse_date(record.get("contract_signing_date")))
        if amount:
            award.add("amountUsd", amount)

        yield award
