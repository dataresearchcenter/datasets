from ftmq.types import Entities
from ftmq.util import get_country_code
from investigraph.model import SourceContext
from investigraph.types import Record
from investigraph.util import make_fingerprint_id

from common.worldbank import parse_date


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    """Transform IFC Investment Services project record to FTM entities."""
    project_number = record.get("project_number")
    if not project_number:
        ctx.log.warning("Missing project_number", row=ix)
        return

    # Create Project entity
    project = ctx.make_entity("Project")
    project.id = ctx.make_slug("ifc", project_number)
    project.add("projectId", project_number)
    project.add("name", record.get("project_name"))
    project.add("sourceUrl", record.get("project_url"))

    # Dates
    project.add("startDate", parse_date(record.get("ifc_approval_date")))
    project.add("date", parse_date(record.get("ifc_signed_date")))
    project.add("modifiedAt", parse_date(record.get("date_disclosed")))

    # Status
    status = record.get("status")
    if status:
        project.add("status", status.lower())

    # Country
    country_code = record.get("wb_country_code")
    if country_code:
        project.add("country", get_country_code(country_code))

    # Keywords from industry, product_line, department
    keywords = []
    if record.get("industry"):
        keywords.append(record["industry"])
    if record.get("product_line"):
        keywords.append(record["product_line"])
    if record.get("department"):
        keywords.append(record["department"])
    project.add("keywords", keywords)

    # Total investment amount (in millions USD, convert to USD)
    total_investment = record.get(
        "total_ifc_investment_as_approved_by_boardmillion__usd"
    )
    if total_investment:
        # Convert millions to actual USD
        project.add("amountUsd", int(total_investment * 1_000_000))

    # Notes for additional data
    notes = []

    # Investment breakdown
    loan = record.get("ifc_investment_for_loanmillion__usd")
    equity = record.get("ifc_investment_for_equitymillion__usd")
    guarantee = record.get("ifc_investment_for_guaranteemillion__usd")
    risk_mgmt = record.get("ifc_investment_for_risk_managementmillion__usd")

    breakdown = []
    if loan:
        breakdown.append(f"Loan: ${loan}M")
    if equity:
        breakdown.append(f"Equity: ${equity}M")
    if guarantee:
        breakdown.append(f"Guarantee: ${guarantee}M")
    if risk_mgmt:
        breakdown.append(f"Risk Management: ${risk_mgmt}M")
    if breakdown:
        notes.append(f"Investment breakdown: {', '.join(breakdown)}")

    # Environmental category
    env_category = record.get("environmental_category")
    if env_category:
        notes.append(f"Environmental category: {env_category}")

    project.add("notes", notes)

    # Publisher
    project.add("publisher", "International Finance Corporation (IFC)")

    yield project

    # Create Company entity for recipient
    company_name = record.get("company_name")
    country_code = record.get("wb_country_code")
    if company_name:
        company = ctx.make_entity("Company")
        # Use fingerprint of name + country for stable ID
        company.id = ctx.make_slug(
            "company",
            make_fingerprint_id(company_name),
        )
        company.add("name", company_name)
        if country_code:
            company.add("country", get_country_code(country_code))

        yield company

        # Create ProjectParticipant to link company to project
        participant = ctx.make_entity("ProjectParticipant")
        participant.id = ctx.make_id("participant", project.id, company.id)
        participant.add("project", project)
        participant.add("participant", company)
        participant.add("role", "Investment recipient")
        participant.add("startDate", parse_date(record.get("ifc_signed_date")))

        yield participant
