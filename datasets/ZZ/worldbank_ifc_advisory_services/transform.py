from ftmq.types import Entities
from ftmq.util import get_country_code
from investigraph.model import SourceContext
from investigraph.types import Record

from util.worldbank import parse_date


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    """Transform IFC Advisory Services project record to FTM entities."""
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
    project.add("date", parse_date(record.get("projected_start_date")))
    project.add("modifiedAt", parse_date(record.get("disclosure_date")))

    # Status
    status = record.get("status")
    if status:
        project.add("status", status.lower())

    # Country - use WB country code (ISO 2-letter)
    country_code = record.get("wb_country_code")
    if country_code:
        project.add("country", get_country_code(country_code))

    # Keywords from business line, department, region
    keywords = []
    if record.get("business_line"):
        keywords.append(record["business_line"])
    if record.get("department"):
        keywords.append(record["department"])
    project.add("keywords", keywords)

    # Budget (in USD)
    budget = record.get("estimated_total_budget__")
    if budget:
        project.add("amountUsd", budget)

    # Region as note
    region = record.get("ifc_region")
    if region:
        project.add("notes", f"IFC Region: {region}")

    # Publisher is IFC
    project.add("publisher", "International Finance Corporation (IFC)")

    yield project
