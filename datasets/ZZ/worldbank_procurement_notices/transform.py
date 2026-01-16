from ftmq.types import Entities
from ftmq.util import get_country_code
from investigraph.model import SourceContext
from investigraph.types import Record

from util.worldbank import parse_date


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    """Transform World Bank procurement notice to FTM entities."""
    notice_id = record.get("id")
    if not notice_id:
        ctx.log.warning("Missing id", row=ix)
        return

    # Create Project entity for the development project
    project_id = record.get("project_id")
    if project_id:
        project = ctx.make_entity("Project")
        project.id = ctx.make_slug("project", project_id)
        project.add("projectId", project_id)

        # Country
        country_code = record.get("country_code")
        if country_code:
            project.add("country", get_country_code(country_code))

        # Sectors as keywords
        sectors = record.get("sector")
        if sectors:
            project.add("keywords", [s.strip() for s in sectors.split(";")])

        # Region
        region = record.get("region")
        if region:
            project.add("notes", f"Region: {region}")

        project.add("publisher", "World Bank Group")

        yield project

    # Create CallForTenders entity
    tender = ctx.make_entity("CallForTenders")
    tender.id = ctx.make_slug("tender", str(notice_id))
    tender.add("callId", str(notice_id))
    tender.add("title", record.get("bid_description"))
    tender.add("sourceUrl", record.get("url"))

    # Dates
    tender.add("publicationDate", parse_date(record.get("publication_date")))
    tender.add("submissionDeadline", parse_date(record.get("deadline_date")))

    # Country
    country_code = record.get("country_code")
    if country_code:
        tender.add("country", get_country_code(country_code))

    # Procurement details
    notice_type = record.get("notice_type")
    if notice_type:
        tender.add("procurementType", notice_type)

    method = record.get("procurement_method")
    if method:
        tender.add("procedure", method)

    # Keywords from category and sector
    keywords = []
    category = record.get("procurement_category")
    if category:
        keywords.append(category)
    tender.add("keywords", keywords)

    # Notes
    notes = []
    region = record.get("region")
    if region:
        notes.append(f"Region: {region}")
    country_name = record.get("country_name")
    if country_name:
        notes.append(f"Country: {country_name}")
    tender.add("notes", notes)

    # Link to project via program field
    if project_id:
        tender.add("programId", project_id)

    tender.add("publisher", "World Bank Group")

    yield tender
