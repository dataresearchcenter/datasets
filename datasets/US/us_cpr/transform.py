from banal import as_bool
from ftmq.types import Entities
from ftmq.util import get_country_code
from investigraph.model import SourceContext
from investigraph.types import Record
from investigraph.util import make_string_id


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    country_raw = record.pop("COUNTRY").split(",")
    countries = [get_country_code(c.strip()) for c in country_raw]
    countries = [c for c in countries if c]  # Filter None values

    date = record.pop("DATE")
    name = record.pop("COMPANY")

    # Create Company entity
    company = ctx.make_entity("Company")
    company.id = ctx.make_slug(make_string_id(name))
    company.add("name", name)
    company.add("country", countries)
    if as_bool(record.pop("US_PUBLIC_CO")):
        company.add("legalForm", "us public company")
    yield company

    # Create CourtCase if case_id exists
    case_id = record.pop("CASE_ID")
    if case_id:
        case = ctx.make_entity("CourtCase")
        case.id = ctx.make_slug(case_id)
        case.add("caseNumber", case_id)
        case.add("name", record.pop("CASE_NAME"))
        case.add("country", countries)
        case.add("fileDate", date)
        yield case

        # Create CourtCaseParty linking company to case
        party = ctx.make_entity("CourtCaseParty")
        party.id = ctx.make_id(company.id, case.id)
        party.add("case", case)
        party.add("party", company)
        party.add("date", date)
        party.add("description", record.pop("PRIMARY_CRIME_CODE"))
        yield party
