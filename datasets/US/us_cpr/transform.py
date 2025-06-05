import countrynames
from banal import as_bool
from ftmq.types import CEGenerator
from investigraph.model import SourceContext
from investigraph.types import Record
from investigraph.util import make_string_id


def handle(ctx: SourceContext, record: Record, ix: int) -> CEGenerator:
    country = record.pop("COUNTRY").split(",")
    country = [countrynames.to_code(c) for c in country]
    country = [c for c in country if not c == "ANHH"]  # FIXME
    date = record.pop("DATE")
    name = record.pop("COMPANY")
    company = ctx.make_proxy(
        "Company",
        id=ctx.make_slug(make_string_id(name)),
        name=name,
        country=country,
        legalForm="us public company" if as_bool(record.pop("US_PUBLIC_CO")) else None,
    )
    yield company

    case_id = record.pop("CASE_ID")
    if case_id:
        case = ctx.make_proxy(
            "CourtCase",
            id=ctx.make_slug(case_id),
            caseNumber=case_id,
            name=record.pop("CASE_NAME"),
            country=country,
            fileDate=date,
        )
        yield case

        yield ctx.make_proxy(
            "CourtCaseParty",
            id=ctx.make_id(company.id, case.id),
            case=case,
            party=company,
            date=date,
            description=record.pop("PRIMARY_CRIME_CODE"),
        )
