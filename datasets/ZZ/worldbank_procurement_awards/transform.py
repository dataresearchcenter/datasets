from ftmq.types import Entities
from ftmq.util import get_country_code
from investigraph.model import SourceContext
from investigraph.types import Record
from investigraph.util import make_fingerprint_id

from common.worldbank import parse_date


# World Bank Group organizations
WBG_ORGS = {
    "IBRD": "International Bank for Reconstruction and Development",
    "IDA": "International Development Association",
    "IFC": "International Finance Corporation",
    "MIGA": "Multilateral Investment Guarantee Agency",
    "ICSID": "International Centre for Settlement of Investment Disputes",
}


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    """Transform World Bank procurement contract award to FTM entities."""
    selection_number = record.pop("selection_number")
    contract_description = record.pop("contract_description")
    supplier_name = record.pop("supplier")
    award_date = record.pop("award_date")

    # Create Contract entity (corporate procurement)
    contract = ctx.make_entity("Contract")

    # Generate ID from selection_number, or fall back to hash of other fields
    if selection_number and str(selection_number).strip() not in ("", "-"):
        contract.id = ctx.make_slug("contract", selection_number)
        contract.add("procedureNumber", selection_number)
    else:
        # No selection number - generate ID from contract details
        contract.id = ctx.make_id(
            contract_description,
            supplier_name,
            award_date,
            prefix="wb-contract",
        )

    contract.add("title", contract_description)
    contract.add("contractDate", parse_date(award_date))

    # Amount
    amount = record.get("contract_award_amount")
    if amount:
        contract.add("amountUsd", amount)

    # Keywords
    keywords = []
    if record.get("commodity_category"):
        keywords.append(record["commodity_category"])
    if record.get("fund_source"):
        keywords.append(record["fund_source"])
    contract.add("keywords", keywords)

    # Notes
    notes = []
    fiscal = record.get("quarter_and_fiscal_year")
    if fiscal:
        notes.append(f"Fiscal period: {fiscal}")
    vpu = record.get("vpu_description")
    if vpu:
        notes.append(f"VPU: {vpu}")
    contract.add("notes", notes)

    contract.add("publisher", "World Bank Group")

    # Create or reference WBG organization as authority
    wbg_org_code = record.get("wbg_organization")
    if wbg_org_code:
        wbg_org = ctx.make_entity("PublicBody")
        wbg_org.id = ctx.make_slug("org", wbg_org_code.lower())
        wbg_org.add("name", WBG_ORGS.get(wbg_org_code, wbg_org_code))
        wbg_org.add("weakAlias", wbg_org_code)
        wbg_org.add("topics", "gov.igo")
        yield wbg_org

        contract.add("authority", wbg_org)

    yield contract

    # Create Company entity for supplier
    supplier_country = record.pop("supplier_country_code", None)
    if supplier_name:
        supplier = ctx.make_entity("Company")
        supplier.id = ctx.make_slug(
            "company",
            make_fingerprint_id(supplier_name),
        )
        supplier.add("name", supplier_name)
        if supplier_country:
            supplier.add("country", get_country_code(supplier_country))

        yield supplier

        # Create ContractAward linking contract to supplier
        award = ctx.make_entity("ContractAward")
        award.id = ctx.make_id("award", contract.id, supplier.id)
        award.add("contract", contract)
        award.add("supplier", supplier)
        award.add("date", parse_date(award_date))
        if amount:
            award.add("amountUsd", amount)

        yield award
