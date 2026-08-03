"""
Transform Land Matrix deals and investors into FollowTheMoney entities.

A deal is a large scale land acquisition. It becomes a `Project` acting as the
hub, because that is the only schema the rest can hang off natively:

    Project (the deal)
      +- License        the land grant itself: area, commodities, nature
      +- Contract       one per signed contract, via `Contract.project`
      +- RealEstate     the locations it covers, via `UnknownLink`
      +- ProjectParticipant   the investors behind it

Investors form their own network of `Ownership` relations on the side.

The api and the legacy csv export describe the same deals from different angles,
so both handlers write onto the same entity ids and the results merge.

Where a `LAKEHOUSE_URI` is configured, the sources a deal cites are archived
there, emitted as documents and tied back to the deal by a `Documentation`
entity. That is around 46.000 files, so a run without a lakehouse keeps the
links on the deal and skips the documents.
"""

import os
import re

from followthemoney import EntityProxy
from ftmq.types import Entities
from investigraph.model import SourceContext, TaskContext
from investigraph.types import Record
from investigraph.util import join_text

from datasets.ZZ.landmatrix.choices import (
    ANIMALS,
    CARBON_SEQUESTRATION,
    CROPS,
    DEFAULT_INVESTOR_SCHEMA,
    ELECTRICITY_GENERATION,
    IMPLEMENTATION_STATUS,
    INTENTION_OF_INVESTMENT,
    INVESTMENT_TYPE,
    INVESTOR_CLASSIFICATION,
    INVESTOR_SCHEMATA,
    INVOLVEMENT_ROLE,
    LOCATION_ACCURACY,
    MINERALS,
    NEGOTIATION_STATUS,
    PARENT_RELATION,
    get_label,
    get_labels,
)

DEAL_URL = "https://landmatrix.org/deal/%s/"
INVESTOR_URL = "https://landmatrix.org/investor/%s/"
LM_MEDIA_URL = "https://landmatrix.org/media/%s"
MAX_FILENAME = 120
# what the `File` column says when the source is deliberately not published
CONFIDENTIAL = "-confidential-"

# what the land is used for, across the produce vocabularies
PRODUCE = (
    ("current_crops", CROPS),
    ("current_animals", ANIMALS),
    ("current_mineral_resources", MINERALS),
    ("current_electricity_generation", ELECTRICITY_GENERATION),
    ("current_carbon_sequestration", CARBON_SEQUESTRATION),
)


# Shared id helpers: the api and the csv export have to agree on these to the
# character, otherwise the two halves of a deal never merge. Everything the
# export keys by a `nid` is hashed and scoped by its deal, because those ids are
# case sensitive, reuse `-` and `_`, and repeat across deals.


def deal_id(ctx: TaskContext, deal: str | int) -> str:
    return ctx.make_slug("deal", str(deal))


def license_id(ctx: TaskContext, deal: str | int) -> str:
    return ctx.make_slug("license", str(deal))


def nested_id(ctx: TaskContext, kind: str, deal: str | int, nid: str) -> str:
    return ctx.make_id(kind, str(deal), nid)


def format_coordinate(value: float) -> str:
    """Coordinates have to reach followthemoney as a string: a float gets
    rounded to two decimals on the way in, which is about a kilometer off.
    The source has at most six decimals, and `%f` never goes exponential."""
    return f"{value:.6f}".rstrip("0").rstrip(".")


def get_current_date(entries: list[Record] | None) -> str | None:
    """The date of the entry flagged `current` in a dated choice list."""
    for entry in entries or []:
        if entry.get("current"):
            return entry.get("date") or None
    return None


def split_multi(value: str | None) -> list[str]:
    """The csv export joins multiple choices with a pipe."""
    return [v.strip() for v in (value or "").split("|") if v.strip()]


def make_investor(
    ctx: TaskContext, investor_id: int, name: str, classification: str | None = None
) -> EntityProxy:
    """A stub investor, referenced from a deal or an involvement. Merges with
    the full entity emitted from the investors source."""
    # deals reference their operating company without a classification: stay on
    # the common ancestor, or the stub won't merge with e.g. a `Person`
    schema = (
        INVESTOR_SCHEMATA.get(classification, DEFAULT_INVESTOR_SCHEMA)
        if classification
        else "LegalEntity"
    )
    proxy = ctx.make_entity(schema)
    proxy.id = ctx.make_slug("investor", investor_id)
    proxy.add("name", name)
    proxy.add("legalForm", get_label(INVESTOR_CLASSIFICATION, classification))
    proxy.add("sourceUrl", INVESTOR_URL % investor_id)
    return proxy


def make_participant(
    ctx: TaskContext, project: EntityProxy, investor: EntityProxy, role: str
) -> None:
    proxy = ctx.make_entity("ProjectParticipant")
    proxy.id = ctx.make_id("participant", project.id, investor.id, role)
    proxy.add("project", project)
    proxy.add("participant", investor)
    proxy.add("role", role)
    proxy.add("sourceUrl", project.first("sourceUrl"))
    ctx.emit(proxy)


def make_location(
    ctx: TaskContext, deal: str | int, nid: str, country: str | None = None
) -> EntityProxy:
    proxy = ctx.make_entity("RealEstate")
    proxy.id = nested_id(ctx, "location", deal, nid)
    proxy.add("name", f"Land deal #{deal} location")
    proxy.add("country", country)
    proxy.add("sourceUrl", DEAL_URL % deal)
    return proxy


def link_location(ctx: TaskContext, deal: str | int, land: EntityProxy) -> None:
    """followthemoney has no project-to-place edge, so the land a deal covers
    hangs off it as a generic link."""
    proxy = ctx.make_entity("UnknownLink")
    proxy.id = ctx.make_id("location", deal_id(ctx, deal), land.id)
    proxy.add("subject", deal_id(ctx, deal))
    proxy.add("object", land)
    proxy.add("role", "Deal location")
    proxy.add("sourceUrl", DEAL_URL % deal)
    ctx.emit(proxy)


# --- the json api -----------------------------------------------------------


def parse_deal(ctx: TaskContext, record: Record) -> None:
    version = record["selected_version"]
    deal = record["id"]
    country_code, country = record.get("country_code"), record.get("country_name")
    area = version.get("deal_size")
    intention = get_labels(
        INTENTION_OF_INVESTMENT, version.get("current_intention_of_investment")
    )
    negotiation = get_label(
        NEGOTIATION_STATUS, version.get("current_negotiation_status")
    )
    implementation = get_label(
        IMPLEMENTATION_STATUS, version.get("current_implementation_status")
    )
    signed = get_current_date(version.get("negotiation_status"))

    project = ctx.make_entity("Project")
    project.id = deal_id(ctx, deal)
    project.add("projectId", deal)
    name = f"Land deal #{deal}"
    if country:  # a few country names carry a comma themselves
        name = f"{name} ({country})"
    project.add("name", name)
    project.add("country", country_code)
    project.add("goal", intention)
    project.add("status", [negotiation, implementation])
    project.add("startDate", version.get("initiation_year") or signed)
    project.add("modifiedAt", record.get("fully_updated_at"))
    project.add("sourceUrl", DEAL_URL % deal)
    project.add(
        "summary",
        join_text(
            f"{area:,.0f} ha" if area else None,
            country,
            *intention,
            negotiation,
            implementation,
            sep=" · ",
        ),
    )
    ctx.emit(project)

    # the grant of land itself, the only schema carrying area and commodities
    grant = ctx.make_entity("License")
    grant.id = license_id(ctx, deal)
    grant.add("project", project)
    grant.add("name", f"Land grant for land deal #{deal}")
    grant.add("title", f"Land grant for land deal #{deal}")
    grant.add("country", country_code)
    if area:  # a lot of deals have an unknown (zero) size
        grant.add("area", area)
    grant.add("classification", intention)
    grant.add("status", [negotiation, implementation])
    # `License` is not an `Interval`, `contractDate` is the only date it carries
    grant.add("contractDate", signed or version.get("initiation_year"))
    grant.add("sourceUrl", DEAL_URL % deal)
    for field, vocabulary in PRODUCE:
        grant.add("commodities", get_labels(vocabulary, version.get(field)))
    ctx.emit(grant)

    for location in version.get("locations") or []:
        land = make_location(ctx, deal, location["nid"], country_code)
        point = location.get("point") or {}
        coordinates = point.get("coordinates")
        if coordinates:
            longitude, latitude = (format_coordinate(c) for c in coordinates)
            land.add("latitude", latitude)
            land.add("longitude", longitude)
        land.add(
            "summary", get_label(LOCATION_ACCURACY, location.get("level_of_accuracy"))
        )
        ctx.emit(land)
        link_location(ctx, deal, land)

    # the operating company is the investor the land was granted to
    company = version.get("operating_company")
    if company:
        operator = make_investor(
            ctx, company["id"], company["selected_version"]["name"]
        )
        ctx.emit(operator)
        make_participant(ctx, project, operator, "Operating company")

    # the ultimate parent investors behind the operating company, as computed by
    # land matrix by walking the involvement chain
    for investor in version.get("top_investors") or []:
        parent = make_investor(
            ctx, investor["id"], investor["name"], investor.get("classification")
        )
        ctx.emit(parent)
        make_participant(ctx, project, parent, "Top parent company")


def parse_investor(ctx: TaskContext, record: Record) -> None:
    version = record["selected_version"]
    proxy = make_investor(
        ctx, record["id"], version["name"], version.get("classification")
    )
    proxy.add("country", record.get("country_code"))
    proxy.add("jurisdiction", record.get("country_code"))
    proxy.add("website", version.get("homepage"))
    proxy.add("notes", version.get("comment"))
    proxy.add("modifiedAt", version.get("modified_at"))
    url = version.get("opencorporates")
    if url:
        # the field is not restricted to open corporates in practice
        if "opencorporates" in url:
            proxy.add("opencorporatesUrl", url)
            proxy.add("idNumber", url.rstrip("/").split("/")[-1])
        else:
            proxy.add("sourceUrl", url)
    ctx.emit(proxy)

    # involvements are the investor's parent relations, each listed exactly once
    for involvement in version.get("involvements") or []:
        parse_involvement(ctx, involvement, proxy)


def parse_involvement(
    ctx: TaskContext, involvement: Record, child: EntityProxy
) -> None:
    parent_id = involvement.get("parent_investor_id")
    if not parent_id or not involvement.get("parent_known"):
        return  # the parent investor is not part of the public subset
    if parent_id == involvement.get("child_investor_id"):
        return  # a handful of records list an investor as its own parent
    proxy = ctx.make_entity("Ownership")
    proxy.id = ctx.make_id("involvement", involvement["nid"])
    proxy.add("owner", ctx.make_slug("investor", parent_id))
    proxy.add("asset", child)
    proxy.add("role", get_label(INVOLVEMENT_ROLE, involvement.get("role")))
    proxy.add(
        "ownershipType", get_label(PARENT_RELATION, involvement.get("parent_relation"))
    )
    proxy.add(
        "sharesType", get_labels(INVESTMENT_TYPE, involvement.get("investment_type"))
    )
    proxy.add("percentage", involvement.get("percentage"))
    proxy.add("sharesValue", involvement.get("loans_amount"))
    proxy.add("sharesCurrency", involvement.get("loans_currency"))
    proxy.add("date", involvement.get("loans_date"))
    proxy.add("summary", involvement.get("comment"))
    proxy.add("sourceUrl", INVESTOR_URL % involvement["child_investor_id"])
    ctx.emit(proxy)


# --- the legacy csv export --------------------------------------------------


def parse_legacy_deal(ctx: TaskContext, record: Record) -> None:
    """The wide columns the json list endpoint leaves out."""
    deal = record["Deal ID"]
    if not deal:
        return

    project = ctx.make_entity("Project")
    project.id = deal_id(ctx, deal)
    project.add("name", record["Name of investment project"])
    project.add("notes", record["Overall comment"])
    project.add("description", record["Comment on intention of investment"])
    project.add("keywords", record["Deal scope"])
    project.add("createdAt", record["Created at"])
    project.add("amount", record["Purchase price"])
    project.add("currency", record["purchase_currency"])
    project.add("sourceUrl", DEAL_URL % deal)
    ctx.emit(project)

    grant = ctx.make_entity("License")
    grant.id = license_id(ctx, deal)
    grant.add("project", project)
    grant.add("type", split_multi(record["Nature of the deal"]))
    # the recurring price of the grant, as opposed to the purchase price
    grant.add("amount", record["Annual leasing fee"])
    grant.add("currency", record["leasing_currency"])
    grant.add("summary", record["Comment on land area"])
    ctx.emit(grant)


def parse_legacy_location(ctx: TaskContext, record: Record) -> None:
    """Names the places the json endpoint only gives coordinates for."""
    deal, nid = record["Deal ID"], record["ID"]
    if not (deal and nid):
        return

    land = make_location(ctx, deal, nid)
    land.add("name", record["Location"])
    land.add("alias", record["Facility name"])
    land.add("description", record["Location description"])
    land.add("notes", record["Comment on location"])
    land.add("summary", record["Spatial accuracy level"])
    point = record["Point"]
    if "," in point:
        latitude, longitude = point.split(",", 1)
        land.add("latitude", latitude.strip())
        land.add("longitude", longitude.strip())
    ctx.emit(land)
    # locations without coordinates are missing from the api response entirely
    link_location(ctx, deal, land)


def parse_legacy_contract(ctx: TaskContext, record: Record) -> None:
    deal, nid = record["Deal ID"], record["ID"]
    if not (deal and nid):
        return

    number = record["Contract number"]
    name = f"Contract {number}" if number else f"Contract for land deal #{deal}"
    proxy = ctx.make_entity("Contract")
    proxy.id = nested_id(ctx, "contract", deal, nid)
    proxy.add("name", name)
    proxy.add("title", name)
    proxy.add("project", deal_id(ctx, deal))
    proxy.add("procedureNumber", number)
    proxy.add("contractDate", record["Contract date"])
    proxy.add("summary", record["Comment on contract"])
    expires, duration = (
        record["Contract expiration date"],
        record["Duration of the agreement"],
    )
    proxy.add(
        "description",
        join_text(
            f"Expires {expires}" if expires else None,
            f"agreed for {duration} years" if duration else None,
            sep=", ",
        ),
    )
    ctx.emit(proxy)

    if expires:
        # `Contract` has no end date, keep it queryable on the deal instead
        project = ctx.make_entity("Project")
        project.id = deal_id(ctx, deal)
        project.add("endDate", expires)
        ctx.emit(project)


def datasource_path(record: Record) -> str:
    """The copy Land Matrix mirrors, where there is one. Sources it withholds
    carry a placeholder in place of a path."""
    path = record["File"]
    return "" if path == CONFIDENTIAL else path


def datasource_filename(record: Record) -> str:
    """A readable name for the archived copy.

    Land Matrix already slugifies the files it mirrors, it is the rows that only
    have a link that need cleaning up: a `:` in the last path segment makes the
    archive read the key as a uri scheme, and a handful run to several hundred
    characters of url fragment.

    The id goes on the end regardless, so that sources a deal cites under the
    same name - the same report behind three different query strings, say - keep
    their own archive entry instead of overwriting each other.
    """
    path = datasource_path(record)
    name = path.rsplit("/", 1)[-1] if path else ""
    if not name:
        name = record["URL"].split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
    name = re.sub(r"[:#?%!\\]+", "-", name).strip("-. ")[:MAX_FILENAME]
    stem, dot, extension = name.rpartition(".")
    if dot and len(extension) <= 5:
        return f"{stem} ({record['ID']}).{extension}"
    return f"{name} ({record['ID']})" if name else record["ID"]


def parse_legacy_datasource(ctx: TaskContext, record: Record) -> None:
    """Provenance: the reports, filings and contracts a deal is based on.

    Land Matrix mirrors most of them, and its own copy is fetched in preference
    to the original link, which is dead often enough after fifteen years. The
    link itself is kept as the document's `sourceUrl` either way.
    """
    deal, url, path = record["Deal ID"], record["URL"], datasource_path(record)
    if not deal:
        return

    project = ctx.make_entity("Project")
    project.id = deal_id(ctx, deal)
    if url.lower().startswith("http"):
        project.add("sourceUrl", url)
    ctx.emit(project)

    if not os.environ.get("LAKEHOUSE_URI"):
        return  # nowhere to archive the files, see the module docstring
    remote = LM_MEDIA_URL % path if path else url
    if not remote.lower().startswith("http"):
        return  # a handful of rows carry neither a file nor a usable link

    contact = " · ".join(p for p in (record["Email"], record["Phone"]) if p)
    notes = [record["Comment on data source"], f"Contact: {contact}" if contact else ""]
    try:
        file = ctx.fetch(
            remote,
            key=f"Data sources/Land deal #{deal}/{datasource_filename(record)}",
            title=record["Publication title"] or None,
            sourceUrl=url or remote,
            publishedAt=record["Date"] or None,
            author=record["Name"] or None,
            publisher=record["Organisation"] or None,
            keywords=record["Data source type"] or None,
            notes="\n".join(n for n in notes if n) or None,
        )
    except Exception as exc:
        ctx.log.warning("Cannot archive data source", url=remote, error=str(exc))
        return

    document = file.to_entity()
    ctx.emit(*file.make_parents())
    ctx.emit(document)

    proxy = ctx.make_entity("Documentation")
    proxy.id = nested_id(ctx, "documentation", deal, record["ID"])
    proxy.add("document", document)
    proxy.add("entity", project)
    proxy.add("role", record["Data source type"])
    proxy.add("date", record["Date"])
    ctx.emit(proxy)


LEGACY_HANDLERS = {
    "deals.csv": parse_legacy_deal,
    "locations.csv": parse_legacy_location,
    "contracts.csv": parse_legacy_contract,
    "datasources.csv": parse_legacy_datasource,
}


def parse_legacy(ctx: TaskContext, record: Record) -> None:
    LEGACY_HANDLERS[record["__table__"]](ctx, record)


HANDLERS = {
    "deals": parse_deal,
    "investors": parse_investor,
    "legacy": parse_legacy,
}


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    tx = ctx.task()
    HANDLERS[ctx.source.name](tx, record)
    yield from tx
