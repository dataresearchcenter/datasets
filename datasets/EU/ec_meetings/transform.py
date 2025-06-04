from typing import Generator

from fingerprints import generate as fp
from followthemoney.util import join_text, make_entity_id
from nomenklatura.entity import CE

from investigraph.model import SourceContext
from investigraph.types import CEGenerator, Record


def make_address(ctx: SourceContext, data: Record) -> CE | None:
    location = data.pop("Location")
    if not location:
        return
    id_ = ctx.make_id(location, prefix="addr")
    proxy = ctx.make_proxy("Address", id_)
    proxy.add("full", location)
    return proxy


def make_person(ctx: SourceContext, name: str, role: str, body: CE) -> CE:
    id_ = ctx.make_slug("person", make_entity_id(body.id, fp(name)))
    proxy = ctx.make_proxy("Person", id_)
    proxy.add("name", name)
    proxy.add("description", role)
    return proxy


def make_organization(
    ctx: SourceContext, regId: str, name: str | None = None
) -> CE | None:
    if not fp(regId):
        return
    id_ = ctx.make_slug(regId, prefix="eu-tr")
    proxy = ctx.make_proxy("Organization", id_)
    if fp(name):
        proxy.add("name", name)
    proxy.add("idNumber", regId)
    return proxy


def zip_things(
    things1: str, things2: str, scream: bool | None = False
) -> Generator[tuple[str, str], None, None]:
    t1 = [t.strip() for t in things1.split(",")]
    t2 = [t.strip() for t in things2.split(",")]
    if len(t1) == len(t2):
        yield from zip(t1, t2)
    elif len(t2) == 1:
        yield things1, things2
    else:
        if scream:
            raise Exception
            # log.error(f"Unable to unzip things: {things1} | {things2}")


def make_organizations(ctx: SourceContext, data: Record) -> CEGenerator:
    regIds = data.pop("Transparency register ID") or ""
    orgs = False
    for name, regId in zip_things(
        data.pop("Name of interest representative") or "",
        regIds,
    ):
        org = make_organization(ctx, regId, name)
        if org is not None:
            orgs = True
            yield org
    if not orgs:
        # yield only via id
        for regId in regIds.split(","):
            regId = regId.strip()
            org = make_organization(ctx, regId)
            if org is not None:
                yield org


def make_persons(
    ctx: SourceContext, data: Record, body: CE
) -> Generator[CE, None, None]:
    for name, role in zip_things(
        data.pop("Name of EC representative"),
        data.pop("Title of EC representative") or "",
        scream=True,
    ):
        yield make_person(ctx, name, role, body)


def make_event(
    ctx: SourceContext, data: Record, organizer: CE, involved: list[CE]
) -> Generator[CE, None, None]:
    date = data.pop("Date of meeting")
    participants = [o for o in make_organizations(ctx, data)]
    id_ = ctx.make_slug(
        "meeting",
        date,
        make_entity_id(organizer.id, *sorted([p.id for p in participants])),
    )
    proxy = ctx.make_proxy("Event", id_)
    label = join_text(*[p.first("name") for p in participants])
    name = f"{date} - {organizer.caption} x {label}"
    proxy.add("name", name)
    proxy.add("date", date)
    proxy.add("summary", data.pop("Subject of the meeting"))
    proxy.add("organizer", organizer)
    proxy.add("involved", involved)
    proxy.add("involved", participants)
    portfolio = data.pop("Portfolio", None)
    if portfolio:
        proxy.add("notes", "Portfolio: " + portfolio)

    address = make_address(ctx, data)
    if address is not None:
        proxy.add("location", address.caption)
        proxy.add("address", address.caption)
        proxy.add("addressEntity", address)
        yield address

    yield from participants
    yield proxy


def parse_record(ctx: SourceContext, data: Record, body: CE):
    involved = [x for x in make_persons(ctx, data, body)]
    yield from make_event(ctx, data, body, involved)
    yield from involved

    for member in involved:
        id_ = ctx.make_slug("membership", make_entity_id(body.id, member.id))
        rel = ctx.make_proxy("Membership", id_)
        rel.add("organization", body)
        rel.add("member", member)
        rel.add("role", member.get("description"))
        yield rel


def parse_record_ec(ctx: SourceContext, data: Record):
    # meetings of EC representatives
    name = data.pop("Name of cabinet")
    id_ = ctx.make_slug(fp(name))
    body = ctx.make_proxy("PublicBody", id_)
    body.add("name", name)
    body.add("jurisdiction", "eu")

    yield body
    yield from parse_record(ctx, data, body)


def parse_record_dg(ctx: SourceContext, data: Record):
    # meetings of EC Directors-General
    acronym = data.pop("Name of DG - acronym")
    id_ = ctx.make_slug("dg", acronym)
    body = ctx.make_proxy("PublicBody", id_)
    body.add("name", data.pop("Name of DG - full name"))
    body.add("weakAlias", acronym)
    body.add("jurisdiction", "eu")

    yield body
    yield from parse_record(ctx, data, body)


def handle(ctx: SourceContext, record: Record, ix: int) -> CEGenerator:
    if ctx.source.name.startswith("ec"):
        handler = parse_record_ec
    else:
        handler = parse_record_dg
    yield from handler(ctx, record)
