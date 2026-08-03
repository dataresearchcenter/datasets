"""
Turn the crawled portal rows into entities.

Five kinds of record arrive from the extract stage, distinguished by `type`:
meetings hosted by a commissioner, by their cabinet, by a directorate-general
or by an executive agency, plus the missions of a commissioner.

Note that the portal no longer publishes the transparency register id of the
interest representatives it lists, so organisations are keyed by their
fingerprinted name instead.

Where a `LAKEHOUSE_URI` is configured, the minutes a meeting links to are
archived there, emitted as documents and tied to that meeting by a
`Documentation` entity.
"""

import os

from fingerprints import generate as fp
from followthemoney import EntityProxy
from followthemoney.util import join_text, make_entity_id
from ftm_lakehouse import get_archive
from ftmq.types import Entities
from investigraph.model import SourceContext
from investigraph.types import Record

MISSION = "mission"
COMMISSIONER = "commissioner"
CABINET = "cabinet"

# the sections the portal organises its hosts in, used as archive folders
SECTIONS = {
    COMMISSIONER: "Commissioners",
    CABINET: "Cabinets",
    "dg": "Directorates-General",
    "ea": "Executive agencies",
}

COSTS = (
    ("travel_costs", "Travel costs"),
    ("accommodation_costs", "Accommodation costs"),
    ("daily_allowances", "Daily allowances"),
    ("miscellaneous_costs", "Miscellaneous costs"),
)


def make_address(ctx: SourceContext, location: str) -> EntityProxy | None:
    if not location:
        return None
    proxy = ctx.make_entity("Address", ctx.make_id(location, prefix="addr"))
    proxy.add("full", location)
    return proxy


def make_commissioner(
    ctx: SourceContext, name: str, title: str | None = None
) -> EntityProxy | None:
    """A commissioner hosting meetings or missions. Keyed by name: the portal
    issues a fresh id per college, but a re-appointed commissioner is one
    person."""
    if not fp(name):
        return None
    proxy = ctx.make_entity("Person", ctx.make_slug(COMMISSIONER, fp(name)))
    proxy.add("name", name)
    if title and title != name:
        proxy.add("alias", title)
    return proxy


def make_body(ctx: SourceContext, record: Record) -> EntityProxy:
    """The public body hosting a meeting - a cabinet, a directorate-general or
    an executive agency."""
    label, name = record["host_label"], record["host_name"]
    if record["type"] == CABINET:
        proxy = ctx.make_entity("PublicBody", ctx.make_slug(CABINET, fp(label)))
    else:
        # `label` is the acronym the portal lists the body under
        proxy = ctx.make_entity("PublicBody", ctx.make_slug(record["type"], label))
        proxy.add("weakAlias", label)
    proxy.add("name", name)
    proxy.add("jurisdiction", "eu")
    return proxy


def make_representative(
    ctx: SourceContext, host: EntityProxy, value: str
) -> EntityProxy | None:
    """A commission representative, published as `Name, Role` - cabinet members
    come without a role."""
    name, _, role = value.partition(",")
    name, role = name.strip(), role.strip()
    if not fp(name):
        return None
    # scoped to the hosting body, there is nothing to tell namesakes apart
    proxy = ctx.make_entity(
        "Person", ctx.make_slug("person", make_entity_id(host.id, fp(name)))
    )
    proxy.add("name", name)
    proxy.add("description", role or None)
    return proxy


def make_membership(
    ctx: SourceContext, host: EntityProxy, member: EntityProxy, role: str | None = None
) -> EntityProxy:
    proxy = ctx.make_entity(
        "Membership", ctx.make_slug("membership", make_entity_id(host.id, member.id))
    )
    proxy.add("organization", host)
    proxy.add("member", member)
    proxy.add("role", role or member.get("description"))
    return proxy


def make_organisation(ctx: SourceContext, name: str) -> EntityProxy | None:
    if not fp(name):
        return None
    proxy = ctx.make_entity("Organization", ctx.make_fingerprint_id(name))
    proxy.add("name", name)
    return proxy


def clean_path(value: str, limit: int | None = None) -> str:
    """A single archive path component - no separators, no runaway length."""
    value = " ".join(value.replace("/", "-").replace("\\", "-").split())
    if limit is not None and len(value) > limit:
        # back off to the last word boundary instead of cutting mid-word
        value = value[:limit].rsplit(" ", 1)[0]
    return value.strip(" .-,")


def make_minutes_key(record: Record, event: EntityProxy) -> str:
    """A verbose archive path, mirroring how the portal organises its hosts.

    A host holds several meetings on the same date, sometimes even with the
    same organisations - and those are one and the same event here, so the
    event id is what keeps the file names apart.
    """
    folders = [SECTIONS.get(record["type"], "Meetings")]
    if record["college"]:
        folders.append(record["college"])
    folders.append(record["host_name"] or record["host_label"])

    name = join_text("Minutes", "-", record["date"], sep=" ")
    # the organisations keep the name readable, but they can run very long
    organisations = clean_path(", ".join(record["organisations"]), limit=80)
    if organisations:
        name = f"{name} - {organisations}"
    filename = f"{clean_path(name)} ({event.id}).pdf"
    return "/".join([*(clean_path(f) for f in folders), filename])


def make_documentation(
    ctx: SourceContext, record: Record, event: EntityProxy
) -> Entities:
    """Archive the minutes a meeting publishes, emit them as documents and tie
    each of them to the meeting.

    Only runs against a configured lakehouse - without one there is nowhere to
    put the files, and the extract stage doesn't collect their links either.
    """
    if not os.environ.get("LAKEHOUSE_URI"):
        return
    # they are all served as `.../<uuid>/minutes`, so name them after the
    # archive path instead of that bare last path segment
    key = make_minutes_key(record, event)
    title = join_text("Minutes:", event.caption)
    for ix, url in enumerate(record.get("minutes") or []):
        path = key if not ix else key.replace(".pdf", f" ({ix + 1}).pdf")
        try:
            file = get_archive(ctx.dataset).store(
                url, key=path, name=path.rsplit("/", 1)[-1], title=title, sourceUrl=url
            )
        except Exception as exc:
            ctx.log.warning("Cannot archive minutes", url=url, error=str(exc))
            continue
        document = file.to_entity()
        yield from file.make_parents()
        yield document

        proxy = ctx.make_entity(
            "Documentation",
            ctx.make_slug("documentation", make_entity_id(document.id, event.id)),
        )
        proxy.add("document", document)
        proxy.add("entity", event)
        proxy.add("role", "Minutes")
        proxy.add("date", record["date"])
        yield proxy


def make_meeting(ctx: SourceContext, record: Record) -> Entities:
    if record["type"] == COMMISSIONER:
        host = make_commissioner(ctx, record["host_label"], record["host_name"])
    else:
        host = make_body(ctx, record)
    if host is None:
        ctx.log.warning("Cannot build host", url=record["url"])
        return
    yield host

    involved: list[EntityProxy] = []
    for value in record["representatives"]:
        person = make_representative(ctx, host, value)
        if person is not None:
            involved.append(person)
            yield person
            yield make_membership(ctx, host, person)

    if record["type"] == CABINET:
        # the cabinet is named after the commissioner leading it
        commissioner = make_commissioner(ctx, record["host_label"])
        if commissioner is not None:
            yield commissioner
            yield make_membership(ctx, host, commissioner, "Commissioner")

    organisations = []
    for name in record["organisations"]:
        org = make_organisation(ctx, name)
        if org is not None:
            organisations.append(org)
            yield org

    date = record["date"]
    proxy = ctx.make_entity(
        "Event",
        ctx.make_slug(
            "meeting",
            make_entity_id(host.id, date, *sorted(o.id for o in organisations)),
        ),
    )
    label = join_text(*[o.first("name") for o in organisations], sep=", ")
    proxy.add("name", join_text(date, "-", host.caption, "x", label))
    proxy.add("date", date)
    proxy.add("summary", record["subject"])
    proxy.add("organizer", host)
    proxy.add("involved", involved)
    proxy.add("involved", organisations)
    proxy.add("sourceUrl", record["url"])
    if record["college"]:
        proxy.add("notes", f"College: {record['college']}")

    yield from make_documentation(ctx, record, proxy)

    address = make_address(ctx, record["location"])
    if address is not None:
        proxy.add("location", address.caption)
        proxy.add("address", address.caption)
        proxy.add("addressEntity", address)
        yield address

    yield proxy


def make_mission(ctx: SourceContext, record: Record) -> Entities:
    person = make_commissioner(ctx, record["host_label"], record["host_name"])
    if person is None:
        ctx.log.warning("Cannot build commissioner", url=record["url"])
        return
    yield person

    start, end = record["start_date"], record["end_date"]
    proxy = ctx.make_entity(
        "Event",
        ctx.make_slug(
            MISSION,
            make_entity_id(
                person.id, start, end, record["location"], record["purpose"]
            ),
        ),
    )
    name = join_text(start, "-", person.caption)
    if record["purpose"]:
        name = f"{name}: {record['purpose']}"
    proxy.add("name", name)
    proxy.add("startDate", start)
    proxy.add("endDate", end)
    proxy.add("summary", record["purpose"])
    proxy.add("description", record["context"])
    proxy.add("organizer", person)
    proxy.add("involved", person)
    proxy.add("sourceUrl", record["url"])
    # the portal publishes the reimbursed amounts without stating a currency
    costs = [f"{label}: {record[key]}" for key, label in COSTS if record.get(key)]
    proxy.add("notes", join_text(*costs, sep="\n"))
    proxy.add("notes", record["comments"] or None)
    if record["college"]:
        proxy.add("notes", f"College: {record['college']}")

    address = make_address(ctx, record["location"])
    if address is not None:
        proxy.add("location", address.caption)
        proxy.add("address", address.caption)
        proxy.add("addressEntity", address)
        yield address

    yield proxy


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    if record["type"] == MISSION:
        yield from make_mission(ctx, record)
    else:
        yield from make_meeting(ctx, record)
