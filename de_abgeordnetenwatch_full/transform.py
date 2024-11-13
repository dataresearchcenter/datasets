from enum import StrEnum

from banal import ensure_dict, ensure_list
from investigraph import Context
from investigraph.types import CE, CEGenerator, Record
from investigraph.util import join_text


class EntityType(StrEnum):
    POLITICIAN = "politician"
    MANDATE = "candidacy_mandate"


def make_politician(context: Context, data: Record) -> CE:
    proxy = context.make("Person")
    proxy.id = context.make_slug("person", data.pop("id"))
    title = data.get("field_title")
    firstName = data.get("first_name")
    lastName = data.get("last_name")
    proxy.add("name", join_text(title, firstName, lastName))
    proxy.add("title", title)
    proxy.add("topics", "role.pep")
    proxy.add("firstName", firstName)
    proxy.add("lastName", lastName)
    proxy.add("phone", data.get("phoneNumber"))
    proxy.add("sourceUrl", data.get("api_url"))
    proxy.add("sourceUrl", data.get("abgeordnetenwatch_url"))
    proxy.add("gender", data.get("sex"))
    proxy.add("birthDate", data.get("year_of_birth"))
    proxy.add("education", data.get("education"))
    proxy.add("wikidataId", data.get("qid_wikidata"))
    return proxy


def make_party(context: Context, name: str) -> CE:
    proxy = context.make("Organization")
    proxy.id = context.make_slug(name)
    proxy.add("name", name)
    proxy.add("topics", "pol.party")
    return proxy


def make_membership(context: Context, person: CE, party: CE) -> CE:
    proxy = context.make("Membership")
    proxy.id = context.make_id(party.id, person.id)
    proxy.add("member", person)
    proxy.add("organization", party)
    return proxy


def make_party_membership(context, person: CE, party_name: str) -> CEGenerator:
    party = make_party(context, party_name)
    yield party
    yield make_membership(context, person, party)


def make_fraction_membership(context: Context, person: CE, data: Record) -> CEGenerator:
    fraction = data.pop("fraction")
    party = make_party(context, fraction.pop("label"))
    party.add("legalForm", fraction.pop("entity_type"))
    party.add("sourceUrl", fraction.pop("api_url"))
    yield party
    membership = make_membership(context, person, party)
    membership.add("startDate", data.pop("valid_from"))
    membership.add("endDate", data.pop("valid_until"))
    membership.add("summary", data.pop("label"))
    yield membership


def make_position(
    context: Context, name: str, country: str, subnational_area: str | None = None
) -> CE:
    parts: list[str] = [name, country]
    if subnational_area is not None:
        parts.append(subnational_area)
    proxy = context.make("Position")
    proxy.id = context.make_id(*parts)
    proxy.add("name", name)
    proxy.add("country", country)
    proxy.add("subnationalArea", subnational_area)
    return proxy


def make_occupancy(
    context: Context, person: CE, position: CE, start_date: str, end_date: str
) -> CE:
    occupancy = context.make("Occupancy")
    # Include started and ended strings so that two occupancies, one missing start
    # and and one missing end, don't get normalisted to the same ID
    parts = [
        person.id,
        position.id,
        "started",
        start_date,
        "ended",
        end_date,
    ]
    occupancy.id = context.make_id(*parts)
    occupancy.add("holder", person)
    occupancy.add("post", position)
    occupancy.add("startDate", start_date)
    occupancy.add("endDate", end_date)
    # occupancy.add("status", status.value)

    person.add("topics", "role.pep")
    person.add("country", position.get("country"))

    return occupancy


def make_mandate(context: Context, record: Record, politician: CE) -> CEGenerator:
    parliament_period_detail = record.pop("parliament_period")
    parliament_detail = parliament_period_detail.pop("parliament")
    parliament_label = parliament_detail["label"]
    parliament_label_long = parliament_detail["label_external_long"]

    # Create position
    position = make_position(
        context,
        name="Member of the {}".format(parliament_label_long),
        country="Germany",
        subnational_area=(
            parliament_label if parliament_label != "Bundestag" else None
        ),
    )

    record_start_date = record.pop("start_date") or parliament_period_detail.pop(
        "start_date_period"
    )
    record_end_date = record.pop("end_date") or parliament_period_detail.pop(
        "end_date_period"
    )
    occupancy = make_occupancy(
        context,
        politician,
        position,
        start_date=record_start_date,
        end_date=record_end_date,
    )

    yield position
    yield occupancy


def handle(ctx: Context, record: Record, *args, **kwargs) -> CEGenerator:
    tx = ctx.task()
    if record["entity_type"] == EntityType.POLITICIAN:
        politician = make_politician(tx, record)
        yield politician
        party_name = ensure_dict(record.pop("party")).get("label")
        if party_name is not None:
            yield from make_party_membership(tx, politician, party_name)
    if record["entity_type"] == EntityType.MANDATE:
        politician = make_politician(tx, record["politician"])
        yield politician
        yield from make_mandate(tx, record, politician)
        for membership in ensure_list(record.pop("fraction_membership", None)):
            yield from make_fraction_membership(tx, politician, membership)
    yield from tx
