import re
from datetime import datetime
from followthemoney import EntityProxy
from ftmq.types import Entities
from ftmq.util import make_fingerprint_id, make_fingerprint

from investigraph.model import SourceContext
from investigraph.types import Record


def parse_date(value: str | None) -> str | None:
    """Parse /Date(timestamp)/ format to ISO date string."""
    if not value:
        return None
    match = re.match(r"/Date\((\d+)\)/", str(value))
    if match:
        timestamp_ms = int(match.group(1))
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")
    return None


def make_address(ctx: SourceContext, record: Record) -> EntityProxy | None:
    street = record.pop("StrasseConcealed", None)
    house_number = record.pop("HausnummerConcealed", None)
    postcode = record.pop("Postleitzahl", None)
    city = record.pop("OrtConcealed", None)
    state = record.pop("Bundesland", None)
    country = record.pop("Land", None)

    if postcode:
        postcode = str(postcode).zfill(5)

    street_full = f"{street} {house_number}".strip() if street else None
    full = ", ".join(filter(None, [street_full, f"{postcode} {city}" if postcode and city else city, state]))

    # Skip address if no meaningful data
    fingerprint = make_fingerprint_id(full)
    if not fingerprint:
        return None

    proxy = ctx.make_entity("Address")
    proxy.id = ctx.make_slug(fingerprint, prefix="de-addr")
    proxy.add("full", full)
    proxy.add("street", street_full)
    proxy.add("postalCode", postcode)
    proxy.add("city", city)
    proxy.add("country", country)
    proxy.add("state", state)
    return proxy


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    name = record.pop("NameConcealed", None)
    if not name:
        return

    city = record.get("OrtConcealed")
    proxy = ctx.make_entity("LegalEntity")
    proxy.id = ctx.make_id(make_fingerprint(name), city)
    proxy.add("name", name)

    address = make_address(ctx, record)
    if address:
        proxy.add("country", address.get("country"))
        proxy.add("address", address.caption)
        proxy.add("addressEntity", address)
        yield address

    proxy.add("idNumber", record.pop("MaStRNummer", None))
    proxy.add("idNumber", record.pop("AcerCodeConcealed", None))
    proxy.add("incorporationDate", parse_date(record.pop("TaetigkeitsBeginn", None)))
    proxy.add("dissolutionDate", parse_date(record.pop("TaetigkeitsEnde", None)))
    proxy.add("status", record.pop("TaetigkeitsStatus", None))
    proxy.add("classification", record.pop("Marktfunktion", None))

    marktrollen = record.pop("Marktrollen", None)
    if marktrollen:
        proxy.add("classification", marktrollen.split(","))

    yield proxy
