from followthemoney import EntityProxy
from ftmq.types import Entities
from investigraph.model import SourceContext, TaskContext
from investigraph.types import Record
from investigraph.util import join_text


def get_values(record: Record, category: str, subcategory) -> list[str]:
    return [elem[subcategory] for elem in record[category]]


def add_payer_properties(proxy: EntityProxy, record: Record) -> EntityProxy:
    proxy.add("name", get_values(record, "Geldgeber", "fulltext"))
    proxy.add("sourceUrl", get_values(record, "Geldgeber", "fullurl"))
    proxy.add("topics", record["Branche"])
    proxy.add("keywords", record["Schlagworte"])
    return proxy


def create_payer(ctx: TaskContext, record: Record) -> EntityProxy:
    if record["Kategorie"][0] == "natürliche Person":
        proxy = make_person(ctx, record)
    else:
        proxy = make_legalentity(ctx, record)
    return proxy


def make_address(ctx: TaskContext, record: Record) -> EntityProxy | None:
    proxy = ctx.make_entity("Address")
    try:
        proxy.id = ctx.make_slug(
            *record["Ort"], *record["Bundesland"], prefix="de-addr"
        )
    except ValueError:  # empty slug: no location data
        return None
    proxy.add("city", record["Ort"])
    proxy.add("state", record["Bundesland"])
    proxy.add("country", "de")
    proxy.add("full", join_text(*record["Ort"], *record["Bundesland"], sep=", "))
    ctx.emit(proxy)
    return proxy


def make_organization(ctx: TaskContext, record: Record) -> EntityProxy:
    proxy = ctx.make_entity("Organization")
    proxy.id = ctx.make_slug("party", record["Empfänger"][0]["fulltext"])
    proxy.add("name", get_values(record, "Empfänger", "fulltext"))
    proxy.add("sourceUrl", get_values(record, "Empfänger", "fullurl"))
    proxy.add("topics", "pol.party")
    ctx.emit(proxy)
    return proxy


def make_legalentity(ctx: TaskContext, record: Record) -> EntityProxy:
    proxy = ctx.make_entity("LegalEntity")
    proxy.id = ctx.make_id(record["Geldgeber"][0]["fulltext"])
    proxy = add_payer_properties(proxy, record)
    address = make_address(ctx, record)
    if address:
        proxy.add("addressEntity", address)
        proxy.add("address", address.get("full"))
    ctx.emit(proxy)
    return proxy


def make_person(ctx: TaskContext, record: Record) -> EntityProxy:
    proxy = ctx.make_entity("Person")
    proxy.id = ctx.make_slug("person", record["Geldgeber"][0]["fulltext"])
    proxy = add_payer_properties(proxy, record)
    address = make_address(ctx, record)
    if address:
        proxy.add("addressEntity", address)
        proxy.add("address", address.get("full"))
    ctx.emit(proxy)
    return proxy


def make_payment(
    ctx: TaskContext, record: Record, payer: str, beneficiary: str
) -> EntityProxy:
    proxy = ctx.make_entity("Payment")
    date = record["printouts"]["Jahr"]
    amounts = [str(round(a, 2)) for a in record["printouts"]["Betrag"]]
    proxy.id = ctx.make_slug(record["fulltext"])
    proxy.add("payer", payer)
    proxy.add("beneficiary", beneficiary)
    proxy.add("sourceUrl", record["fullurl"])
    proxy.add("amountEur", amounts)
    proxy.add("amount", amounts)
    proxy.add("currency", "EUR")
    proxy.add("date", date)
    ctx.emit(proxy)
    return proxy


def handle(ctx: SourceContext, record: Record, ix: int) -> Entities:
    tx = ctx.task()
    payer = create_payer(tx, record["printouts"])
    beneficiary = make_organization(tx, record["printouts"])
    make_payment(tx, record, payer.id, beneficiary.id)
    yield from tx
