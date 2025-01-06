from datetime import datetime, timedelta

from banal import ensure_dict, ensure_list
from furl import furl
from memorious.logic.context import Context
from servicelayer import env

from utils import Data
from utils.cache import emit_cached


def seed(context: Context, data: Data):
    f = furl(context.params["url"])
    if not env.to_bool("FULL_RUN"):
        start_date = (
            env.get("START_DATE")
            or (
                datetime.now()
                - timedelta(**ensure_dict(context.params.get("timedelta")))
            )
            .date()
            .isoformat()
        )
        f.args["f.datum.start"] = start_date
    data["url"] = f.url
    context.emit(data=data)


def parse(context: Context, data: Data):
    res = context.http.rehash(data)

    for document in ensure_list(res.json["documents"]):
        detail_data = parse_drucksache(document)
        emit_cached(context, {**data, **detail_data, **{"meta": document}}, "download")

    # next page
    fu = furl(data["url"])
    if res.json["cursor"] != fu.args.get("cursor"):
        fu.args["cursor"] = res.json["cursor"]
        context.emit("cursor", data={**data, **{"url": fu.url}})


def parse_drucksache(document: Data) -> Data:
    base = None
    if document["herausgeber"] == "BT":
        base = "Bundestag"
    elif document["herausgeber"] == "BR":
        base = "Bundesrat"
    else:
        return {}
    data = {"base": base}
    data["title"] = " - ".join(
        (
            base,
            document["dokumentnummer"],
            document["drucksachetyp"],
            document["titel"],
        )
    )
    data["published_at"] = document["datum"]
    data["foreign_id"] = document["id"]
    if "urheber" in document:
        data["publisher"] = ", ".join([u["titel"] for u in document["urheber"]])
    else:
        data["publisher"] = document["herausgeber"]
    data["url"] = document["fundstelle"]["pdf_url"]
    return data


# def make_proxies(context: Context, data: Data):
#     m = data["meta"]

#     for item in ensure_list(m.get("urheber")):
#         proxy = make_body(zavod, item)
#         role = "Einbringender Urheber" if item.get("einbringer") else "Urheber"
#         rel = make_documentation(zavod, document, proxy, role, data)
#         zavod.emit(proxy)
#         zavod.emit(rel)

#     for item in ensure_list(m.get("ressort")):
#         proxy = make_body(zavod, item)
#         role = "Federführendes Ressort" if item["federfuehrend"] else "Ressort"
#         rel = make_documentation(zavod, document, proxy, role, data)
#         zavod.emit(proxy)
#         zavod.emit(rel)

#     for item in ensure_list(m.get("autoren_anzeige")):
#         proxy = make_person(zavod, item)
#         rel = make_documentation(zavod, document, proxy, "Autor", data)
#         zavod.emit(proxy)
#         zavod.emit(rel)

#     context.emit(data=data)


# def make_body(context: Zavod, data: Data) -> CE:
#     proxy = context.make("PublicBody")
#     proxy.id = context.make_slug(data["titel"])
#     proxy.add("name", data["titel"])
#     proxy.add("country", "de")
#     proxy.add("jurisdiction", "de")
#     return proxy


# def make_person(context: Zavod, data: Data) -> CE:
#     proxy = context.make("Person")
#     proxy.id = context.make_slug("author", data["id"])
#     proxy.add("name", data["autor_titel"])
#     proxy.add("summary", data["titel"])
#     proxy.add("country", "de")
#     return proxy


# def make_documentation(
#     context: Zavod, document: CE, entity: CE, role: str, data: Data
# ) -> CE:
#     proxy = context.make("Documentation")
#     proxy = context.make("Documentation")
#     proxy.id = context.make_id(document.id, entity.id, role)
#     proxy.add("role", role)
#     proxy.add("date", data["published_at"])
#     proxy.add("entity", entity)
#     proxy.add("document", document)
#     return proxy
