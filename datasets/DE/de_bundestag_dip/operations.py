from datetime import datetime, timedelta

from anystore.types import SDict
from banal import ensure_dict, ensure_list
from furl import furl
from memorious.logic.context import Context
from normality import squash_spaces
# from followthemoney import E, EntityProxy
# from ftmq.types import Entities


def seed(context: Context, data: SDict):
    f = furl(context.params["url"])
    if not context.env.full_run:
        start_date = (
            context.env.start_date
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


def parse(context: Context, data: SDict):
    res = context.http.rehash(data)

    for document in ensure_list(res.json["documents"]):
        if document:
            detail_data = parse_drucksache(document)
            # detail_data["entities"] = [
            #     e.to_dict() for e in make_entities(context, document)
            # ]
            context.emit("download", data={**data, **detail_data, **{"meta": document}})

    # next page
    f = furl(data["url"])
    if res.json["cursor"] != f.args.get("cursor"):
        f.args["cursor"] = res.json["cursor"]
        context.emit("cursor", data={**data, **{"url": f.url}})


def parse_drucksache(document: SDict) -> SDict:
    base = None
    if document["herausgeber"] == "BT":
        base = "Bundestag"
    elif document["herausgeber"] == "BR":
        base = "Bundesrat"
    else:
        return {}
    data = {"base": base}
    document["titel"] = squash_spaces(document["titel"])
    data["published_at"] = document["datum"]
    data["foreign_id"] = document["id"]
    if "urheber" in document:
        data["publisher"] = ", ".join([u["titel"] for u in document["urheber"]])
    else:
        data["publisher"] = document["herausgeber"]
    data["url"] = document["fundstelle"]["pdf_url"]
    return data


# def make_entities(context: Context, data: SDict) -> Entities:
#     for item in ensure_list(data.get("urheber")):
#         entity = make_body(context, item)
#         role = "Einbringender Urheber" if item.get("einbringer") else "Urheber"
#         yield entity
#         yield make_documentation(context, document, entity, role, data)

#     for item in ensure_list(data.get("ressort")):
#         entity = make_body(context, item)
#         role = "Federführendes Ressort" if item["federfuehrend"] else "Ressort"
#         yield entity
#         yield make_documentation(context, document, entity, role, data)

#     for item in ensure_list(data.get("autoren_anzeige")):
#         entity = make_person(context, item)
#         yield entity
#         yield make_documentation(context, document, entity, "Autor", data)

#     context.emit(data=data)


# def make_body(context: Context, data: SDict) -> E:
#     entity = context.make_entity("PublicBody")
#     entity.id = entity.make_id(data["titel"])
#     entity.add("name", data["titel"])
#     entity.add("country", "de")
#     entity.add("jurisdiction", "de")
#     return entity


# def make_person(context: Context, data: SDict) -> E:
#     entity = context.make_entity("Person")
#     entity.id = entity.make_id("author", data["id"])
#     entity.add("name", data["autor_titel"])
#     entity.add("summary", data["titel"])
#     entity.add("country", "de")
#     return entity


# def make_documentation(
#     context: Context, document: E, entity: E, role: str, data: SDict
# ) -> E:
#     entity = context.make_entity("Documentation")
#     entity.id = entity.make_id(document.id, entity.id, role)
#     entity.add("role", role)
#     entity.add("date", data["published_at"])
#     entity.add("entity", entity)
#     entity.add("document", document)
#     return entity
