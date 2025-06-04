import os
from datetime import datetime

from investigraph.model import SourceContext, TaskContext
from investigraph.types import CE, CEGenerator, Record
from investigraph.util import get_func

BASE_PATH = os.path.dirname(os.path.realpath(__file__))

get_title_from_sru_request = get_func("./sru.py:get_title_from_sru_request", BASE_PATH)
get_title_from_standard_vocab = get_func(
    "./standard_vocab.py:get_title_from_standard_vocab", BASE_PATH
)


BASE = "https://d-nb.info/standards/elementset/gnd#"

PERSON_MAPPING = {
    "preferredNameForThePerson": "name",
    "personalName": "name",
    "variantNameForThePerson": "alias",
    "forename": "firstName",
    "surname": "lastName",
    "dateOfBirth": "birthDate",
    "dateOfDeath": "deathDate",
    "placeOfBirth": "birthPlace",
    "gndIdentifier": "gndId",
    "gender": "gender",
    "geographicAreaCode": "country",
    "academicDegree": "title",
    "biographicalOrHistoricalInformation": "description",
    "professionOrOccupation": "position",
}

CORPORATE_MAPPING = {
    "preferredNameForTheCorporateBody": "name",
    "variantNameForTheCorporateBody": "alias",
    "abbreviatedNameForTheCorporateBody": "weakAlias",
    "dateOfEstablishment": "incorporationDate",
    "gndIdentifier": "gndId",
    "geographicAreaCode": "country",
    "gndSubjectCategory": "classification",
    "homepage": "website",
    "placeOfBusiness": "address",
}


def get_country_code(country_uri: str) -> str:
    area_code = country_uri.split("#")[-1]
    code_elements = area_code.split("-")
    if len(code_elements) > 1:
        return code_elements[1].lower()
    else:
        region_code = code_elements[0]
        if region_code == "XA":
            return "eu"
        elif region_code == "XQ":
            return "zz"
        else:
            return country_uri


def process(key: str, values: list[str]) -> list[str]:
    if "date" in key.lower():
        values = [convert_to_iso_date(elem) for elem in values]
    if "country" in key.lower():
        values = [get_country_code(elem) for elem in values]
    if "gender" in key.lower():
        values = [extract_gender(elem) for elem in values]
    if "position" in key.lower():
        values = [get_title_from_vocab_url(elem, "profession") for elem in values]
    if "place" in key.lower() or "address" in key.lower():
        values = [get_title_from_vocab_url(elem, "place") for elem in values]
    if "classification" in key.lower():
        values = [get_title_from_standard_vocab(elem) for elem in values]
    return values


def get_values(record: Record, key: str) -> list[str]:
    try:
        return [value.get("@value", value.get("@id")) for value in record[BASE + key]]
    except KeyError:
        return []


def get_title_from_vocab_url(url: str, category_type: str) -> str:
    gndId = extract_id(url)
    return get_title_from_sru_request(gndId, category_type)


# TODO: Move into general utils function and convert
def convert_to_iso_date(date_str: str) -> str:
    date_str = date_str.replace("XX.", "")
    formats = [
        "%Y-%m-%d",
        "%Y-%m",
        "%Y",
        "%d.%m.%Y",
        "%d.%m.%y",
        "%m.%Y",
        "%Y, %d.%m.",
        "%Y,%d.%m.",
        "%Y,%m,%d",
        "%Y,%b.",
        "%Y,%b",
        "%Y,%B",
        "%Y,%d.%B",
        "%Y/%m",
        "%Y,%d.%b.",
        "%Y,%d.%B",
    ]
    for format_str in formats:
        try:
            date_obj = datetime.strptime(date_str, format_str)
            if format_str == "%Y":
                return date_obj.strftime("%Y")
            elif "%d" not in format_str:
                return date_obj.strftime("%Y-%m")
            else:
                return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def extract_id(value: str) -> str:
    return value.split("/")[-1]


def extract_gender(gender_str: str) -> str:
    gender = gender_str.split("#")[-1]
    if gender == "male" or gender == "female":
        return gender
    return ""


def get_reference_id(record: Record, domain: str) -> list[str]:
    key = "http://www.w3.org/2002/07/owl#sameAs"
    if key in record.keys():
        return [
            item.get("@id", "").split("/")[-1]
            for item in record[key]
            if domain in item.get("@id")
        ]
    return []


def add_reference_urls(proxy, record: Record) -> CE:
    reference_domains = {"wikidata": "wikidataId", "viaf": "viafId", "isni": "isni"}
    for reference_domain, ftm_key in reference_domains.items():
        proxy.add(ftm_key, get_reference_id(record, reference_domain))
    return proxy


def create_relationships(ctx: TaskContext, person_id: str, record: Record) -> None:
    REL_BASE = "https://d-nb.info/standards/elementset/agrelon"
    for key in record:
        if REL_BASE in key:
            relation = key.split("#")[-1]
            if relation.startswith("has"):
                relation = relation[3:]
            for relative in record[key]:
                relative_id = extract_id(relative["@id"])
                ctx.emit(make_family(ctx, person_id, relative_id, relation))


def add_properties(proxy, record: Record, mapping: dict[str, str]) -> CE:
    for gnd_key, ftm_key in mapping.items():
        values = get_values(record, gnd_key)
        proc_values = process(ftm_key, values)
        proxy.add(ftm_key, proc_values)
    proxy = add_reference_urls(proxy, record)
    return proxy


def make_family(
    ctx: TaskContext, person_id: str, relative_id: str, relation: str
) -> CE:
    proxy = ctx.make_proxy("Family")
    proxy.id = ctx.make_slug(relative_id, person_id, "family")
    proxy.add("person", person_id)
    proxy.add("relative", ctx.make_slug(relative_id))
    proxy.add("relationship", relation)
    return proxy


def make_organization(ctx: TaskContext, url: str) -> CE:
    proxy = ctx.make_proxy("Organization")
    proxy.id = ctx.make_slug(extract_id(url))
    proxy.add("sourceUrl", url)
    return proxy


def make_membership(ctx: TaskContext, member: CE, organization_url: str) -> CE:
    proxy = ctx.make_proxy("Membership")
    proxy.id = ctx.make_slug(extract_id(organization_url), member.id, "membership")
    proxy.add("organization", make_organization(ctx, organization_url))
    proxy.add("member", member)
    return proxy


def make_person(ctx: TaskContext, record: Record) -> CE | None:
    proxy = ctx.make_proxy("Person")
    proxy.id = ctx.make_slug(extract_id(record["@id"]))
    proxy.add("sourceUrl", record["@id"])
    proxy = add_properties(proxy, record, PERSON_MAPPING)
    create_relationships(ctx, proxy.id, record)
    for pos in proxy.get("position"):
        if "politiker" in pos.lower():
            proxy.add("topics", "role.pep")
            break
    if proxy.to_dict()["properties"]:
        return proxy


def make_legalentity(ctx: TaskContext, record: Record) -> CE:
    proxy = ctx.make_proxy("LegalEntity")
    proxy.id = ctx.make_slug(extract_id(record["@id"]))
    proxy.add("sourceUrl", record["@id"])
    proxy.add(
        "legalForm", [legal_form.split("#")[-1] for legal_form in record["@type"]]
    )
    proxy = add_properties(proxy, record, CORPORATE_MAPPING)
    if BASE + "corporateBodyIsMember" in record.keys():
        for membership in get_values(record, "corporateBodyIsMember"):
            ctx.emit(make_membership(ctx, proxy, membership))
    return proxy


def make_company(ctx: TaskContext, record: Record) -> CE:
    proxy = ctx.make_proxy("Company")
    proxy.id = ctx.make_slug(extract_id(record["@id"]))
    proxy.add("sourceUrl", record["@id"])
    proxy = add_properties(proxy, record, CORPORATE_MAPPING)
    return proxy


def get_type(record: Record) -> str | None:
    # TODO: Adjust for multiple corporate types
    if "@type" in record.keys():
        return record["@type"][0].split("#")[-1]


def handle(ctx: SourceContext, record: Record, ix: int) -> CEGenerator:
    ctx = ctx.task()
    if ctx.source.name == "legalentity":
        record_type = get_type(record)
        if record_type == "Company":
            yield make_company(ctx, record)
        else:
            yield make_legalentity(ctx, record)
    elif ctx.source.name == "person":
        person = make_person(ctx, record)
        if person is not None:
            yield person
    yield from ctx
