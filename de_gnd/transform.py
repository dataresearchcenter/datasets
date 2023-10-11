from investigraph.types import CE, CEGenerator, Record
from investigraph.model import Context

from datetime import datetime
import locale

locale.setlocale(locale.LC_ALL, 'de_DE.utf8')


PERSON_MAPPING = {
    'preferredNameEntityForThePerson': 'name',
    'variantNameForThePerson': 'alias',          
    'forename': 'firstName',
    'surname': 'lastName',
    'dateOfBirth': 'birthDate',
    'dateOfDeath': 'deathDate'
    }

CORPORATE_MAPPING = {
    'preferredNameForTheCorporateBody': 'name',
    'variantNameForTheCorporateBody': 'alias',
    'abbreviatedNameForTheCorporateBody': 'alias',
    'geographicAreaCode': 'country',
    'spatialAreaOfActivity': 'country',
    'placeOfBusiness': 'country',
    'dateOfEstablishment': 'incorporationDate',
    'homepage': 'website',
}


def convert_to_iso_date(date_str: str) -> str:
    date_str = date_str.replace('XX.', '')
    formats = [
        "%Y-%m-%d", "%Y-%m", "%Y", "%d.%m.%Y", "%d.%m.%y", '%m.%Y', '%Y, %d.%m.',
        "%Y,%d.%m.", "%Y,%m,%d", "%Y,%b.", "%Y,%b", "%Y,%B", "%Y,%d.%B", "%Y/%m",
        "%Y,%d.%b.", "%Y,%d.%B"
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


def get_values(record: Record, key: str) -> list[str]:
    # TODO: Adjust for different base urls
    # TODO: get id values 
    base = "https://d-nb.info/standards/elementset/gnd#"
    try: 
        values = [value['@value'] for value in record[base + key]]
        # convert dates to iso format
        if 'date' in key.lower():
            values = [convert_to_iso_date(elem) for elem in values]
        return values 
    except KeyError:
        #print('KeyError: ' + key + ' not found in ' )
        return []

# TODO: Move into general utils function and convert 
def convert_to_iso_date(date_str):
    date_str = date_str.replace('XX.', '')
    formats = [
        "%Y-%m-%d", "%Y-%m", "%Y", "%d.%m.%Y", "%d.%m.%y", '%m.%Y', '%Y, %d.%m.',
        "%Y,%d.%m.", "%Y,%m,%d", "%Y,%b.", "%Y,%b", "%Y,%B", "%Y,%d.%B", "%Y/%m",
        "%Y,%d.%b.", "%Y,%d.%B"
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


def extract_id(value: str, person_type: str = 'DifferentiatedPerson') -> str:
    # format of person ids is different for the person type
    if person_type != 'DifferentiatedPerson':
        return value.split('x')[-1]
    return value.split('/')[-1]


def get_type(record: Record) -> str:
    # TODO: Adjust for multiple corporate types
    record_type = ""
    if '@type' in record.keys():
        record_type = record['@type'][0].split('#')[-1]
    return record_type


def get_wikidata_url(record: Record) -> list[str]:
    key = 'http://www.w3.org/2002/07/owl#sameAs'
    if key in record.keys():
        return [item.get('@id') for item in record[key] if 'wikidata' in item.get('@id')]
    return []


def add_properties(proxy, record: Record, mapping: dict[str, str]):
    for gnd_key, ftm_key in mapping.items():
        proxy.add(ftm_key, get_values(record, gnd_key))
    proxy.add('wikidataId', get_wikidata_url(record))


def make_person(ctx: Context, record: Record) -> CE:
    proxy = ctx.make("Person")
    proxy.id = ctx.make_slug(extract_id(record["@id"], get_type(record)))
    add_properties(proxy, record, PERSON_MAPPING)
    return proxy


def make_legalentity(ctx: Context, record: Record) -> CE:
    proxy = ctx.make("LegalEntity")
    proxy.id = ctx.make_slug(extract_id(record['@id']))
    proxy.add('legalForm', record['@type'])
    add_properties(proxy, record, CORPORATE_MAPPING)
    return proxy


def make_company(ctx: Context, record: Record) -> CE:
    proxy = ctx.make("Company")
    proxy.id = ctx.make_slug(extract_id(record['@id']))
    add_properties(proxy, record, CORPORATE_MAPPING)
    return proxy


def handle(ctx: Context, record: Record, ix: int) -> CEGenerator:
    tx = ctx.task()

    # skip ids with '/about' as this is metadata instead of a new entity
    if 'about' not in record['@id']:

        if ctx.source.name == "legalentity":
            record_type = get_type(record)
            if record_type == 'Company':
                entity = make_company(tx, record)
            else:
                entity = make_legalentity(tx, record)
        elif ctx.source.name == "person":
                person_type = get_type(record)
                if person_type != 'Family':
                    entity = make_person(tx, record)
        yield entity 
