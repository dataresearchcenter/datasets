from investigraph.types import CE, CEGenerator, Record
from investigraph.model import Context


PERSON_MAPPING = {
    'preferredNameEntityForThePerson': 'name',
    'variantNameForThePerson': 'alias',          # TODO: process name format
    'forename': 'firstName',
    'surname': 'lastName',
    'dateOfBirth': 'birthDate',
    'dateOfDeath': 'deathDate'
    }

CORPORATE_MAPPING = {
    'preferredNameForTheCorporateBody': 'name',
    'variantNameForTheCorporateBody': 'alias',
    'geographicAreaCode': 'country',
    'spatialAreaOfActivity': 'country',
    'placeOfBusiness': 'country',
    'dateOfEstablishment': 'incorporationDate',
    'homepage': 'website',
}
# TODO: add wikidataId as property

def get_values(record: Record, key: str) -> list[str]:
    # TODO: Adjust for different base urls
    # TODO: get id values 
    base = "https://d-nb.info/standards/elementset/gnd#"
    try: 
        nested_values = record[base + key]
        return [value['@value'] for value in nested_values]
    except KeyError:
        #print('KeyError: ' + key + ' not found in ' )
        return []


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


def add_properties(proxy, record: Record, mapping: dict[str, str]):
    for gnd_key, ftm_key in mapping.items():
        proxy.add(ftm_key, get_values(record, gnd_key))


def make_person(ctx: Context, record: Record) -> CE:
    proxy = ctx.make("Person")
    proxy.id = ctx.make_slug(extract_id(record["@id"], get_type(record)))
    add_properties(proxy, record, PERSON_MAPPING)
    return proxy


def make_legalentity(ctx: Context, record: Record) -> CE:
    proxy = ctx.make("LegalEntity")
    proxy.id = ctx.make_slug(extract_id(record['@id']))
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
                entity = make_person(tx, record)
        yield entity 
