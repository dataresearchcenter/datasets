# Generic OCDS to FollowTheMoney Transformer

A generic, reusable transformer that converts OCDS 1.1.5 (Open Contracting Data Standard) data to FollowTheMoney entities for use with investigraph datasets.

## Features

- **Generic and reusable** - Works with any OCDS 1.1.5 dataset
- **Comprehensive entity mapping** - Creates organizations, addresses, identifiers, tenders, contracts, and awards
- **Relationship tracking** - Links buyers, suppliers, contracts, and awards
- **Contact management** - Creates contact person entities with Representation relationships
- **Schema selection** - Automatically uses PublicBody for buyers, LegalEntity for suppliers
- **Investigraph integration** - Uses investigraph context for ID generation and entity management

## Installation

This module is part of the `datasets` project:

```bash
poetry install
```

## Usage

### With investigraph

The transformer is designed to work as an investigraph transform handler:

```python
from common.ocds.transform import handle

# In your dataset's transform.py:
from common.ocds.transform import handle

# Or use it directly:
# (investigraph will call it with ctx, record, ix)
```

### In a dataset config.yml

```yaml
name: my_ocds_dataset
prefix: my-prefix

extract:
  handler: ./extract.py:handle

transform:
  handler: common.ocds.transform:handle
```

### Direct usage

```python
from investigraph.model import SourceContext
from common.ocds.transform import handle

# Mock or real investigraph context
ctx = SourceContext(...)

# OCDS release (dict)
ocds_release = {
    "ocid": "ocds-213czf-...",
    "parties": [...],
    "buyer": {...},
    "tender": {...},
    "awards": [...],
    "contracts": [...]
}

# Transform to FollowTheMoney entities
for entity in handle(ctx, ocds_release, 0):
    print(f"{entity.schema.name}: {entity.id}")
    # entity is a FollowTheMoney StatementEntity
```

## OCDS to FollowTheMoney Mapping

### Organizations (parties)

OCDS parties are mapped to FollowTheMoney entities based on their roles:

| OCDS Role | FTM Schema | Properties |
|-----------|------------|------------|
| buyer, procuringEntity | PublicBody | name, country, website, email, phone |
| supplier, tenderer | LegalEntity | name, country, website, email, phone |
| other | LegalEntity | name, country, website, email, phone |

**Additional entities created:**
- **Address** - Full address entity linked via `addressEntity`
- **Identification** - Organization registration numbers/IDs
- **LegalEntity** (contact) - Named contact persons
- **Representation** - Links contact person to organization

### Tender → CallForTenders

| OCDS Field | FTM Property |
|------------|--------------|
| tender.title | title |
| tender.description | description |
| tender.submissionMethodDetails | sourceUrl |
| tender.tenderPeriod.startDate | publicationDate |
| tender.tenderPeriod.endDate | submissionDeadline |
| tender.items[].classification (CPV) | cpvCode |
| buyer | authority |

### Contract → Contract

| OCDS Field | FTM Property |
|------------|--------------|
| contract.id | id (prefixed) |
| contract.title | title |
| contract.description | description |
| contract.dateSigned | contractDate |
| contract.status | status |
| contract.value.amount | amount |
| contract.value.currency | currency |
| buyer | authority |

**Note:** Contract schema does not have `callForTenders` or period properties. These are on ContractAward.

### Award → ContractAward

| OCDS Field | FTM Property |
|------------|--------------|
| award.id | id (prefixed) |
| award.title, award.description | role, summary |
| award.date | date |
| award.status | status |
| award.value.amount | amount |
| award.value.currency | currency |
| award.contractPeriod | startDate, endDate |
| award.items[].classification (CPV) | cpvCode |
| contract | contract (linked) |
| callForTenders | callForTenders (linked) |
| award.suppliers | supplier (linked) |

## Entity ID Generation

All IDs are generated using investigraph context methods:

```python
# Organization IDs from OCDS party.id
org.id = ctx.make_slug('party', party_id)

# Identification IDs
identification.id = ctx.make_slug('id', country, scheme, id_value)

# Address IDs (deterministic from content)
address_entity = make_address(ctx, ...)  # investigraph helper

# Contact person IDs
contact.id = ctx.make_id('contact', email)  # or phone, or org.id + name

# Tender IDs
cft.id = ctx.make_slug('tender', ocid)

# Contract IDs
contract.id = ctx.make_slug('contract', ocid, contract_id)

# Award IDs
award.id = ctx.make_slug('award', ocid, award_id)
```

This ensures:
- Stable, reproducible IDs across runs
- Prefix from dataset config is applied
- No ID collisions between entity types

## Supported OCDS Properties

### Organization Identifiers

Maps OCDS identifier schemes to FTM properties:

| OCDS Scheme | FTM Property |
|-------------|--------------|
| TRADE_REGISTER | registrationNumber |
| National-ID | registrationNumber |
| TAX_ID | vatCode |
| AU-ABN | registrationNumber |
| Other | registrationNumber |

### Classification Schemes

- **CPV** - Common Procurement Vocabulary codes mapped to `cpvCode`
- Other classification schemes are currently not mapped

## Limitations

- Contract schema doesn't support `callForTenders` or period properties
- Period information (startDate/endDate) only on ContractAward
- Items/lots details are not fully mapped (only CPV codes)
- Document attachments are not mapped
- Milestones and implementation details are not mapped

## Example Output

From one OCDS release with 2 parties, 1 tender, 2 awards, 2 contracts:

```
PublicBody: eu-ted-party-abc123...
Address: eu-ted-addr-xyz789...
Identification: eu-ted-id-pl-national-id-123456
LegalEntity: eu-ted-contact-email@example.com
Representation: eu-ted-contact-abc123-contact-email
LegalEntity: eu-ted-party-def456...
Address: eu-ted-addr-uvw456...
CallForTenders: eu-ted-tender-ocds-213czf-...
Contract: eu-ted-contract-ocds-213czf-aaa111
Contract: eu-ted-contract-ocds-213czf-bbb222
ContractAward: eu-ted-award-ocds-213czf-award1
ContractAward: eu-ted-award-ocds-213czf-award2
```

## Used by Datasets

- `datasets/EU/eu_ted/` - TED (Tenders Electronic Daily) procurement data

## See Also

- `util/ocds/eu_ted/` - TED-specific OCDS extraction
- [OCDS 1.1.5 Schema](https://standard.open-contracting.org/schema/1__1__5/)
- [FollowTheMoney Schema](https://followthemoney.tech/explorer/schemata/)
- [investigraph Documentation](https://investigraph.dev/)
