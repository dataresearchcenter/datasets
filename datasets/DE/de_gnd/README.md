# de_gnd

This scraper creates `Person`, `Company`, `LegalEntity` entities from the Integrated Authority File (GND) of the German National Library.

## run the whole thing

Set up a postgres database and tell prefect.io to use it. Also, start a redis server and tell investigraph to use it:

    export REDIS_URL=redis://localhost:6379/0
    export PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg:///investigraph
    export FTM_STORE_URI=postgresql///investigraph

    investigraph run -c de_gnd/config.yml --chunk-size 10000 --no-aggregate --fragments-uri postgresql:///ftm

This will emit the entity fragments directly into the postgres database and avoids in-memory aggregation.

After the whole process, export the `ftm store` to a file:

    ftm store iterate -d de_gnd > ./data/de_gnd/entities.ftm.json

## testing

use env var `GND_TEST_SIZE=1000` to only process a subset of data.