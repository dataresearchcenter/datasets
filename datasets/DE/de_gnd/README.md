# de_gnd

This scraper creates `Person`, `Company`, `LegalEntity` entities from the Integrated Authority File (GND) of the German National Library.

## run the whole thing

Set up a postgres database or kvrocks store to write statements to in parallel.

    export REDIS_URL=redis://localhost:6379/0
    export FTMQ_STORE_URI=postgresql///ftm  # redis://localhost fpr kvrocks

    investigraph extract | parallel --pipe -j8 -N10000 --roundrobin investigraph transform

This will emit the entity fragments directly into the postgres database and avoids in-memory aggregation.

After the whole process, export entities to a file:

    ftmq -i $FTMQ_STORE_URI -d de_gnd > ./data/de_gnd/entities.ftm.json

## testing

use env var `GND_TEST_SIZE=1000` to only process a subset of data.
