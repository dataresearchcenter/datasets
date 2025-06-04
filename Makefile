CATALOG_NAMES := opensanctions reference-dach index
CATALOGS := $(CATALOG_NAMES:%=catalogs/%.json)

all: clean install $(CATALOGS) publish

catalogs: $(CATALOGS)

catalogs/%.json:
	python ./build_catalog.py -i catalogs/$*.yml -o $@

install:
	pip install .

clean:
	rm -rf catalogs/*.json

publish: catalogs
	aws --endpoint-url https://s3.investigativedata.org s3 sync --exclude "*" --include "*.json" catalogs s3://data.ftm.store/catalogs/
	aws --endpoint-url https://s3.investigativedata.org s3 cp catalogs/index.json s3://data.ftm.store/index.json
