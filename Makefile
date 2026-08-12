CATALOG_NAMES := opensanctions worldbank investigraph-eu reference-dach index
CATALOGS := $(CATALOG_NAMES:%=catalogs/%.json)
BUCKET := data.openaleph.org
LAKEHOUSE_URI := s3://$(BUCKET)
DATASETS_DIR ?= ./datasets
CONCURRENCY ?= 2

all: clean install $(CATALOGS) publish

catalogs: $(CATALOGS)

catalogs/%.json:
	python ./build_catalog.py -i catalogs/$*.yml -o $@

install:
	pip install .

clean:
	rm -rf catalogs/*.json

publish: catalogs
	aws --endpoint-url https://s3.investigativedata.org s3 sync --exclude "*" --include "*.json" catalogs s3://$(BUCKET)/catalogs/
	aws --endpoint-url https://s3.investigativedata.org s3 cp catalogs/index.json s3://$(BUCKET)/index.json

check_dataset:
	@test -n "$(dataset)" || (echo "dataset is required. Usage: make <target> dataset=<name>" && exit 1)

memorious: check_dataset
	memorious run $(DATASETS_DIR)/$(dataset)/config.yml -c $(CONCURRENCY)
	ftm-lakehouse -d $(dataset) make -c $(DATASETS_DIR)/$(dataset)/config.yml --full

investigraph: check_dataset
	investigraph run -c $(DATASETS_DIR)/$(dataset)/config.yml
	ftm-lakehouse -d $(dataset) make -c $(DATASETS_DIR)/$(dataset)/config.yml --full
