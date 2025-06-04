all: clean process publish

data/fragments.json: parse.py
	python parse.py

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/export/entities.ftm.json: data/sorted.json
	mkdir -p data/export
	nk sorted-aggregate -i data/sorted.json -o data/export/entities.ftm.json

publish:
	bash ../../upload.sh de_abgeordnetenwatch data/export

process: data/export/entities.ftm.json

clean:
	rm -rf data/
