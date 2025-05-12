FROM ghcr.io/dataresearchcenter/investigraph:develop

COPY ./datasets/catalog.yml /datasets/catalog.yml
COPY ./datasets/GB/gb_ocod /datasets/gb_ocod
