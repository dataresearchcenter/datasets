FROM ghcr.io/investigativedata/investigraph:develop

COPY ./datasets/catalog.yml /datasets/catalog.yml
COPY ./datasets/GB/gb_ocod /datasets/gb_ocod
