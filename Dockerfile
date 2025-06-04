FROM ghcr.io/dataresearchcenter/investigraph:develop

COPY ./datasets/GB/gb_ocod /datasets/gb_ocod
COPY ./datasets/GB/gb_ccod /datasets/gb_ccod
COPY ./datasets/GB/gb_pricepaid /datasets/gb_pricepaid

COPY Makefile /Makefile
COPY catalogs /catalogs

ENV INVESTIGRAPH_ARCHIVE_URI s3://memorious/investigraph
