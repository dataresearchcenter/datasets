FROM ghcr.io/dataresearchcenter/investigraph:develop

COPY ./datasets/EU/ec_meetings /datasets/ec_meetings
COPY ./datasets/EU/eu_transparency_register /datasets/eu_transparency_register

COPY ./datasets/GB/gb_ocod /datasets/gb_ocod
COPY ./datasets/GB/gb_ccod /datasets/gb_ccod
COPY ./datasets/GB/gb_pricepaid /datasets/gb_pricepaid

COPY Makefile /Makefile
COPY catalogs /catalogs

ENV INVESTIGRAPH_ARCHIVE_URI s3://memorious/investigraph
