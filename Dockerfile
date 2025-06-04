FROM ghcr.io/dataresearchcenter/investigraph:develop

COPY ./datasets/EU/ec_meetings /datasets/ec_meetings
COPY ./datasets/EU/eu_transparency_register /datasets/eu_transparency_register

COPY ./datasets/GB/gb_ocod /datasets/
COPY ./datasets/GB/gb_ccod /datasets/
COPY ./datasets/GB/gb_pricepaid /datasets/

COPY Makefile /datasets/
COPY catalogs /datasets/
COPY build_catalog.py /datasets/
COPY setup.py /datasets/
COPY pyproject.toml /datasets/
COPY README.md /datasets/

RUN pip install /datasets

WORKDIR /datasets
ENTRYPOINT [ "" ]

ENV INVESTIGRAPH_ARCHIVE_URI=s3://memorious/investigraph
ENV ANYSTORE_URI=memory://
