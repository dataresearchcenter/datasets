FROM ghcr.io/dataresearchcenter/investigraph:develop

COPY ./datasets/EU/ec_meetings /datasets/ec_meetings
COPY ./datasets/EU/eu_transparency_register /datasets/eu_transparency_register

COPY ./datasets/GB/gb_ocod /datasets/gb_ocod
COPY ./datasets/GB/gb_ccod /datasets/gb_ccod
COPY ./datasets/GB/gb_pricepaid /datasets/gb_pricepaid

COPY ./datasets/US/us_cpr /datasets/us_cpr

COPY Makefile /datasets/
COPY catalogs /datasets/catalogs
COPY build_catalog.py /datasets/
COPY setup.py /datasets/
COPY pyproject.toml /datasets/
COPY README.md /datasets/

USER 0
RUN mkdir /datasets/datasets
RUN touch /datasets/datasets/__init__.py
RUN chown -R 1000 /datasets
RUN pip install /datasets
RUN pip install awscli
USER 1000

WORKDIR /datasets
ENTRYPOINT [ "" ]

ENV INVESTIGRAPH_ARCHIVE_URI=s3://memorious/investigraph
ENV ANYSTORE_URI=memory://
