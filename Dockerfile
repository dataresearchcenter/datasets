FROM ghcr.io/dataresearchcenter/investigraph:0.7.0 AS base

# Stage 1: Install Python dependencies (rarely changes)
FROM base AS dependencies

USER 0

# Copy only files needed for pip install
COPY pyproject.toml setup.py README.md /datasets/
COPY util /datasets/util

# Create required structure and install
RUN mkdir -p /datasets/datasets && \
    touch /datasets/datasets/__init__.py && \
    pip install --no-cache-dir /datasets awscli && \
    chown -R 1000 /datasets

# Stage 2: Add build tools (changes occasionally)
FROM dependencies AS build-tools

COPY Makefile build_catalog.py /datasets/
COPY catalogs /datasets/catalogs

RUN chown -R 1000 /datasets

# Stage 3: Add datasets (changes frequently)
FROM build-tools AS final

# DE datasets
COPY --chown=1000:1000 ./datasets/DE/de_lobbyregister /datasets/de_lobbyregister

# EU datasets
COPY --chown=1000:1000 ./datasets/EU/ec_meetings /datasets/ec_meetings
COPY --chown=1000:1000 ./datasets/EU/eu_transparency_register /datasets/eu_transparency_register

# GB datasets
COPY --chown=1000:1000 ./datasets/GB/gb_ocod /datasets/gb_ocod
COPY --chown=1000:1000 ./datasets/GB/gb_ccod /datasets/gb_ccod
COPY --chown=1000:1000 ./datasets/GB/gb_pricepaid /datasets/gb_pricepaid

# US datasets
COPY --chown=1000:1000 ./datasets/US/us_cpr /datasets/us_cpr

# ZZ datasets (World)
COPY --chown=1000:1000 ./datasets/ZZ/worldbank_ifc_advisory_services /datasets/worldbank_ifc_advisory_services
COPY --chown=1000:1000 ./datasets/ZZ/worldbank_ifc_investment_services /datasets/worldbank_ifc_investment_services
COPY --chown=1000:1000 ./datasets/ZZ/worldbank_procurement_awards /datasets/worldbank_procurement_awards
COPY --chown=1000:1000 ./datasets/ZZ/worldbank_project_procurement /datasets/worldbank_project_procurement
COPY --chown=1000:1000 ./datasets/ZZ/worldbank_procurement_notices /datasets/worldbank_procurement_notices

USER 1000
WORKDIR /datasets
ENTRYPOINT [ "" ]

ENV INVESTIGRAPH_ARCHIVE_URI=s3://memorious/investigraph
ENV ANYSTORE_URI=memory://
