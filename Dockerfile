# Stage 1: Install Python dependencies (rarely changes)
FROM ghcr.io/dataresearchcenter/investigraph:main AS base

USER root

# Copy only files needed for pip install
COPY pyproject.toml setup.py README.md Makefile requirements.txt /datasets/
COPY common /datasets/common

# Create required structure and install
RUN mkdir -p /datasets/datasets && \
    pip uninstall -y -q investigraph && \  # force re-install with most recent version
    pip install -q --no-cache-dir --no-deps -r /datasets/requirements.txt && \
    pip install -q --no-cache-dir psycopg-binary==3.3.2 && \
    pip install -q --no-cache-dir --no-deps /datasets && \
    chown -R 1000 /datasets

# Stage 2: Add build tools (changes occasionally)
FROM base AS build-tools

COPY Makefile build_catalog.py /datasets/
COPY catalogs /datasets/catalogs

RUN chown -R 1000 /datasets

# Stage 3: Add datasets (changes frequently)
FROM build-tools AS final

# Copy all datasets preserving directory structure, then flatten
COPY --chown=1000:1000 ./datasets /datasets/_src

# Flatten dataset directories: /datasets/_src/XX/dataset_name -> /datasets/dataset_name
RUN for d in /datasets/_src/*/*; do \
        if [ -d "$d" ]; then \
            name=$(basename "$d"); \
            mv "$d" "/datasets/$name"; \
        fi; \
    done && \
    rm -rf /datasets/_src

RUN mkdir -p /home/1000/.duckdb && chown -R 1000 /home/1000

USER 1000
WORKDIR /datasets
ENTRYPOINT [ "/bin/bash", "-c" ]

ENV HOME=/home/1000

ENV AWS_REGION=eu-central-1
ENV MEMORIOUS_HTTP_TIMEOUT=3600
ENV MEMORIOUS_MAX_RUNTIME=18000
ENV LAKEHOUSE_URI=https://data.openaleph.org
ENV LAKEHOUSE_PUBLIC_URL_PREFIX="https://data.openaleph.org/{{ dataset }}"
