# Stage 1: Install Python dependencies (rarely changes)
FROM ghcr.io/dataresearchcenter/investigraph:0.7.1 AS base

USER root

# Copy only files needed for pip install
COPY pyproject.toml setup.py README.md /datasets/
COPY common /datasets/common

# Create required structure and install
RUN mkdir -p /datasets/datasets && \
    touch /datasets/datasets/__init__.py && \
    pip install --no-cache-dir /datasets awscli && \
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

USER 1000
WORKDIR /datasets
ENTRYPOINT [ "" ]

ENV INVESTIGRAPH_ARCHIVE_URI=s3://memorious/investigraph
ENV ANYSTORE_URI=memory://
ENV INVESTIGRAPH_HTTP_TIMEOUT=3600
