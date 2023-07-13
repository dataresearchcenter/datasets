#!/bin/bash

# args:
# 1 folder to sync
# 2 dataset name
#
# example:
#   bash ../../scripts/upload.sh icij_offshoreleaks data/export

aws s3 --endpoint-url https://minio.ninja sync --no-progress $2 s3://data.ftm.store/$1
