#!/usr/bin/env bash
#
# Script to fetch some Parquet input files for local testing
#
set -euxo pipefail

DATA_LAKE=':s3,provider=AWS,env_auth:covid-19-puerto-rico-data'
BIOSTATISTICS_DIR='biostatistics.salud.pr.gov'
DOWNLOADED_DATE='2023-08-03'

cd "$(dirname $0)"
mkdir -p input_files

exec rclone copy \
  --fast-list \
  --checksum \
  --progress \
  --include "downloaded_date=${DOWNLOADED_DATE}/*.parquet" \
  "${DATA_LAKE}/${BIOSTATISTICS_DIR}" \
  input_files/"${BIOSTATISTICS_DIR}"