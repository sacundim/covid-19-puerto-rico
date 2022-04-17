#!/usr/bin/env bash
#
# If we don't get a Covid19Datos CSV set from one date, copy
# over the Parquets from the previous day to the target

set -eu -o pipefail

S3_PREFIX="${S3_PREFIX-s3://covid-19-puerto-rico-data/covid19datos-v2}"
DATASETS=(casos defunciones sistemas_salud vigilancia vacunacion pruebas)

# This is the timestamp in the filenames that we copy over,
# e.g., `2022-04-15T16:26:31Z`
SOURCE_TIMESTAMP="${1:?"No source timestamp given"}"
source_date="${SOURCE_TIMESTAMP:0:10}"

# The timestamp we copy to as target.  If none given defaults to now.
TARGET_TIMESTAMP="${2-$(date -u +"%Y-%m-%dT%H:%M:%SZ")}"
target_date="${TARGET_TIMESTAMP:0:10}"

echo "$(date): SOURCE_TIMESTAMP=${SOURCE_TIMESTAMP}"
echo "$(date): TARGET_TIMESTAMP=${TARGET_TIMESTAMP}"

for dataset in "${DATASETS[@]}"
do
  source_url="${S3_PREFIX}/${dataset}/parquet_v1/downloaded_date=${source_date}/${dataset}_${SOURCE_TIMESTAMP}.parquet"
  target_url="${S3_PREFIX}/${dataset}/parquet_v1/downloaded_date=${target_date}/${dataset}_${TARGET_TIMESTAMP}.parquet"
  echo "$(date): Copying ${source_url} to ${target_url}"
  aws s3 cp "${source_url}" "${target_url}"
done