#!/usr/bin/env bash
#
# Script to download from the V2 of covid19datos.salud.gov.pr
#

set -eu -o pipefail

ENDPOINT="https://covid19datos.salud.gov.pr/estadisticas_v2/download/data"
DATASETS=(casos defunciones sistemas_salud vacunacion pruebas)

HERE="$(dirname $0)"
REPO_ROOT="${HERE}"/..
TMP="${REPO_ROOT}"/tmp
S3_SYNC_DIR="${REPO_ROOT}"/s3-bucket-sync/covid-19-puerto-rico-data
COVID19DATOS_V2_DIR="${S3_SYNC_DIR}"/covid19datos-v2

cd "${TMP}"
for dataset in "${DATASETS[@]}"
do
  url="${ENDPOINT}/${dataset}/completo"

  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  downloaded_date="${timestamp:0:10}"
  ts_seconds="$(date -u +"%s")"
  # PyArrow tends to barf on filenames with colons (thinks they're URLs),
  # so we have to generate a weird filename and rename it.
  tempname="${dataset}_${ts_seconds}"

  echo "$(date): Downloading ${dataset} from ${url}"
  wget --compression gzip --no-verbose -O "${tempname}".csv "${url}"

  echo "$(date): Converting to Parquet"
  csv2parquet --codec gzip --row-group-size 10000000 "${tempname}".csv

  basename="${dataset}_${timestamp}"
  mv "${tempname}".csv "${basename}".csv
  mv "${tempname}".parquet "${basename}".parquet

  echo "$(date): Compressing csv"
  bzip2 -9 "${basename}".csv

#  echo "$(date): File sizes:"
#  du -h "${basename}".csv.bz2 "${basename}".parquet

  dataset_dir="${COVID19DATOS_V2_DIR}/${dataset}"
  echo "$(date): Moving to ${dataset_dir}"
  mkdir -p \
    "${dataset_dir}"/csv_v1/downloaded_date="$downloaded_date" \
    "${dataset_dir}"/parquet_v1/downloaded_date="$downloaded_date"
  mv "${basename}".csv.bz2 "${dataset_dir}"/csv_v1/downloaded_date="$downloaded_date"/
  mv "${basename}".parquet "${dataset_dir}"/parquet_v1/downloaded_date="$downloaded_date"/
done

echo "$(date): File sizes:"
cd "${COVID19DATOS_V2_DIR}"
find . -type f -print0 \
  |xargs -0 du -h