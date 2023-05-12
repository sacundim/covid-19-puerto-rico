#!/usr/bin/env bash
#
# Script to download from the V2 of covid19datos.salud.gov.pr
#

set -eu -o pipefail

ENDPOINT="${ENDPOINT-https://covid19datos.salud.gov.pr/estadisticas_v2/download/data}"
DATASETS=(casos defunciones sistemas_salud vigilancia vacunacion pruebas)

HERE="$(dirname $0)"
REPO_ROOT="${HERE}"/..
TMP="${REPO_ROOT}"/tmp
S3_SYNC_DIR="${REPO_ROOT}"/s3-bucket-sync/covid-19-puerto-rico-data
COVID19DATOS_V2_DIR="${S3_SYNC_DIR}"/covid19datos-v2

timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
downloaded_date="${timestamp:0:10}"

echo "$(date): $(csv2parquet --version)"

cd "${TMP}"
for dataset in "${DATASETS[@]}"
do
  url="${ENDPOINT}/${dataset}/completo"
  basename="${dataset}_${timestamp}"

  echo "$(date): Downloading ${dataset} from ${url}"
  wget \
    --tries=3 \
    --compression gzip \
    --no-verbose \
    -O "${basename}".csv \
    "${url}"
done


echo "$(date): Converting downloads to Parquet..."
ls *.csv \
  | parallel --line-buffer "${HERE}"/csv-to-parquet.sh


echo "$(date): Organizing files for S3 upload..."
for dataset in "${DATASETS[@]}"
do
  basename="${dataset}_${timestamp}"
  dataset_dir="${COVID19DATOS_V2_DIR}/${dataset}"
  echo "$(date): Moving to ${dataset_dir}"
  mkdir -p \
    "${dataset_dir}/csv_v1/downloaded_date=${downloaded_date}" \
    "${dataset_dir}/parquet_v2/downloaded_date=${downloaded_date}"
  mv "${basename}.csv.bz2" "${dataset_dir}/csv_v3/downloaded_date=${downloaded_date}/"
  mv "${basename}.parquet" "${dataset_dir}/parquet_v3/downloaded_date=${downloaded_date}/"
done

echo "$(date): File sizes:"
cd "${COVID19DATOS_V2_DIR}"
find . -type f -print0 \
  |xargs -0 du -h