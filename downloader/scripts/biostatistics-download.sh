#!/usr/bin/env bash
#
# Script to download from PRDoH's Biostatistics API:
#
# * https://biostatistics.salud.pr.gov/swagger/index.html
#

set -eu -o pipefail

ENDPOINT="${ENDPOINT-https://biostatistics.salud.pr.gov}"

# Bash associative array; needs Bash v4+. MacOS comes with v3;
# you will need to `brew install bash`.
declare -r -A DATASETS=(
  ["data-sources"]="data-sources"
  ["cases"]="cases/covid-19/minimal"
  ["deaths"]="deaths/covid-19/minimal"
  ["tests"]="orders/tests/covid-19/minimal"
  ["tests-grouped"]="orders/tests/covid-19/grouped-by-sample-collected-date-and-entity"
  ["persons-with-vaccination-status"]="vaccines/covid-19/persons-with-vaccination-status"
)

HERE="$(dirname $0)"
REPO_ROOT="${HERE}"/..
TMP="${REPO_ROOT}"/tmp
S3_SYNC_DIR="${REPO_ROOT}"/s3-bucket-sync/covid-19-puerto-rico-data
BIOSTATISTICS_SYNC_DIR="${S3_SYNC_DIR}/biostatistics.salud.pr.gov"

timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
downloaded_date="${timestamp:0:10}"

echo "$(date): $(json2parquet --version)"


cd "${TMP}"
for dataset in "${!DATASETS[@]}"
do
  url="${ENDPOINT}/${DATASETS[$dataset]}"
  basename="${dataset}_${timestamp}"

  echo "$(date): Downloading ${dataset} from ${url}"
  wget \
    --tries=3 \
    --no-verbose \
    --compression gzip \
    -O "${basename}".json \
    "${url}"
  echo "$(date): Downloaded ${dataset} to ${basename}.json"
done


echo "$(date): Converting downloads to Parquet..."
ls *.json \
  | parallel --line-buffer "${HERE}"/json-array-to-parquet.sh
echo "$(date): Converted downloads to Parquet."


echo "$(date): Moving outputs to ${BIOSTATISTICS_SYNC_DIR}..."
for dataset in "${!DATASETS[@]}"
do
  basename="${dataset}_${timestamp}"
  dataset_dir="${BIOSTATISTICS_SYNC_DIR}/${dataset}"
  mkdir -p \
    "${dataset_dir}/json_v1" \
    "${dataset_dir}/parquet_v1/downloaded_date=${downloaded_date}"
  mv "${basename}.json.bz2" "${dataset_dir}/json_v1/"
  mv "${basename}.parquet" "${dataset_dir}/parquet_v1/downloaded_date=${downloaded_date}/"
done
echo "$(date): Moved outputs to ${dataset_dir}."


echo "$(date): File sizes:"
cd "${BIOSTATISTICS_SYNC_DIR}"
find . -type f -print0 \
  |xargs -0 du -h