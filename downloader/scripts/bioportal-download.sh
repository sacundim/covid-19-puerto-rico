#!/usr/bin/env bash
#
# Script to download from Bioportal
#

set -eu -o pipefail

# Official URL:
#ENDPOINT="${ENDPOINT-https://bioportal-apim.salud.pr.gov/bioportal}"

# ...but that one's slow, so we're using this one:
ENDPOINT="${ENDPOINT-https://api-bioportal-prod-eastus2-01.azurewebsites.net}"


# Bash associative array; needs Bash v4+. MacOS comes with v3;
# you will need to `brew install bash`.
declare -r -A DATASETS=(
  ["deaths"]="administration/reports/deaths/summary"
  ["minimal-info-unique-tests"]="administration/reports/minimal-info-unique-tests"
  ["orders-basic"]="administration/reports/orders/basic"
)


HERE="$(dirname $0)"
REPO_ROOT="${HERE}"/..
TMP="${REPO_ROOT}"/tmp
S3_SYNC_DIR="${REPO_ROOT}"/s3-bucket-sync/covid-19-puerto-rico-data
BIOPORTAL_SYNC_DIR="${S3_SYNC_DIR}/bioportal"

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

echo "$(date): Moving outputs to ${BIOPORTAL_SYNC_DIR}..."
for dataset in "${!DATASETS[@]}"
do
  basename="${dataset}_${timestamp}"
  dataset_dir="${BIOPORTAL_SYNC_DIR}/${dataset}"
  mkdir -p \
    "${dataset_dir}/json_v4" \
    "${dataset_dir}/parquet_v5/downloaded_date=${downloaded_date}"
  mv "${basename}.json.bz2" "${dataset_dir}/json_v4/"
  mv "${basename}.parquet" "${dataset_dir}/parquet_v5/downloaded_date=${downloaded_date}/"
done
echo "$(date): Moved outputs to ${dataset_dir}."

echo "$(date): File sizes:"
cd "${BIOPORTAL_SYNC_DIR}"
find . -type f -print0 \
  |xargs -0 du -h