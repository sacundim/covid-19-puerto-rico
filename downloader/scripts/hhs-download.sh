#!/usr/bin/env bash
#
# Download CSVs from HHS/Socrata API servers, convert them all
# to Parquet, and organize for S3 datalake upload.
#
set -eu -o pipefail

HERE="$(dirname $0)"
REPO_ROOT="${HERE}"/..
TMP="${REPO_ROOT}"/tmp
S3_SYNC_DIR="${REPO_ROOT}"/s3-bucket-sync/covid-19-puerto-rico-data
HHS_SYNC_DIR="${S3_SYNC_DIR}/HHS"


# We use a Python script to interact with the API to download the CSVs
echo "$(date): Downloading datasets from HHS & CDC Socrata API servers..."
SOCRATA_APP_TOKEN="${SOCRATA_APP_TOKEN?"SOCRATA_APP_TOKEN not set"}"
hhs-socrata-download \
  --socrata-app-token-env-var SOCRATA_APP_TOKEN
echo "$(date): Downloaded datasets from HHS & CDC Socrata API servers."


echo "$(date): Converting datasets to Parquet..."
ls *.csv \
  | parallel --line-buffer "${HERE}"/csv-to-parquet.sh
echo "$(date): Converted datasets to Parquet."


echo "$(date): Moving outputs to ${HHS_SYNC_DIR}..."
datasets="$(
# TRICKY: Note the reverse sort here. This is because later we use these
# dataset names with a `*` wildcard, and reverse sort guarantees that
# if X is a prefix of XY then we visit XY first.
ls *.csv.bz2 \
  | sed 's/_[0-9]\{8\}_[0-9]\{4\}\.csv\.bz2$//' \
  | sort --reverse \
  | uniq
)"
for dataset in ${datasets}
do
  echo "$(date): Moving ${dataset} to ${HHS_SYNC_DIR}..."
  dataset_dir="${HHS_SYNC_DIR}/${dataset}"
  mkdir -p \
    "${dataset_dir}" \
    "${dataset_dir}/v3" \
    "${dataset_dir}/v3/csv" \
    "${dataset_dir}/v3/parquet"
  mv "${dataset}"_*.csv.bz2 "${dataset_dir}/v3/csv/"
  mv "${dataset}"_*.parquet "${dataset_dir}/v3/parquet/"
  echo "$(date): Moved ${dataset} to ${HHS_SYNC_DIR}."
done