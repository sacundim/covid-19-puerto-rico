#!/usr/bin/env bash
#
# One-off script to convert old minimal-info csv files to Parquet.
# Uses this utility:
#
# * https://github.com/cldellow/csv2parquet
#

set -e
set -o pipefail

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/../.."
S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"
CASES_DIR="${S3_SYNC_DIR}/bioportal/cases"

mkdir -p "${CASES_DIR}"/parquet_v1

cd "${CASES_DIR}/csv_v1"
for file in minimal-info_*.csv.bz2
do
  echo "$(date): converting ${file}..."
  bzcat "${file}" \
    | time csv2parquet \
        --codec gzip \
        --row-group-size 10000000 \
        --output /dev/stdout \
        /dev/stdin \
    > ../parquet_v1/"${file%.csv.bz2}".parquet
done

