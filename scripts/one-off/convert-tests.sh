#!/usr/bin/env bash

#
# One-off script to convert old minimal-info-unique-tests_V* csv files to Parquet.
# Uses this utility:
#
# * https://github.com/cldellow/csv2parquet
#

set -e
set -o pipefail

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/../.."
S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"
TESTS_DIR="${S3_SYNC_DIR}/bioportal/tests"

mkdir -p "${TESTS_DIR}"/parquet_v1

cd "${TESTS_DIR}/csv_v1"
for file in minimal-info-unique-tests_*.csv.bz2
do
  echo "$(date): converting ${file}..."
  bzcat "${file}" \
    | time csv2parquet \
        --exclude patientid \
        --codec gzip \
        --row-group-size 10000000 \
        --output /dev/stdout \
        /dev/stdin \
    > ../parquet_v1/"${file%.csv.bz2}".parquet
done

cd ../csv_v2
for file in minimal-info-unique-tests_*.csv.bz2
do
  echo "$(date): converting ${file}..."
  bzcat "${file}" \
    | time csv2parquet \
        --exclude patientid \
        --codec gzip \
        --row-group-size 10000000 \
        --output /dev/stdout \
        /dev/stdin \
    > ../parquet_v1/"${file%.csv.bz2}".parquet
done
