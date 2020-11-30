#!/usr/bin/env bash

set -e
set -o pipefail

TESTS_ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests"
ORDERS_ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/orders/basic"
timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
tests_basename="minimal-info-unique-tests_V1_${timestamp}"
orders_basename="orders-basic_${timestamp}"

# Set environment variable externally to override this default
S3_DATA_URL="${S3_DATA_URL-s3://covid-19-puerto-rico-data}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."

TMP="${REPO_ROOT}/tmp"
TESTS_JSON_TMP="${TMP}/${tests_basename}.json.bz2"
TESTS_CSV_TMP="${TMP}/${tests_basename}.csv.bz2"
ORDERS_JSON_TMP="${TMP}/${orders_basename}.json.bz2"
ORDERS_CSV_TMP="${TMP}/${orders_basename}.csv.bz2"
ORDERS_PARQUET_TMP="${TMP}/${orders_basename}.parquet"

S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"
BIOPORTAL_SYNC_DIR="${S3_SYNC_DIR}/bioportal"
TESTS_JSON="${BIOPORTAL_SYNC_DIR}/tests/json_v2/${tests_basename}.json.bz2"
TESTS_CSV="${BIOPORTAL_SYNC_DIR}/tests/csv_v2/${tests_basename}.csv.bz2"
ORDERS_JSON="${BIOPORTAL_SYNC_DIR}/orders-basic/json_v1/${orders_basename}.json.bz2"
ORDERS_CSV="${BIOPORTAL_SYNC_DIR}/orders-basic/csv_v1/${orders_basename}.csv.bz2"
ORDERS_PARQUET="${BIOPORTAL_SYNC_DIR}/orders-basic/parquet_v1/${orders_basename}.parquet"


echo "$(date): Fetching from tests endpoint..."
time wget -O - "${TESTS_ENDPOINT}" \
  | bzip2 -9 \
  > "${TESTS_JSON_TMP}"
echo "$(date): Downloaded to ${TESTS_JSON_TMP}"

echo "$(date): Fetching from orders/basic endpoint..."
time wget -O - "${ORDERS_ENDPOINT}" \
  | bzip2 -9 \
  > "${ORDERS_JSON_TMP}"
echo "$(date): Downloaded to ${ORDERS_JSON_TMP}"


echo "$(date): Converting tests to csv..."
time "${HERE}"/bioportal-tests-to-csv.sh "${timestamp}" "${TESTS_JSON_TMP}" \
    | bzip2 -9 \
    > "${TESTS_CSV_TMP}"
echo "$(date): Wrote output to ${TESTS_CSV_TMP}"

echo "$(date): Converting orders-basic to csv..."
time "${HERE}"/bioportal-basic-to-csv.sh "${timestamp}" "${ORDERS_JSON_TMP}" \
    | bzip2 -9 \
    > "${ORDERS_CSV_TMP}"
echo "$(date): Wrote output to ${ORDERS_CSV_TMP}"

echo "$(date): Converting orders-basic to parquet..."
# TRICKY: One is tempted to use the timestamp data type option
# in the `csv2parquet` tool, but Athena can't read the timestamps
# it produces, because Parquet is a horrible mess.
bzcat "${ORDERS_CSV_TMP}" \
  | time csv2parquet --codec gzip --row-group-size 10000000 \
      --output /dev/stdout \
      /dev/stdin \
  > "${ORDERS_PARQUET_TMP}"
echo "$(date): Wrote output to ${ORDERS_PARQUET_TMP}"


echo "$(date): File sizes:"
du -h "${TESTS_JSON_TMP}" "${ORDERS_JSON_TMP}" \
  "${TESTS_CSV_TMP}" "${ORDERS_CSV_TMP}" \
  "${ORDERS_PARQUET_TMP}"

echo "$(date): Moving files to the sync directory"
mv "${TESTS_JSON_TMP}" "${TESTS_JSON}"
mv "${TESTS_CSV_TMP}" "${TESTS_CSV}"
mv "${ORDERS_JSON_TMP}" "${ORDERS_JSON}"
mv "${ORDERS_CSV_TMP}" "${ORDERS_CSV}"
mv "${ORDERS_PARQUET_TMP}" "${ORDERS_PARQUET}"
