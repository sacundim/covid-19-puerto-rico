#!/usr/bin/env bash

set -e
set -o pipefail

TESTS_ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests"
ORDERS_ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/orders/basic"
timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
tests_basename="minimal-info-unique-tests_${timestamp}"
orders_basename="orders-basic_${timestamp}"

# Set environment variable externally to override this default
S3_DATA_URL="${S3_DATA_URL-s3://covid-19-puerto-rico-data}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."

TMP="${REPO_ROOT}/tmp"
TESTS_JSON_TMP="${TMP}/${tests_basename}.json.bz2"
TESTS_PARQUET_TMP="${TMP}/${tests_basename}.parquet"
ORDERS_JSON_TMP="${TMP}/${orders_basename}.json.bz2"
ORDERS_PARQUET_TMP="${TMP}/${orders_basename}.parquet"

S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"
BIOPORTAL_SYNC_DIR="${S3_SYNC_DIR}/bioportal"
TESTS_JSON="${BIOPORTAL_SYNC_DIR}/tests/json_v2/${tests_basename}.json.bz2"
TESTS_PARQUET="${BIOPORTAL_SYNC_DIR}/tests/parquet_v1/${tests_basename}.parquet"
ORDERS_JSON="${BIOPORTAL_SYNC_DIR}/orders-basic/json_v1/${orders_basename}.json.bz2"
ORDERS_PARQUET="${BIOPORTAL_SYNC_DIR}/orders-basic/parquet_v1/${orders_basename}.parquet"


echo "$(date): Fetching from tests endpoint..."
time wget --header="Accept-Encoding: gzip" -O - "${TESTS_ENDPOINT}" \
  | gunzip \
  | bzip2 -9 \
  > "${TESTS_JSON_TMP}"
echo "$(date): Downloaded to ${TESTS_JSON_TMP}"

echo "$(date): Fetching from orders/basic endpoint..."
time wget --header="Accept-Encoding: gzip"  -O - "${ORDERS_ENDPOINT}" \
  | gunzip \
  | bzip2 -9 \
  > "${ORDERS_JSON_TMP}"
echo "$(date): Downloaded to ${ORDERS_JSON_TMP}"


echo "$(date): Converting tests to parquet..."
# TRICKY: One is tempted to use the timestamp data type option
# in the `csv2parquet` tool, but the files have weird date/time formats
# better fixed in a different tool.
time "${HERE}"/bioportal-tests-to-csv.sh "${timestamp}" "${TESTS_JSON_TMP}" \
  | time csv2parquet --codec gzip --row-group-size 10000000 \
      --output /dev/stdout \
      /dev/stdin \
    > "${TESTS_PARQUET_TMP}"
echo "$(date): Wrote output to ${TESTS_PARQUET_TMP}"

echo "$(date): Converting orders-basic to parquet..."
# TRICKY: One is tempted to use the timestamp data type option
# in the `csv2parquet` tool, but Athena can't read the timestamps
# it produces, because Parquet is a horrible mess.
time "${HERE}"/bioportal-basic-to-csv.sh "${timestamp}" "${ORDERS_JSON_TMP}" \
  | time csv2parquet --codec gzip --row-group-size 10000000 \
      --output /dev/stdout \
      /dev/stdin \
  > "${ORDERS_PARQUET_TMP}"
echo "$(date): Wrote output to ${ORDERS_PARQUET_TMP}"

#REMOVE ME
exit 0

echo "$(date): File sizes:"
du -h "${TESTS_JSON_TMP}" "${ORDERS_JSON_TMP}" \
  "${TESTS_PARQUET_TMP}" "${ORDERS_PARQUET_TMP}"

echo "$(date): Moving files to the sync directory"
mv "${TESTS_JSON_TMP}" "${TESTS_JSON}"
mv "${TESTS_PARQUET_TMP}" "${TESTS_PARQUET}"
mv "${ORDERS_JSON_TMP}" "${ORDERS_JSON}"
mv "${ORDERS_PARQUET_TMP}" "${ORDERS_PARQUET}"
