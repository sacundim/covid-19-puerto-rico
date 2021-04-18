#!/usr/bin/env bash

set -e
set -o pipefail

TESTS_ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests"
ORDERS_ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/orders/basic"
timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
downloaded_date="${timestamp:0:10}"
ts_seconds="$(date -u +"%s")"
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
TESTS_DIR="${BIOPORTAL_SYNC_DIR}/minimal-info-unique-tests"
TESTS_JSON="${TESTS_DIR}/json_v3/${tests_basename}.json.bz2"
TESTS_PARQUET="${TESTS_DIR}/parquet_v4/downloaded_date=${downloaded_date}/${tests_basename}.parquet"
ORDERS_DIR="${BIOPORTAL_SYNC_DIR}/orders-basic"
ORDERS_JSON="${ORDERS_DIR}/json_v1/${orders_basename}.json.bz2"
ORDERS_PARQUET="${ORDERS_DIR}/parquet_v2/downloaded_date=${downloaded_date}/${orders_basename}.parquet"

mkdir -p "${TMP}"

echo "$(date): Fetching from endpoint: ${TESTS_ENDPOINT}"
time wget \
    --no-verbose \
    --header="Accept-Encoding: gzip" \
    -O - "${TESTS_ENDPOINT}" \
  | gunzip \
  | bzip2 -9 \
  > "${TESTS_JSON_TMP}"
echo "$(date): Downloaded to ${TESTS_JSON_TMP}"

echo "$(date): Fetching from endpoint: ${ORDERS_ENDPOINT}"
time wget \
    --no-verbose \
    --header="Accept-Encoding: gzip" \
    -O - "${ORDERS_ENDPOINT}" \
  | gunzip \
  | bzip2 -9 \
  > "${ORDERS_JSON_TMP}"
echo "$(date): Downloaded to ${ORDERS_JSON_TMP}"


echo "$(date): Converting tests to parquet..."
# TRICKY: One is tempted to use the timestamp data type option
# in the `csv2parquet` tool, but the files have weird date/time formats
# better fixed in a different tool.
time "${HERE}"/bioportal-tests-to-csv.sh "${timestamp}" "${TESTS_JSON_TMP}" \
  > "${TMP}/minimal-info-unique-tests_${ts_seconds}.csv"

# PyArrow tends to barf on filenames with colons (thinks they're URLs),
# so we have to generate a weird filename and rename it. Also, we used to
# just pipe data through csv2parquet, but we've hit problems that I think
# were unflushed buffers so we write a CSV and convert it.
time csv2parquet --codec gzip --row-group-size 10000000 \
  "${TMP}/minimal-info-unique-tests_${ts_seconds}.csv"
mv "${TMP}/minimal-info-unique-tests_${ts_seconds}.parquet" "${TESTS_PARQUET_TMP}"
echo "$(date): Wrote output to ${TESTS_PARQUET_TMP}"


echo "$(date): Converting orders-basic to parquet..."
time "${HERE}"/bioportal-basic-to-csv.sh "${timestamp}" "${ORDERS_JSON_TMP}" \
  > "${TMP}/orders-basic_${ts_seconds}.csv"
time csv2parquet --codec gzip --row-group-size 10000000 \
  "${TMP}/orders-basic_${ts_seconds}.csv"
mv "${TMP}/orders-basic_${ts_seconds}.parquet" "${ORDERS_PARQUET_TMP}"
echo "$(date): Wrote output to ${ORDERS_PARQUET_TMP}"


echo "$(date): File sizes:"
du -h "${TESTS_JSON_TMP}" "${ORDERS_JSON_TMP}" \
  "${TESTS_PARQUET_TMP}" "${ORDERS_PARQUET_TMP}"

echo "$(date): Moving files to the sync directory"
mkdir -p \
  "${TESTS_DIR}" \
  "$(dirname "${TESTS_JSON}")" \
  "$(dirname "${TESTS_PARQUET}")" \
  "${ORDERS_DIR}" \
  "$(dirname "${ORDERS_JSON}")" \
  "$(dirname "${ORDERS_PARQUET}")"
mv "${TESTS_JSON_TMP}" "${TESTS_JSON}"
mv "${TESTS_PARQUET_TMP}" "${TESTS_PARQUET}"
mv "${ORDERS_JSON_TMP}" "${ORDERS_JSON}"
mv "${ORDERS_PARQUET_TMP}" "${ORDERS_PARQUET}"
