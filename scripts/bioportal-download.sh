#!/usr/bin/env bash

set -e
set -o pipefail

CASES_ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/orders/minimal-info"
TESTS_ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests"
timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
tests_basename="minimal-info-unique-tests_V2_${timestamp}"
cases_basename="minimal-info_${timestamp}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
TMP="${REPO_ROOT}/tmp"
TESTS_JSON="${TMP}/tests/json_v2/${tests_basename}.json.bz2"
TESTS_CSV="${TMP}/tests/csv_v2/${tests_basename}.csv.bz2"
CASES_JSON="${TMP}/cases/json_v1/${cases_basename}.json.bz2"
CASES_CSV="${TMP}/cases/csv_v1/${cases_basename}.csv.bz2"

OUTPUT_DIR="${REPO_ROOT}/assets/data/bioportal"


echo "$(date): Fetching from tests endpoint..."
time wget -O - "${TESTS_ENDPOINT}" \
  | bzip2 -9 \
  > "${TESTS_JSON_TMP}"
echo "$(date): Downloaded to ${TESTS_JSON_TMP}"

echo "$(date): Fetching from cases endpoint..."
time wget -O - "${CASES_ENDPOINT}" \
  | bzip2 -9 \
  > "${CASES_JSON_TMP}"
echo "$(date): Downloaded to ${CASES_JSON_TMP}"


echo "$(date): Converting tests to csv..."
time "${HERE}"/bioportal-tests-to-csv.sh "${timestamp}" "${TESTS_JSON_TMP}" \
    | bzip2 -9 \
    > "${TESTS_CSV_TMP}"
echo "$(date): Wrote output to ${TESTS_CSV_TMP}"
cp "${TESTS_CSV_TMP}" "${OUTPUT_DIR}/v2/"
echo "$(date): Copied output to ${OUTPUT_DIR}/v2/"
cp "${TESTS_CSV_TMP}" "${TESTS_CSV}"
echo "$(date): Copied output to ${TESTS_CSV}"

echo "$(date): Converting cases to csv..."
time "${HERE}"/bioportal-cases-to-csv.sh "${timestamp}" "${CASES_JSON_TMP}" \
    | bzip2 -9 \
    > "${CASES_CSV_TMP}"
echo "$(date): Wrote output to ${CASES_CSV_TMP}"
cp "${CASES_CSV_TMP}" "${CASES_CSV}"
echo "$(date): Copied output to ${CASES_CSV}"


#echo "$(date): Syncing data to S3..."
#time aws s3 sync "${S3_SYNC_DIR}" "${S3_DATA_URL}"