#!/usr/bin/env bash

set -e

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
time curl "${TESTS_ENDPOINT}" \
  | bzip2 -9 \
  > "${TESTS_JSON}"
echo "$(date): Downloaded to ${TESTS_JSON}"

echo "$(date): Fetching from cases endpoint..."
time curl "${CASES_ENDPOINT}" \
  | bzip2 -9 \
  > "${CASES_JSON}"
echo "$(date): Downloaded to ${CASES_JSON}"


echo "$(date): Converting tests to csv..."
time "${HERE}"/bioportal-tests-to-csv.sh "${timestamp}" "${TESTS_JSON}" \
    | bzip2 -9 \
    > "${TESTS_CSV}"
echo "$(date): Wrote output to ${TESTS_CSV}"
cp "${TESTS_CSV}" "${OUTPUT_DIR}/v2/"
echo "$(date): Copied output to ${OUTPUT_DIR}/v2/"

echo "$(date): Converting cases to csv..."
time "${HERE}"/bioportal-cases-to-csv.sh "${timestamp}" "${CASES_JSON}" \
    | bzip2 -9 \
    > "${CASES_CSV}"
echo "$(date): Wrote output to ${CASES_CSV}"

