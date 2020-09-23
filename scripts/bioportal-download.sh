#!/usr/bin/env bash

set -e

CASES_ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/orders/minimal-info"
TESTS_ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests"
timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
cases_basename="minimal-info_${timestamp}"
tests_basename="minimal-info-unique-tests_V2_${timestamp}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
TMP="${REPO_ROOT}/tmp"
OUTPUT_DIR="${REPO_ROOT}/assets/data/bioportal"
TESTS_DIR="${OUTPUT_DIR}/v2"
CASES_DIR="${TMP}/cases/csv_v1"


echo "$(date): Fetching from tests endpoint..."
time curl "${TESTS_ENDPOINT}" \
  | bzip2 -9 \
  > "${TMP}/tests/json_v2/${tests_basename}.json.bz2"

echo "$(date): Fetching from cases endpoint..."
time curl "${CASES_ENDPOINT}" \
  | bzip2 -9 \
  > "${TMP}/cases/json_v1/${cases_basename}.json.bz2"


echo "$(date): Converting tests to csv..."
"${HERE}"/bioportal-tests-to-csv.sh "${timestamp}" "${TMP}/${tests_basename}.json" \
    | bzip2 -9 \
    > "${TESTS_DIR}/${tests_basename}.csv.bz2"
echo "$(date): Wrote output to ${TESTS_DIR}/${tests_basename}.csv.bz2"

echo "$(date): Converting cases to csv..."
mkdir -p ${CASES_DIR}
"${HERE}"/bioportal-cases-to-csv.sh "${timestamp}" "${TMP}/${cases_basename}.json" \
    | bzip2 -9 \
    > "${CASES_DIR}/${cases_basename}.csv.bz2"
echo "$(date): Wrote output to ${CASES_DIR}/${cases_basename}.csv.bz2"

