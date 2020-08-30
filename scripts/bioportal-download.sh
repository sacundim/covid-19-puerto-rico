#!/usr/bin/env bash

set -e

ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests"
timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
basename="minimal-info-unique-tests_V2_${timestamp}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
TMP="${REPO_ROOT}/tmp"
JSON_PATH="${REPO_ROOT}/tmp/${basename}.json"
CSV_PATH="${REPO_ROOT}/assets/data/bioportal/v2/${basename}.csv.bz2"

time wget \
    --output-document="${JSON_PATH}" \
    "${ENDPOINT}"

echo "$(date): Converting to csv..."
"${HERE}"/bioportal-json-to-csv.sh "${timestamp}" "${JSON_PATH}" \
    | bzip2 -9 \
    > "${CSV_PATH}"
echo "$(date): Wrote output to ${CSV_PATH}"

LINE_COUNT="$(cat "${CSV_PATH}" |bunzip2 |tail -n+2 |wc -l)"
echo "$(date): Line count: ${LINE_COUNT}"

echo "$(date): Compressing downloaded json..."
bzip2 -9 "${JSON_PATH}"