#!/usr/bin/env bash

set -e

ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests"
timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
basename="minimal-info-unique-tests_${timestamp}"

HERE="$(dirname $0)"
TMP="$(dirname $0)/../tmp"

time wget \
    --output-document="${TMP}/${basename}.json" \
    "${ENDPOINT}"

echo "Converting to csv..."
"${HERE}"/bioportal-json-to-csv.sh \
    "${TMP}/${basename}.json" \
    > "${TMP}/${basename}.csv"

echo "Wrote output to ${TMP}/${basename}.csv"
