#!/usr/bin/env bash

ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests"
timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
filename="minimal-info-unique-tests_${timestamp}.json"

time wget --output-document="${filename}" "${ENDPOINT}"