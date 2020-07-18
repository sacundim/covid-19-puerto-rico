#!/usr/bin/env bash

ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests"
file="${1:?"No argument file given"}"

echo 'collectedDate,reportedDate,ageRange,testType,result,patientCity,createdAt'
cat "${file}" \
    | jq -r '.[] | [.collectedDate, .reportedDate, .ageRange, .testType, .result, .patientCity, .createdAt] | @csv' \
    | sort