#!/usr/bin/env bash

ENDPOINT="https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests"
downloadedAt="${1:?"No downloadedAt argument given"}"
file="${2:?"No argument file given"}"


echo 'downloadedAt,patientId,collectedDate,reportedDate,ageRange,testType,result,patientCity,createdAt'
cat "${file}" \
    | jq -r '.[] | [.patientId, .collectedDate, .reportedDate, .ageRange, .testType, .result, .patientCity, .createdAt] | @csv' \
    | sed -e "s/^/${downloadedAt},/"