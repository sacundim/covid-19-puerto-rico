#!/usr/bin/env bash

downloadedAt="${1:?"No downloadedAt argument given"}"
file="${2:?"No argument file given"}"

echo 'downloadedAt,patientId,collectedDate,reportedDate,ageRange,testType,result,patientCity,createdAt'
bzcat "${file}" \
    | jq -r '.[] | [.patientId, .collectedDate, .reportedDate, .ageRange, .testType, .result, .patientCity, .createdAt] | @csv' \
    | sed -e "s/^/${downloadedAt},/"