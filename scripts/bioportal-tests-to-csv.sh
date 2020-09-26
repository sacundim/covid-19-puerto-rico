#!/usr/bin/env bash

set -e
set -o pipefail

downloadedAt="${1:?"No downloadedAt argument given"}"
file="${2:?"No argument file given"}"

echo 'downloadedAt,patientId,collectedDate,reportedDate,ageRange,testType,result,patientCity,createdAt'
cat "${file}" \
    | bunzip2 \
    | jq -r '.[] | [.patientId, .collectedDate, .reportedDate, .ageRange, .testType, .result, .patientCity, .createdAt] | @csv' \
    | sed -e "s/^/${downloadedAt},/"