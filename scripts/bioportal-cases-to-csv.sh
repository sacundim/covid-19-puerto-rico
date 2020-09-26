#!/usr/bin/env bash

set -e

downloadedAt="${1:?"No downloadedAt argument given"}"
file="${2:?"No argument file given"}"

echo 'downloadedAt,patientId,collectedDate,reportedDate,ageRange,testType,result,region,createdAt'
cat "${file}" \
    | bunzip2 \
    | jq -e -r '.[] | [.patientId, .collectedDate, .reportedDate, .ageRange, .testType, .result, .region, .createdAt] | @csv' \
    | sed -e "s/^/${downloadedAt},/"