#!/usr/bin/env bash
#
# Convert a `minimal-info-unique-tests` download to CSV.
#

set -e
set -o pipefail

downloadedAt="${1:?"No downloadedAt argument given"}"
file="${2:?"No argument file given"}"

echo 'downloadedAt,collectedDate,reportedDate,ageRange,testType,result,city,createdAt'
cat "${file}" \
    | bunzip2 \
    | jq -r '.[] | [.collectedDate, .reportedDate, .ageRange, .testType, .result, .city, .createdAt] | @csv' \
    | sed -e "s/^/${downloadedAt},/"