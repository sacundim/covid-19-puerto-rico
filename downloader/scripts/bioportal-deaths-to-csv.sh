#!/usr/bin/env bash

set -e
set -o pipefail

downloadedAt="${1:?"No downloadedAt argument given"}"
file="${2:?"No argument file given"}"

echo 'downloadedAt,region,ageRange,sex,deathDate,reportDate'
cat "${file}" \
    | bunzip2 \
    | jq -e -r '.[] | [.region, .ageRange, .sex, .deathDate, .reportDate] | @csv' \
    | sed -e "s/^/${downloadedAt},/"