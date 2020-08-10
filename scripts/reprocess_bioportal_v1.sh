#!/usr/bin/env bash
#
# One-time script to reprocess a bunch of old Bioportal V1 downloads
# into the new format CSVs.

set -e

HERE="$(dirname $0)"

timestamps="$(\
    ls minimal-info-unique-tests_*.json \
        |sed -E -e 's/^minimal-info-unique-tests_(.+)\.json/\1/g')"

for ts in $timestamps
do
    echo "$(date): Processing timestamp ${ts}"
    echo "downloadedAt,collectedDate,reportedDate,ageRange,testType,result,patientCity,createdAt" \
        > minimal-info-unique-tests_V1_"${ts}".csv
    cat minimal-info-unique-tests_"${ts}".json \
        | jq -r '.[] | [.collectedDate, .reportedDate, .ageRange, .testType, .result, .patientCity, .createdAt] | @csv' \
        | sed -e "s/^/${ts},/" \
        >> minimal-info-unique-tests_V1_"${ts}".csv
done