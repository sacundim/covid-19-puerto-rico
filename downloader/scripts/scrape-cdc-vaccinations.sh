#!/usr/bin/env bash
#
# Script to scrape the municipal vaccinations data from the
# CDC COVID Tracker website.
#

set -eu -o pipefail

ENDPOINT="https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_county_condensed_data"
timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
downloaded_date="${timestamp:0:10}"
basename="vaccination_county_condensed_data_${timestamp}"
HERE="$(dirname $0)"
REPO_ROOT="${HERE}"/..
TMP="${REPO_ROOT}"/tmp
S3_SYNC_DIR="${REPO_ROOT}"/s3-bucket-sync/covid-19-puerto-rico-data
CDC_TRACKER_DIR="${S3_SYNC_DIR}"/cdc-covid-data-tracker
VACCINATION_DIR="${CDC_TRACKER_DIR}"/vaccination_county_condensed_data

cd "${TMP}"
echo "$(date): Downloading from ${ENDPOINT}"
wget --no-verbose -O "${basename}".json "${ENDPOINT}"
cat "${basename}".json \
  |jq -c '{runid: .runid} + .vaccination_county_condensed_data[]' \
  >"${basename}".jsonl

echo "$(date): Compressing files"
bzip2 -9 "${basename}".json "${basename}".jsonl

echo "$(date): File sizes:"
du -h "${basename}".json.bz2 "${basename}".jsonl.bz2

echo "$(date): Moving to sync directory..."
mkdir -p \
  "${VACCINATION_DIR}"/json_v1/downloaded_date="$downloaded_date" \
  "${VACCINATION_DIR}"/jsonl_v1/downloaded_date="$downloaded_date"
mv "${basename}".json.bz2 "${VACCINATION_DIR}"/json_v1/downloaded_date="$downloaded_date"/
mv "${basename}".jsonl.bz2 "${VACCINATION_DIR}"/jsonl_v1/downloaded_date="$downloaded_date"/