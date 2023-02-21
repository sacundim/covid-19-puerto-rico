#!/usr/bin/env bash
#
# Fetch the Walgrens/Aegis variant data from Andy Bloch's
# "Walgreens COVID-19 Tracker Tracker." This is a dashboard
# that scrapes data from Walgrens/Aegis's dashboard but
# presents it better.
#
# * https://observablehq.com/@andy-bloch/walgreens-covid-19-tracker
#

set -eu
set -o pipefail

ENDPOINT="https://labvegas.com/data/covid19/walgreens/dashboard-Tracker_Aggregation.csv"
timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
BASENAME="dashboard-Tracker_Aggregation"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
TMP="${REPO_ROOT}/tmp"
mkdir -p "${TMP}"

S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"
WALGREENS_SYNC_DIR="${S3_SYNC_DIR}/Walgreens"
AGGREGATION_SYNC_DIR="${WALGREENS_SYNC_DIR}/Tracker_Aggregation"
mkdir -p "${S3_SYNC_DIR}" "${WALGREENS_SYNC_DIR}" "${AGGREGATION_SYNC_DIR}"
mkdir -p "${AGGREGATION_SYNC_DIR}"/csv_v1 "${AGGREGATION_SYNC_DIR}"/parquet_v2

echo "$(date): $(csv2parquet --version)"

echo "$(date): Fetching from endpoint: ${ENDPOINT}"
wget --no-verbose -O "${TMP}/${BASENAME}.csv" "${ENDPOINT}"
echo "$(date): Downloaded to ${BASENAME}.csv"

echo "$(date): Generating Parquet..."
csv2parquet \
  --compression gzip \
  --statistics page \
  "${TMP}/${BASENAME}.csv" \
  "${TMP}/${BASENAME}.parquet"

echo "$(date): Compressing CSV..."
lbzip2 -9 "${TMP}/${BASENAME}.csv"

echo "$(date): Moving to ${AGGREGATION_SYNC_DIR}..."
mv "${TMP}/${BASENAME}.csv.bz2" \
  "${AGGREGATION_SYNC_DIR}/csv_v1/${BASENAME}_${timestamp}.csv.bz2"
mv "${TMP}/${BASENAME}.parquet" \
  "${AGGREGATION_SYNC_DIR}/parquet_v2/${BASENAME}_${timestamp}.parquet"

echo "$(date): Listing out ${AGGREGATION_SYNC_DIR}..."
du -h "${AGGREGATION_SYNC_DIR}"/*