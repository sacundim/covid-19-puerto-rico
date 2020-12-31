#!/usr/bin/env bash
#
# Deploy Git repo assets to S3 data bucket.
#
set -e -o pipefail

# Set environment variable externally to override this default
S3_DATA_URL="${S3_DATA_URL-s3://covid-19-puerto-rico-data}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
BULLETIN_CASES_FROM="${REPO_ROOT}/assets/data/cases/PuertoRico-bitemporal.csv"
BULLETIN_CASES_TO="${S3_DATA_URL}/bulletin/cases/PuertoRico-bitemporal.csv"
SOURCE_MATERIAL_DIR="${REPO_ROOT}/assets/source_material"

echo "$(date): Copying ${BULLETIN_CASES_FROM} to ${BULLETIN_CASES_TO}"
aws s3 cp "${BULLETIN_CASES_FROM}" "${BULLETIN_CASES_TO}"

echo "$(date): Syncing ${SOURCE_MATERIAL_DIR} to ${S3_DATA_URL}/source_material"
time aws s3 sync $* --exclude '*.DS_Store' \
  "${SOURCE_MATERIAL_DIR}/" "${S3_DATA_URL}/source_material/"