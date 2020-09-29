#!/usr/bin/env bash

# Set environment variable externally to override this default
S3_DATA_URL="${S3_DATA_URL-s3://covid-19-puerto-rico-data}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
BULLETIN_CASES_CSV="${REPO_ROOT}/assets/data/cases/PuertoRico-bitemporal.csv"
SOURCE_MATERIAL_DIR="${REPO_ROOT}/assets/source_material"
S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"


echo "$(date): Copying ${BULLETIN_CASES_CSV} to ${S3_SYNC_DIR}/bulletin/cases/"
cp -fp "${BULLETIN_CASES_CSV}" \
  "${S3_SYNC_DIR}/bulletin/cases/"

echo "$(date): Syncing ${S3_SYNC_DIR} to ${S3_DATA_URL}"
time aws s3 sync $* --exclude '*.DS_Store' \
  "${S3_SYNC_DIR}" "${S3_DATA_URL}"

echo "$(date): Syncing ${SOURCE_MATERIAL_DIR} to ${S3_DATA_URL}/source_material"
time aws s3 sync $* --exclude '*.DS_Store' \
  "${SOURCE_MATERIAL_DIR}/" "${S3_DATA_URL}/source_material/"