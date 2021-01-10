#!/usr/bin/env bash
#
# Deploy Git repo assets to S3 data bucket.
#
set -e -o pipefail

# Set environment variable externally to override this default
S3_DATA_URL="${S3_DATA_URL-s3://covid-19-puerto-rico-data}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
ASSETS_DIR="${REPO_ROOT}/assets"
S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"

BULLETIN_CASES_FROM="${REPO_ROOT}/assets/data/cases/PuertoRico-bitemporal.csv"
BULLETIN_CASES_TMP="${S3_SYNC_DIR}/bulletin/cases/"
BULLETIN_CASES_TO="${S3_DATA_URL}/bulletin/cases/"

# We copy to a tmp folder then `aws s3 sync`, instead of doing a
# straight `aws s3 cp`, because `sync` has the smarts to check if
# the local file is newer instead of blindly uploading it.
echo "$(date): Copying ${BULLETIN_CASES_FROM} to ${BULLETIN_CASES_TMP}"
mkdir -p "$(dirname "${BULLETIN_CASES_TMP}")"
cp -p "${BULLETIN_CASES_FROM}" "${BULLETIN_CASES_TMP}"

echo "$(date): Syncing ${BULLETIN_CASES_FROM} to ${BULLETIN_CASES_TO}"
aws s3 sync $* "${BULLETIN_CASES_TMP}" "${BULLETIN_CASES_TO}"

echo "$(date): Syncing ${ASSETS_DIR}/source_material/ to ${S3_DATA_URL}/source_material/"
time aws s3 sync $* --exclude '*.DS_Store' \
  "${ASSETS_DIR}/source_material/" "${S3_DATA_URL}/source_material/"

if [ -d "${S3_SYNC_DIR}/worksheets" ]
then
  echo "$(date): Syncing ${S3_SYNC_DIR}/worksheets/ to ${S3_DATA_URL}/worksheets/"
  time aws s3 sync $* --exclude '*.DS_Store' \
    "${S3_SYNC_DIR}/worksheets/" "${S3_DATA_URL}/worksheets/"
fi