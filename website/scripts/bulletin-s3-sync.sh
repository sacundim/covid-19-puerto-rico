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
MUNI_MOLECULAR_FROM="${REPO_ROOT}/assets/data/cases/Municipalities-molecular.csv"
MUNI_ANTIGENS_FROM="${REPO_ROOT}/assets/data/cases/Municipalities-antigens.csv"

BULLETIN_TMP="${S3_SYNC_DIR}/bulletin/"
BULLETIN_TO="${S3_DATA_URL}/bulletin/"

# We copy to a tmp folder then `aws s3 sync`, instead of doing a
# straight `aws s3 cp`, because `sync` has the smarts to check if
# the local file is newer instead of blindly uploading it.
echo "$(date): Copying daily bulletin data to ${BULLETIN_TMP}"
mkdir -p \
  "${BULLETIN_TMP}" \
  "${BULLETIN_TMP}"/cases \
  "${BULLETIN_TMP}"/municipal_molecular \
  "${BULLETIN_TMP}"/municipal_antigens
cp -p "${BULLETIN_CASES_FROM}" "${BULLETIN_TMP}"/cases/
cp -p "${MUNI_MOLECULAR_FROM}" "${BULLETIN_TMP}"/municipal_molecular/
cp -p "${MUNI_ANTIGENS_FROM}" "${BULLETIN_TMP}"/municipal_antigens/

echo "$(date): Syncing ${BULLETIN_TMP} to ${BULLETIN_TO}"
aws s3 sync $* "${BULLETIN_TMP}" "${BULLETIN_TO}"


##############################################################################
# These newer ones are laid out in an Athena-compatible directory structure.

echo "$(date): Syncing ${ASSETS_DIR}/data/Census/ to ${S3_DATA_URL}/Census/"
time aws s3 sync $* --exclude '*.DS_Store' \
  "${ASSETS_DIR}/data/Census/" "${S3_DATA_URL}/Census/"

echo "$(date): Syncing ${ASSETS_DIR}/data/CovidTracking/ to ${S3_DATA_URL}/CovidTracking/"
time aws s3 sync $* --exclude '*.DS_Store' \
  "${ASSETS_DIR}/data/CovidTracking/" "${S3_DATA_URL}/CovidTracking/"

echo "$(date): Syncing ${ASSETS_DIR}/source_material/ to ${S3_DATA_URL}/source_material/"
time aws s3 sync $* --exclude '*.DS_Store' \
  "${ASSETS_DIR}/source_material/" "${S3_DATA_URL}/source_material/"

if [ -d "${S3_SYNC_DIR}/worksheets" ]
then
  echo "$(date): Syncing ${S3_SYNC_DIR}/worksheets/ to ${S3_DATA_URL}/worksheets/"
  time aws s3 sync $* --exclude '*.DS_Store' \
    "${S3_SYNC_DIR}/worksheets/" "${S3_DATA_URL}/worksheets/"
fi