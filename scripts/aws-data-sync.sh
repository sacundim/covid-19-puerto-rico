#!/usr/bin/env bash

# Set environment variable externally to override this default
S3_DATA_URL="${S3_DATA_URL-s3://covid-19-puerto-rico-data}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"

echo "$(date): Syncing ${S3_SYNC_DIR} to ${S3_DATA_URL}"
time aws s3 sync --exclude '*.DS_Store' \
  "${S3_SYNC_DIR}" "${S3_DATA_URL}"