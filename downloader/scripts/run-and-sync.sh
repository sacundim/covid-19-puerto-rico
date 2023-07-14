#!/usr/bin/env bash

set -euxo pipefail


# Set environment variables to override defaults
RCLONE_DESTINATION="${RCLONE_DESTINATION:=":s3,provider=AWS,env_auth:${TARGET_BUCKET:?"TARGET_BUCKET not set"}"}"
HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"

COMMAND="${1?"No command specified"}"

cd /covid-19-puerto-rico

echo "$(date): Running command: ${COMMAND}"
"${COMMAND}" --s3-sync-dir "${S3_SYNC_DIR}" "${@:2}"


echo "$(date): Syncing ${S3_SYNC_DIR}/ to ${RCLONE_DESTINATION}/"
rclone copy \
  --fast-list \
  --verbose \
  --checksum \
  --exclude '*.DS_Store' \
  "${S3_SYNC_DIR}" "${RCLONE_DESTINATION}"

echo "$(date): All done!"
