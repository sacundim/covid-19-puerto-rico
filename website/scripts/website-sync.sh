#!/usr/bin/env bash
#
# Sync the website to the cloud server infra
#

set -euo pipefail

RCLONE_DESTINATION="covid-19-puerto-rico:covid-19-puerto-rico"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
WEBSITE_DIR="${REPO_ROOT}/output"

if [ -d "${WEBSITE_DIR}" ];
then
  echo "$(date): Syncing to S3..."
  rclone copy \
    --fast-list \
    --verbose \
    --checksum \
    --exclude '*.DS_Store' \
    "${WEBSITE_DIR}" "${RCLONE_DESTINATION}"
else
  echo "$(date): No such directory: ${WEBSITE_DIR}"
  exit 1
fi