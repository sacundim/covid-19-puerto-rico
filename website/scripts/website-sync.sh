#!/usr/bin/env bash
#
# Sync the website to the cloud server infra.
#
# Prerequisite tool:
#
# * https://rclone.org/
#
# The Rclone S3 endpoint must be preconfigured in
# the environment.

set -euo pipefail

# Set environment variable to override default
RCLONE_DESTINATION="${RCLONE_DESTINATION:="covid-19-puerto-rico:covid-19-puerto-rico"}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
WEBSITE_DIR="${REPO_ROOT}/output"

if [ -d "${WEBSITE_DIR}" ];
then
  echo "$(date): Syncing to S3..."
  rclone copy \
    --fast-list \
    --progress \
    --verbose \
    --checksum \
    --exclude '*.DS_Store' \
    "${WEBSITE_DIR}" "${RCLONE_DESTINATION}"
else
  echo "$(date): No such directory: ${WEBSITE_DIR}"
  exit 1
fi