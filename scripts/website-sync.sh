#!/usr/bin/env bash
#
# Sync the website to the cloud server infra
#

set -euo pipefail

S3_PATH="s3://covid-19-puerto-rico"
MESSAGE="${1:?"No commit message given"}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
WEBSITE_DIR="${REPO_ROOT}/output"

if [ -d "${WEBSITE_DIR}" ];
then
  # TODO: stop using this
  echo "$(date): Syncing to gh-pages..."
  ghp-import -p -m "${MESSAGE}" "${WEBSITE_DIR}"

  echo "$(date): Syncing to S3..."
  aws s3 sync \
    --exclude '*.DS_Store' \
    "${WEBSITE_DIR}" "${S3_PATH}"
else
  echo "$(date): No such directory: ${WEBSITE_DIR}"
  exit 1
fi