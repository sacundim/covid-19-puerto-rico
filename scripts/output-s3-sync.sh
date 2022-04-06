#!/usr/bin/env bash
#
# Deploy Git repo assets to S3 data bucket.
#
set -e -o pipefail

# Set environment variable externally to override this default
S3_WEBSITE_URL="${S3_WEBSITE_URL-s3://covid-19-puerto-rico}"

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
OUTPUT_DIR="${REPO_ROOT}/output"

echo "$(date): Syncing ${OUTPUT_DIR} to ${S3_WEBSITE_URL}"
aws s3 sync \
  --exclude '*.DS_Store' \
  --exclude 'source_material/*' \
  "$@" \
  "${OUTPUT_DIR}" \
  "${S3_WEBSITE_URL}"
