#!/usr/bin/env bash
#
# This script is meant to be used as an optional image entrypoint
# for jobs that want to also sync to S3
#
set -euxo pipefail

# Set environment variables to override defaults
RCLONE_DESTINATION="${RCLONE_DESTINATION:=":s3,provider=AWS,env_auth:${MAIN_BUCKET:?"MAIN_BUCKET not set"}"}"

cat >config.toml <<EOF
[athena]
region_name = "${AWS_REGION?"AWS_REGION not set"}"
schema_name = "${ATHENA_SCHEMA_NAME?"ATHENA_SCHEMA_NAME not set"}"
s3_staging_dir = "${ATHENA_S3_STAGING_DIR?"ATHENA_S3_STAGING_DIR not set"}"
EOF

WEBSITE_DIR="/output"

echo "$(date): Building website"
covid19pr \
    --config-file config.toml \
    --output-dir "${WEBSITE_DIR}" \
    "$@"


echo "$(date): Syncing to S3"
rclone copy \
  --fast-list \
  --verbose \
  --checksum \
  --exclude '*.DS_Store' \
  "${WEBSITE_DIR}" "${RCLONE_DESTINATION}"
