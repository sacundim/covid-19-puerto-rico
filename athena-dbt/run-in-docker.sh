#!/usr/bin/env bash
set -euxo pipefail

# Overrides for AWS parameters
AWS_REGION="${AWS_REGION:=us-west-2}"
ATHENA_S3_SCHEMA="${ATHENA_S3_SCHEMA:=covid19_puerto_rico_iceberg}"
ATHENA_WORK_GROUP="${ATHENA_WORK_GROUP:=covid-19-puerto-rico-dbt}"
ATHENA_S3_STAGING_DIR="${ATHENA_S3_STAGING_DIR:=s3://covid-19-puerto-rico-athena/}"
ATHENA_S3_DATA_DIR="${ATHENA_S3_DATA_DIR:=s3://covid-19-puerto-rico-iceberg/}"

# Override to use a different image name or tag
IMAGE_NAME="${IMAGE_NAME-docker.io/sacundim/covid-19-puerto-rico-dbt}"

# Override this to use e.g. podman instead of docker:
DOCKER="${DOCKER-docker}"


cd "$(dirname $0)"
PLATFORM="linux" ./build-docker-image.sh --load

time "${DOCKER}" run --rm \
  -v ~/.aws:/awsconfig:ro \
  --env AWS_CONFIG_FILE=/awsconfig/config \
  --env AWS_SHARED_CREDENTIALS_FILE=/awsconfig/credentials \
  --env AWS_REGION="${AWS_REGION}" \
  --env ATHENA_S3_SCHEMA="${ATHENA_S3_SCHEMA}" \
  --env ATHENA_WORK_GROUP="${ATHENA_WORK_GROUP}" \
  --env ATHENA_S3_STAGING_DIR="${ATHENA_S3_STAGING_DIR}" \
  --env ATHENA_S3_DATA_DIR="${ATHENA_S3_DATA_DIR}" \
  "${IMAGE_NAME}" "$@"
