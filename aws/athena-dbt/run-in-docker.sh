#!/usr/bin/env bash

set -e -x

# Override this to use e.g. podman instead of docker:
DOCKER="${DOCKER:=docker}"
DOCKER_IMAGE="${DOCKER_IMAGE:=covid-19-puerto-rico-dbt}"
AWS_REGION="${AWS_REGION:=us-west-2}"
ATHENA_S3_SCHEMA="${ATHENA_S3_SCHEMA:=covid19_puerto_rico_model}"
ATHENA_WORK_GROUP="${ATHENA_WORK_GROUP:=covid-19-puerto-rico}"
ATHENA_S3_STAGING_DIR="${ATHENA_S3_STAGING_DIR:=s3://covid-19-puerto-rico-athena/}"

cd "$(dirname $0)"
./build-docker-image.sh

time "${DOCKER}" run --rm \
  -v ~/.aws:/awsconfig:ro \
  --env AWS_CONFIG_FILE=/awsconfig/config \
  --env AWS_SHARED_CREDENTIALS_FILE=/awsconfig/credentials \
  --env AWS_REGION="${AWS_REGION}" \
  --env ATHENA_S3_SCHEMA="${ATHENA_S3_SCHEMA}" \
  --env ATHENA_WORK_GROUP="${ATHENA_WORK_GROUP}" \
  --env ATHENA_S3_STAGING_DIR="${ATHENA_S3_STAGING_DIR}" \
  "${DOCKER_IMAGE}" ./run-models.sh "$@"
