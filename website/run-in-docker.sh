#!/usr/bin/env bash
set -euxo pipefail

# Override this to use e.g. podman instead of docker:
DOCKER="${DOCKER:=docker}"

export IMAGE_NAME="${IMAGE_NAME-docker.io/sacundim/covid-19-puerto-rico-website}"
export ATHENA_SCHEMA_NAME="${ATHENA_SCHEMA_NAME-covid19_puerto_rico_iceberg}"
export ATHENA_WORK_GROUP="${ATHENA_WORK_GROUP-covid-19-puerto-rico}"

cd "$(dirname $0)"
PLATFORM="linux" ./build-docker-image.sh --load

rm -rf output/*

time "${DOCKER}" run --rm \
  -v ~/.aws:/awsconfig:ro \
  -v "$(pwd)"/assets:/assets:ro \
  -v "$(pwd)"/output:/output:rw \
  --env AWS_CONFIG_FILE=/awsconfig/config \
  --env AWS_SHARED_CREDENTIALS_FILE=/awsconfig/credentials \
  --env AWS_REGION="$(aws configure get region)" \
  --env ATHENA_SCHEMA_NAME="${ATHENA_SCHEMA_NAME}" \
  --env ATHENA_WORK_GROUP="${ATHENA_WORK_GROUP}" \
  "${IMAGE_NAME}" \
    covid19pr \
      --config-file environment.yaml \
      --assets-dir /assets \
      --output-dir /output \
      "$@"