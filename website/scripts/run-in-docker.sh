#!/usr/bin/env bash
#
# Before you run this script:
#
# 1. Build the application image (`build-docker-image.sh`)
# 2. Start up the Docker Compose environment in repo root
#    (`docker-compose up`)
#
# To run:
#
#     run-in-docker.sh --bulletin-date <bulletin date>
#
# For example:
#
#     run-in-docker.sh --bulletin-date 2020-05-04

set -e -x

# Override this to use e.g. podman instead of docker:
DOCKER="${DOCKER:=docker}"
DOCKER_IMAGE="${DOCKER_IMAGE:=covid-19-puerto-rico}"

cd "$(dirname $0)"/..
./scripts/build-docker-image.sh

rm -rf output/*

time "${DOCKER}" run --rm \
  -v ~/.aws:/awsconfig:ro \
  -v "$(pwd)"/config:/config:ro \
  -v "$(pwd)"/assets:/assets:ro \
  -v "$(pwd)"/output:/output:rw \
  --env AWS_CONFIG_FILE=/awsconfig/config \
  --env AWS_SHARED_CREDENTIALS_FILE=/awsconfig/credentials \
  "${DOCKER_IMAGE}" \
    --config-file /config/docker.toml \
    --assets-dir /assets \
    --output-dir /output \
    "$@"
