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

DOCKER_IMAGE="${DOCKER_IMAGE:=covid-19-puerto-rico}"

# If your Docker Compose environment's network is named something
# different you can override it by exporting your own value for
# this variable:
DOCKER_NETWORK="${DOCKER_NETWORK:-covid-19-puerto-rico_default}"

cd "$(dirname $0)"/..
./scripts/build-docker-image.sh

rm -rf output/*

time docker run --rm \
  --network="${DOCKER_NETWORK}" \
  -v "$(pwd)"/config:/config:ro \
  -v "$(pwd)"/assets:/assets:ro \
  -v "$(pwd)"/output:/output:rw \
  "${DOCKER_IMAGE}" \
    --config-file /config/docker.toml \
    --assets-dir /assets \
    --output-dir /output \
    $*
