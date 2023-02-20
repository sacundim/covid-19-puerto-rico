#!/usr/bin/env bash

# Override this to use e.g. podman instead of docker:
DOCKER="${DOCKER:=docker}"

export DOCKER_IMAGE="${DOCKER_IMAGE:=covid-19-puerto-rico-website}"

set -e
cd "$(dirname $0)"/..
"${DOCKER}" build . -t "${DOCKER_IMAGE}"