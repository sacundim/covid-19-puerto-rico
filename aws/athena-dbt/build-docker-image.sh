#!/usr/bin/env bash

set -e -x

# Override this to use e.g. podman instead of docker:
DOCKER="${DOCKER:=docker}"
DOCKER_IMAGE="${DOCKER_IMAGE:=covid-19-puerto-rico-dbt}"

cd "$(dirname $0)"
exec "${DOCKER}" build -t "${DOCKER_IMAGE}" .