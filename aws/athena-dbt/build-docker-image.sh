#!/usr/bin/env bash

set -eux -o pipefail

PLATFORMS="linux/amd64,linux/arm64"
IMAGE_NAME="${IMAGE_NAME-docker.io/sacundim/covid-19-puerto-rico-dbt}"

cd "$(dirname $0)"
exec docker buildx build \
  -t "${IMAGE_NAME}" \
  --platform "${PLATFORMS}" \
  . \
  "$@"
