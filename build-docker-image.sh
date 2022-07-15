#!/usr/bin/env bash
#
# Build the docker image for the download scripts.
#
set -e -o pipefail

#PLATFORMS="linux/amd64"

# Some day:
PLATFORMS="linux/amd64,linux/arm64"

IMAGE_NAME="${IMAGE_NAME-covid-19-puerto-rico-downloader}"
cd "$(dirname $0)"
docker buildx build \
  -t "${IMAGE_NAME}" \
  --platform "${PLATFORMS}" \
  .