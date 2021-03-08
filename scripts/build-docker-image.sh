#!/usr/bin/env bash
#
# Build the docker image for the download scripts.
#
set -e -o pipefail

IMAGE_NAME="${IMAGE_NAME-covid-19-puerto-rico-downloader}"
cd "$(dirname $0)"/..
docker build -t "${IMAGE_NAME}" .