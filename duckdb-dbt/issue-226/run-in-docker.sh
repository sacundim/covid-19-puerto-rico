#!/usr/bin/env bash
set -euxo pipefail

# Override to use a different image name or tag
IMAGE_NAME="${IMAGE_NAME-docker.io/sacundim/dbt-duckdb-issue-226}"

# Override this to use e.g. podman instead of docker:
DOCKER="${DOCKER-docker}"

cd "$(dirname $0)"
PLATFORM="linux" ./build-docker-image.sh --load

time "${DOCKER}" run --rm \
  -v ~/.aws:/awsconfig:ro \
  --env AWS_CONFIG_FILE=/awsconfig/config \
  --env AWS_SHARED_CREDENTIALS_FILE=/awsconfig/credentials \
  "${IMAGE_NAME}" "$@"
