#!/usr/bin/env bash
set -euxo pipefail

# Override to use a different image name or tag
IMAGE_NAME="${IMAGE_NAME-docker.io/sacundim/covid-19-puerto-rico-dbt}"

# Override to just build one platform.  Tip: `PLATFORM="linux"` builds for
# the current host's processor architecture
PLATFORM="${PLATFORM-linux/amd64,linux/arm64}"

# Override this to use e.g. podman instead of docker:
DOCKER="${DOCKER-docker}"


cd "$(dirname $0)"
"${DOCKER}" buildx build \
  -t "${IMAGE_NAME}" \
  --platform="${PLATFORM}" \
  . \
  "$@"