#!/usr/bin/env bash

# Override this to use e.g. podman instead of docker:
DOCKER="${DOCKER:=docker}"

set -e
cd "$(dirname $0)"/..
"${DOCKER}" build . -t covid-19-puerto-rico