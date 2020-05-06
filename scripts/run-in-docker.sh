#!/usr/bin/env bash

set -e -x

cd "$(dirname $0)"/..
docker run --rm \
  --network=covid-19-puerto-rico_default \
  -v "$(pwd)"/config:/config:ro \
  -v "$(pwd)"/output:/output:rw \
  covid-19-puerto-rico \
    --config-file /config/docker.toml \
    --output-dir /output \
    --output-format json \
    --output-format png \
    $*
