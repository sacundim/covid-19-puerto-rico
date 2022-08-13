#!/usr/bin/env bash

set -e
set -o pipefail

downloadedAt="${1:?"No downloadedAt argument given"}"
file="${2:?"No argument file given"}"

cat "${file}" \
    | bunzip2 \
    | jq -e -c \
        --arg downloadedAt "${downloadedAt}" \
        '.[] | . + {downloadedAt: $downloadedAt}'
