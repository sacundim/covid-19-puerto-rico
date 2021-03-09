#!/usr/bin/env bash
#
# Run a HHS download and sync in sequence.
set -eu -o pipefail

HERE="$(dirname $0)"
"${HERE}"/hhs-download.sh
"${HERE}"/hhs-s3-sync.sh