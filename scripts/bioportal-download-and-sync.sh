#!/usr/bin/env bash
#
# Run a Bioportal download and sync in sequence.
set -eu -o pipefail

HERE="$(dirname $0)"
"${HERE}"/bioportal-download.sh
"${HERE}"/bioportal-s3-sync.sh