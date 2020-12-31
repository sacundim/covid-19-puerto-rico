#!/usr/bin/env bash
#
# DEPRECATED: The functionality of this script is now moved out
# to the two separate ones it calls.
#
set -eu -o pipefail

HERE="$(dirname $0)"
"${HERE}"/bulletin-s3-sync.sh
"${HERE}"/bioportal-s3-sync.sh