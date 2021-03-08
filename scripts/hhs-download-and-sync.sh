#!/usr/bin/env bash
#
# Run a HHS download and sync in sequence.
set -eu -o pipefail

HERE="$(dirname $0)"
cd /covid-19-puerto-rico
hhs-downloader \
  --s3-sync-dir s3-bucket-sync/covid-19-puerto-rico-data
"${HERE}"/hhs-s3-sync.sh