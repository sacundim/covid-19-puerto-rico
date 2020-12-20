#!/usr/bin/env bash
#
# One-off script to convert old minimal-info-unique-tests_V* csv files to Parquet.
# Uses this utility:
#
# * https://github.com/cldellow/csv2parquet
#

set -eu -o pipefail

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/../.."
S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"
TESTS_DIR="${S3_SYNC_DIR}/bioportal/minimal-info-unique-tests"

mkdir -p "${TESTS_DIR}"/parquet_v3

for file in "${TESTS_DIR}"/json_v3/minimal-info-unique-tests_*.json.bz2
do
  echo "$(date): converting ${file}..."
  name="$(basename file)"
  ts="$(echo "${name}" |sed -e 's/^minimal-info-unique-tests_\(.*\)\.json\.bz2$/\1/')"
  "${HERE}"/../bioportal-tests-to-csv.sh "${file}" "${ts}"\
    | time csv2parquet \
        --exclude patientid \
        --codec gzip \
        --row-group-size 10000000 \
        --output /dev/stdout \
        /dev/stdin \
    > "${TESTS_DIR}"/parquet_v3/"${name%.json.bz2}".parquet
done

