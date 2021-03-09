#!/usr/bin/env bash
#
# Daily download script for latest HHS datasets. We get these:
#
# * https://healthdata.gov/dataset/covid-19-estimated-patient-impact-and-hospital-capacity-state
# * https://healthdata.gov/dataset/covid-19-diagnostic-laboratory-testing-pcr-testing-time-series
# * https://healthdata.gov/dataset/covid-19-reported-patient-impact-and-hospital-capacity-state
# * https://healthdata.gov/dataset/covid-19-reported-patient-impact-and-hospital-capacity-state-timeseries
# * https://healthdata.gov/dataset/covid-19-reported-patient-impact-and-hospital-capacity-facility

# These are API endpoints that produce JSON that has the URL for the most recent file.
ENDPOINTS=(
"https://healthdata.gov/api/3/action/package_show?id=060e4acc-241d-4d19-a929-f5f7b653c648"
"https://healthdata.gov/api/3/action/package_show?id=c13c00e3-f3d0-4d49-8c43-bf600a6c0a0d"
"https://healthdata.gov/api/3/action/package_show?id=7823dd0e-c8c4-4206-953e-c6d2f451d6ed"
"https://healthdata.gov/api/3/action/package_show?id=83b4a668-9321-4d8c-bc4f-2bef66c49050"
"https://healthdata.gov/api/3/action/package_show?id=d475cc4e-83cd-4c16-be57-9105f300e0bc"
)

set -eu -o pipefail

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
TMP="${REPO_ROOT}/tmp"

S3_SYNC_DIR="${REPO_ROOT}/s3-bucket-sync/covid-19-puerto-rico-data"
HHS_SYNC_DIR="${S3_SYNC_DIR}/HHS"

mkdir -p "${TMP}" "${HHS_SYNC_DIR}"
cd "${TMP}"

for endpoint in ${ENDPOINTS[@]}
do
  echo "$(date): Fetching metadata from ${endpoint}"
  csv_urls="$(wget -O - "${endpoint}" |jq -r '.result[].resources[].url')"
  echo "$(date): Got these URLs: ${csv_urls}"
  for csv_url in ${csv_urls}
  do
    echo "$(date): Downloading $(basename "${csv_url}") from ${csv_url}..."
    time wget --no-verbose "${csv_url}"

    filename="$(basename "${csv_url}")"

    echo "$(date): Converting ${filename} to Parquet..."
    time csv2parquet \
        --codec gzip \
        --row-group-size 10000000 \
        "${filename}"

    echo "$(date): Compressing ${filename}..."
    time bzip2 -9 "${filename}"

    syncdir="$(echo -n "${filename}" |sed -E 's/^([a-z19_\-]+)_202[012].*$/\1/')"
    echo "$(date): Moving ${filename}.bz2 to sync directory ${HHS_SYNC_DIR}/${syncdir}..."
    mkdir -p \
      "${HHS_SYNC_DIR}/${syncdir}" \
      "${HHS_SYNC_DIR}/${syncdir}/csv" \
      "${HHS_SYNC_DIR}/${syncdir}/parquet"
    mv "${filename}".bz2 "${HHS_SYNC_DIR}/${syncdir}"/csv/
    mv "${filename%.csv}".parquet "${HHS_SYNC_DIR}/${syncdir}"/parquet/
  done
done

echo "$(date): All done!"