#!/usr/bin/env bash
#
# Download the current data sets from here and extract Puerto Rico:
#
# * https://healthdata.gov/dataset/covid-19-reported-patient-impact-and-hospital-capacity-facility
# * https://healthdata.gov/dataset/covid-19-reported-patient-impact-and-hospital-capacity-state-timeseries
#
set -eu -o pipefail

HERE="$(dirname $0)"
REPO_ROOT="${HERE}/.."
TMP="${REPO_ROOT}/tmp"


##################################################################
##################################################################
#
# Facilities data set
#

if [ $# -eq 0 ]
then
  # This is a JSON document from which we can fetch the URL
  # of the download, which changes every week.
  JSON_URL="https://healthdata.gov/api/3/action/package_show?id=d475cc4e-83cd-4c16-be57-9105f300e0bc&page=0"
  CSV_URL="$(wget -O - "${JSON_URL}" |jq -r '.result[0].resources[0].url')"
else
  CSV_URL="${1}"
fi

wget --compress=gzip -O - "${CSV_URL}" \
  | xsv search --select state '^PR$' \
  > "${TMP}"/reported_hospital_capacity_admissions_facility_level_weekly_average_timeseries-PuertoRico.csv


##################################################################
##################################################################
#
# State time series data set
#

STATE_JSON_URL="https://healthdata.gov/api/3/action/package_show?id=83b4a668-9321-4d8c-bc4f-2bef66c49050&page=0"
CSV_URL="$(wget -O - "${STATE_JSON_URL}" |jq -r '.result[0].resources[0].url')"

wget --compress=gzip -O - "${CSV_URL}" \
  | xsv search --select state '^PR$' \
  > "${TMP}"/reported_hospital_utilization_timeseries-PuertoRico.csv
