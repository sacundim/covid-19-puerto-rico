#!/usr/bin/env bash
#
# Download the current data sets from here and extract Puerto Rico:
#
# * https://beta.healthdata.gov/Hospital/COVID-19-Reported-Patient-Impact-and-Hospital-Capa/anag-cw7u
# * https://dev.socrata.com/foundry/beta.healthdata.gov/anag-cw7u
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

CSV_URL='https://beta.healthdata.gov/api/views/anag-cw7u/rows.csv?accessType=DOWNLOAD&api_foundry=true'

wget --compress=gzip -O - "${CSV_URL}" \
  | xsv search --select state '^PR$' \
  > "${TMP}"/reported_hospital_capacity_admissions_facility_level_weekly_average_timeseries-PuertoRico.csv

mv "${TMP}"/reported_hospital_capacity_admissions_facility_level_weekly_average_timeseries-PuertoRico.csv \
  "${REPO_ROOT}"/assets/data/HHS/