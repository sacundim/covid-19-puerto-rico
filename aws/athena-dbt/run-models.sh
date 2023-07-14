#!/usr/bin/env bash
#
# Run our DBT models
#

set -euxo pipefail

dbt seed

dbt run

dbt test

# TRICKY: We'd like to do this first but it depends on
# refreshing table partitions (MSCK REPAIR TABLE):
dbt source freshness