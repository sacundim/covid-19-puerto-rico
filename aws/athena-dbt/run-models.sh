#!/usr/bin/env bash
#
# Run our DBT models
#

set -e -x

dbt test --select 'source:*'

dbt seed && dbt run

# TRICKY: We'd like to do this first but it depends on
# refreshing table partitions (MSCK REPAIR TABLE):
dbt source freshness

dbt test --exclude 'source:*'

# dbt docs generate