#!/usr/bin/env bash
#
# Run our DBT models
#

set -e -x

dbt test --select 'source:*'

# The DBT Athena third party plugin is janky, and the seed
# fails when there's more than 1 thread.
dbt seed --threads 1

dbt run

# TRICKY: We'd like to do this first but it depends on
# refreshing table partitions (MSCK REPAIR TABLE):
dbt source freshness

dbt test --exclude 'source:*'

dbt docs generate