#!/usr/bin/env bash
#
# Run our DBT models
#

set -euxo pipefail

dbt seed

dbt run

dbt test
