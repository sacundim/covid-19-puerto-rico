#!/usr/bin/env bash
#
# Run our DBT models
#

set -euxo pipefail

exec dbt build "$@"