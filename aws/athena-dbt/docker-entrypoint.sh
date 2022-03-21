#!/usr/bin/env bash
#
# Setup for DBT to work inside Docker.
#

# Must pass in the DBT `profiles.yml` file contents
# as an environment variable
DBT_PROFILES="${DBT_PROFILES?"No DBT_PROFILES given"}"


mkdir -p ~/.dbt
echo -n "${DBT_PROFILES}" >~/.dbt/profiles.yml
exec "$@"