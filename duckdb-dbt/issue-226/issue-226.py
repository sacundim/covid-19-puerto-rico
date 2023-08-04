#!/usr/bin/env python3

import dbt.adapters.duckdb.credentials as creds
print(creds._load_aws_credentials())