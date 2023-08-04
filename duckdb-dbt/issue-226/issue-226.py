#!/usr/bin/env python3

from dbt.adapters.duckdb.credentials import DuckDBCredentials
from dbt.adapters.duckdb.environments.local import LocalEnvironment

def main():
    payload = {
        "extensions": ['httpfs', 'parquet'],
        "use_credential_provider": "aws"
    }
    credentials = DuckDBCredentials.from_dict(payload)
    environment = LocalEnvironment(credentials)
    for i in range(2):
        print(f"Round {i+1}")
        run_with_environment(environment)

def run_with_environment(environment):
    handle = environment.handle()
    cursor = handle.cursor()
    run_with_cursor(cursor)

def run_with_cursor(cursor):
    cursor.execute("""
    CREATE OR REPLACE TABLE biostatistics_deaths AS
    WITH first_clean AS (
        SELECT
              CAST(downloaded_date AS DATE) AS downloaded_date,
          downloadedAt AS downloaded_at,
            CAST(downloadedAt AT TIME ZONE 'America/Puerto_Rico' AS DATE)
                - INTERVAL 1 DAY
                AS bulletin_date,
            CAST(deathId AS UUID) AS death_id,
          deathDate AS raw_death_date,
          deathReportDate AS raw_death_report_date,
            nullif(sex, '') sex,
            
        CASE ageRange
            WHEN '' THEN NULL
            WHEN 'N/A' THEN NULL
            ELSE ageRange
        END
     AS age_range,
            
        CASE physicalRegion
            WHEN '' THEN NULL
            WHEN 'N/A' THEN NULL
            WHEN 'Bayamon' THEN 'Bayamón'
            WHEN 'Mayaguez' THEN 'Mayagüez'
            ELSE physicalRegion
        END
     AS region,
            nullif(vaccinationStatusAtDeath, '')
                AS vaccination_status_at_death
        FROM 's3://covid-19-puerto-rico-data/biostatistics.salud.pr.gov/deaths/parquet_v2/*/*.parquet'
      WHERE '2023-08-01' <= downloaded_date
      AND downloaded_date <= '2023-08-03'
    )
    SELECT
        *,
        CASE
            WHEN raw_death_date < DATE '2020-01-01' AND month(raw_death_date) >= 3
            THEN CAST(strftime(raw_death_date, '2020-%m-%d') AS DATE)
            WHEN raw_death_date BETWEEN DATE '2020-01-01' AND DATE '2020-03-01'
            THEN raw_death_date + INTERVAL 1 YEAR
            ELSE raw_death_date
        END AS death_date,
        raw_death_report_date AS report_date
    FROM first_clean""")

    cursor.sql("""
    SELECT bulletin_date, count(*) 
    FROM biostatistics_deaths
    GROUP BY bulletin_date""").show()


if __name__ == "__main__":
    main()