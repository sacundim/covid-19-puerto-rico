{{
    config(
        pre_hook=[
            "MSCK REPAIR TABLE {{ source('walgreens', 'walgreens_tracker_parquet_v1').render_hive() }}",
            "MSCK REPAIR TABLE {{ source('walgreens', 'walgreens_tracker_parquet_v2').render_hive() }}",
            "MSCK REPAIR TABLE {{ source('walgreens', 'walgreens_tracker_parquet_v3').render_hive() }}"
        ]
    )
}}
SELECT
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
	state,
	date(from_unixtime(cast(date AS BIGINT) / 1000)) AS date,
    {{ cast_string_column('3day_mvPreOther_nmrtr', 'INT') }},
    {{ cast_string_column('3day_mvPreOmiBA2_nmrtr', 'INT') }},
    {{ cast_string_column('3day_mvPreOmiBA11_nmrtr', 'INT') }},
    {{ cast_string_column('3day_mov_avgPreOmiBA2', 'DOUBLE') }},
    {{ cast_string_column('3day_mov_avgPreOmiBA11', 'DOUBLE') }},
    {{ cast_string_column('3day_mov_avgPreOther', 'DOUBLE') }}
FROM {{ source('walgreens', 'walgreens_tracker_parquet_v1') }}
WHERE "3day_mov_avgPreOmiBA2" != ''
OR "3day_mov_avgPreOmiBA11" != ''
OR "3day_mov_avgPreOther" != ''

UNION ALL
SELECT
    {{ parse_filename_timestamp('"$path"') }}
		AS downloaded_at,
	state,
	date(from_unixtime(date / 1000)) AS date,
    "3day_mvPreOther_nmrtr",
    "3day_mvPreOmiBA2_nmrtr",
    "3day_mvPreOmiBA11_nmrtr",
    "3day_mov_avgPreOmiBA2",
    "3day_mov_avgPreOmiBA11",
    "3day_mov_avgPreOther"
FROM {{ source('walgreens', 'walgreens_tracker_parquet_v2') }}
WHERE "3day_mov_avgPreOmiBA2" IS NOT NULL
OR "3day_mov_avgPreOmiBA11" IS NOT NULL
OR "3day_mov_avgPreOther" IS NOT NULL

UNION ALL
SELECT
    CAST(downloaded_at AS TIMESTAMP(6))
        AS downloaded_at,
	state,
	date(date) AS date,
    "3day_mvPreOther_nmrtr",
    "3day_mvPreOmiBA2_nmrtr",
    "3day_mvPreOmiBA11_nmrtr",
    "3day_mov_avgPreOmiBA2",
    "3day_mov_avgPreOmiBA11",
    "3day_mov_avgPreOther"
FROM {{ source('walgreens', 'walgreens_tracker_parquet_v3') }}
WHERE "3day_mov_avgPreOmiBA2" IS NOT NULL
OR "3day_mov_avgPreOmiBA11" IS NOT NULL
OR "3day_mov_avgPreOther" IS NOT NULL
