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
ORDER BY "$path", state, date
