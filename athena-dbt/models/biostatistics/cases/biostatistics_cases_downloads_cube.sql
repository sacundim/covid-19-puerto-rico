--
-- The "downloads cube" has aggregates for all the file downloads,
-- which may be more than one for each bulletin_date.  This is not
-- what we usually want but the upside is we can update it incrementally
--
{{
    config(
        table_type='iceberg',
        partitioned_by=['month(downloaded_at)'],
        materialized='incremental',
        incremental_strategy='append',
        post_hook = [
            'VACUUM {{ this.render_pure() }};'
        ]
    )
}}
SELECT
    downloaded_at,
	bulletin_date,
	diagnostic_collected_date collected_date,
	age_gte,
	age_lt,
	municipality,
	count(*) cases,
	count(*) FILTER (
		WHERE case_type = 'Probable'
	) AS probable_cases,
	count(*) FILTER (
		WHERE case_type = 'Confirmed'
	) AS confirmed_cases,
	count(*) FILTER (
		WHERE case_classification = 'Initial'
	) AS initial_cases,
	count(*) FILTER (
		WHERE case_classification = 'Reinfection'
	) AS reinfections
FROM {{ ref('biostatistics_cases') }} cases
LEFT OUTER JOIN {{ ref('biostatistics_deaths_age_ranges') }} ranges
	USING (age_range)
WHERE case_category IN ('Covid19')
AND case_type IN ('Probable', 'Confirmed')
AND diagnostic_collected_date <= bulletin_date
{% if is_incremental() %}
AND downloaded_at > (SELECT max(downloaded_at) FROM {{ this }})
{% endif %}
GROUP BY
    downloaded_at,
	bulletin_date,
	diagnostic_collected_date,
	age_gte,
	age_lt,
	municipality;