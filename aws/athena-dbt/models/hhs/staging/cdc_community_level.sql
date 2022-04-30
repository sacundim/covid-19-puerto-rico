--
-- The CDC's Community Levels (a.k.a. the "green map") dataset.
--
SELECT *
FROM {{ ref('cdc_community_level_all') }}
WHERE state = 'Puerto Rico'
ORDER BY file_timestamp, date_updated, county_fips;