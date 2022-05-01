--
-- Normalize out the Health Service Area data from the
-- CDC Community Levels file.
--
SELECT
    file_timestamp,
    bulletin_date,
    date_updated,
    state,
    health_service_area_number,
    health_service_area,
    arbitrary(health_service_area_population)
        AS health_service_area_population,
    arbitrary(covid_inpatient_bed_utilization)
        AS covid_inpatient_bed_utilization,
    arbitrary(covid_hospital_admissions_per_100k)
        AS covid_hospital_admissions_per_100k
FROM {{ ref('cdc_community_level_all') }}
GROUP BY
    file_timestamp,
    bulletin_date,
    date_updated,
    state,
    health_service_area_number,
    health_service_area
ORDER BY
    file_timestamp,
    date_updated,
    state;