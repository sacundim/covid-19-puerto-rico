SELECT
    {{ hhs_parse_filename_date('"$path"') }}
		AS file_timestamp,
	lpad(fips, 5, '0') AS fips,
	county,
	state,
    fema_region,
	date(date_parse(date, '%m/%d/%Y %r'))
	    AS date,
	{{ cast_string_column('cases_last_7_days', 'INT') }},
	{{ cast_string_column('cases_per_100k_last_7_days', 'DOUBLE') }},
	{{ cast_string_column('total_cases', 'INT') }},
    {{ cast_string_column('cases_pct_change_from_prev_week', 'DOUBLE') }},
	{{ cast_string_column('deaths_last_7_days', 'INT') }},
	{{ cast_string_column('deaths_per_100k_last_7_days', 'DOUBLE') }},
	{{ cast_string_column('total_deaths', 'INT') }},
    {{ cast_string_column('deaths_pct_change_from_prev_week', 'DOUBLE') }},
    {{ cast_string_column('test_positivity_rate_last_7_days', 'DOUBLE') }},
	{{ cast_string_column('total_positive_tests_last_7_days', 'INT') }},
	{{ cast_string_column('total_tests_last_7_days', 'INT') }},
    {{ cast_string_column('total_tests_per_100k_last_7_days', 'DOUBLE') }},
    {{ cast_string_column('test_positivity_rate_pct_change_from_prev_week', 'DOUBLE') }},
    {{ cast_string_column('total_tests_pct_change_from_prev_week', 'DOUBLE') }},

    -- HHS doesn't document the semantics of this column, and it's **baroque**.  I reverse
    -- engineered it, however:
    --
    -- 1. Take the sum of total hospital admissions in the 7 day period for all of the
    --    counties in the focal county's Health Service Area (as used in the CDC Community
    --    Level report);
    -- 2. Compute, for each county, its population divided by that of its HSA;
    --
    -- ...and then use the product of these two as the value for the column.  This means
    -- the count-level detail is spurious--the only information really contained in this
    -- dataset is HSA-level values.
    --
    -- The `{{ ref('cdc_community_level_all') }}` table has the values for county and HSA
    -- population, so joining with that we can get:
    --
    --     HSA admissions (7-day sum):
    --         confirmed_covid_hosp_last_7_days
    --             * health_service_area_population
    --             / county_population
    --
    --     HSA admissions per 100k (7-day sum):
    --         1e5 * confirmed_covid_hosp_last_7_days
    --             / county_population
    --
	{{ cast_string_column('confirmed_covid_hosp_last_7_days', 'DOUBLE') }},

    {{ cast_string_column('confirmed_covid_hosp_per_100_beds_last_7_days', 'DOUBLE') }},
    {{ cast_string_column('confirmed_covid_hosp_per_100_beds_pct_change_from_prev_week', 'DOUBLE') }},

    -- This has the same baroque semantics as `confirmed_covid_hosp_last_7_days`:
	{{ cast_string_column('suspected_covid_hosp_last_7_days', 'DOUBLE') }},

    {{ cast_string_column('suspected_covid_hosp_per_100_beds_last_7_days', 'DOUBLE') }},
    {{ cast_string_column('suspected_covid_hosp_per_100_beds_pct_change_from_prev_week', 'DOUBLE') }},
    {{ cast_string_column('pct_inpatient_beds_used_avg_last_7_days', 'DOUBLE') }},
    {{ cast_string_column('pct_inpatient_beds_used_abs_change_from_prev_week', 'DOUBLE') }},
    {{ cast_string_column('pct_inpatient_beds_used_covid_avg_last_7_days', 'DOUBLE') }},
    {{ cast_string_column('pct_inpatient_beds_used_covid_abs_change_from_prev_week', 'DOUBLE') }},
    {{ cast_string_column('pct_icu_beds_used_avg_last_7_days', 'DOUBLE') }},
    {{ cast_string_column('pct_icu_beds_used_abs_change_from_prev_week', 'DOUBLE') }},
    {{ cast_string_column('pct_icu_beds_used_covid_avg_last_7_days', 'DOUBLE') }},
    {{ cast_string_column('pct_icu_beds_used_covid_abs_change_from_prev_week', 'DOUBLE') }},
    {{ cast_string_column('pct_vents_used_avg_last_7_days', 'DOUBLE') }},
    {{ cast_string_column('pct_vents_used_abs_change_from_prev_week', 'DOUBLE') }},
    {{ cast_string_column('pct_vents_used_covid_avg_last_7_days', 'DOUBLE') }},
    {{ cast_string_column('pct_vents_used_covid_abs_change_from_prev_week', 'DOUBLE') }}
FROM {{ source('hhs', 'community_profile_report_county') }}