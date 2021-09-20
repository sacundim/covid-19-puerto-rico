--
-- Data dictionary here:
--
-- * https://healthdata.gov/covid-19-reported-patient-impact-and-hospital-capacity-facility-data-dictionary
--
CREATE TABLE hhs_hospital_history (
    -- Postgres has a maximum column name length of 63, but some of the
    -- field names in the CSV exceed that. So we've changed the names.
    "hospital_pk" TEXT NOT NULL,
    "collection_week" DATE NOT NULL,
    "state" CHAR(2) NOT NULL,
    "ccn" CHAR(6),
    "hospital_name" TEXT NOT NULL,
    "address" TEXT,
    "city" TEXT,
    "zip" CHAR(5) NOT NULL,
    "hospital_subtype" TEXT,
    "fips_code" CHAR(5),
    "is_metro_micro" BOOLEAN NOT NULL,
    "total_beds_7_day_avg" DOUBLE PRECISION,
    "all_adult_hospital_beds_7_day_avg" DOUBLE PRECISION,
    "all_adult_hospital_inpatient_beds_7_day_avg" DOUBLE PRECISION,
    "inpatient_beds_used_7_day_avg" DOUBLE PRECISION,
    "all_adult_hospital_inpatient_bed_occupied_7_day_avg" DOUBLE PRECISION,
    -- Original name: "total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg"
    "total_adult_patients_hospitalized_covid_7_day_avg" DOUBLE PRECISION,
    "total_adult_patients_hospitalized_confirmed_covid_7_day_avg" DOUBLE PRECISION,
    -- Original name: "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_avg"
    "total_pediatric_patients_hospitalized_covid_7_day_avg" DOUBLE PRECISION,
    "total_pediatric_patients_hospitalized_confirmed_covid_7_day_avg" DOUBLE PRECISION,
    "inpatient_beds_7_day_avg" DOUBLE PRECISION,
    "total_icu_beds_7_day_avg" DOUBLE PRECISION,
    "total_staffed_adult_icu_beds_7_day_avg" DOUBLE PRECISION,
    "icu_beds_used_7_day_avg" DOUBLE PRECISION,
    "staffed_adult_icu_bed_occupancy_7_day_avg" DOUBLE PRECISION,
    -- Original name: "staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_avg"
    "staffed_icu_adult_patients_covid_7_day_avg" DOUBLE PRECISION,
    "staffed_icu_adult_patients_confirmed_covid_7_day_avg" DOUBLE PRECISION,
    "total_patients_hospitalized_confirmed_influenza_7_day_avg" DOUBLE PRECISION,
    "icu_patients_confirmed_influenza_7_day_avg" DOUBLE PRECISION,
    -- Original name: "total_patients_hospitalized_confirmed_influenza_and_covid_7_day_avg"
    "total_patients_hospitalized_confirmed_both_7_day_avg" DOUBLE PRECISION,
    "total_beds_7_day_sum" INTEGER,
    "all_adult_hospital_beds_7_day_sum" INTEGER,
    "all_adult_hospital_inpatient_beds_7_day_sum" INTEGER,
    "inpatient_beds_used_7_day_sum" INTEGER,
    "all_adult_hospital_inpatient_bed_occupied_7_day_sum" INTEGER,
    -- Original name: "total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum"
    "total_adult_patients_hospitalized_covid_7_day_sum" INTEGER,
    "total_adult_patients_hospitalized_confirmed_covid_7_day_sum" INTEGER,
    -- Original name: "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_sum"
    "total_pediatric_patients_hospitalized_covid_7_day_sum" INTEGER,
    "total_pediatric_patients_hospitalized_confirmed_covid_7_day_sum" INTEGER,
    "inpatient_beds_7_day_sum" INTEGER,
    "total_icu_beds_7_day_sum" INTEGER,
    "total_staffed_adult_icu_beds_7_day_sum" INTEGER,
    "icu_beds_used_7_day_sum" INTEGER,
    "staffed_adult_icu_bed_occupancy_7_day_sum" INTEGER,
    -- Original name: "staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_sum"
    "staffed_icu_adult_patients_covid_7_day_sum" INTEGER,
    "staffed_icu_adult_patients_confirmed_covid_7_day_sum" INTEGER,
    "total_patients_hospitalized_confirmed_influenza_7_day_sum" INTEGER,
    "icu_patients_confirmed_influenza_7_day_sum" INTEGER,
    -- Original name: "total_patients_hospitalized_confirmed_influenza_and_covid_7_day_sum"
    "total_patients_hospitalized_confirmed_both_7_day_sum" INTEGER,
    "total_beds_7_day_coverage" INTEGER,
    "all_adult_hospital_beds_7_day_coverage" INTEGER,
    "all_adult_hospital_inpatient_beds_7_day_coverage" INTEGER,
    "inpatient_beds_used_7_day_coverage" INTEGER,
    "all_adult_hospital_inpatient_bed_occupied_7_day_coverage" INTEGER,
    -- Original name: "total_adult_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage"
    "total_adult_patients_hospitalized_covid_7_day_coverage" INTEGER,
    "total_adult_patients_hospitalized_confirmed_covid_7_day_coverage" INTEGER,
    -- Original name: "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_7_day_coverage"
    "total_pediatric_patients_hospitalized_covid_7_day_coverage" INTEGER,
    "total_pediatric_patients_hospitalized_confirmed_covid_7_day_coverage" INTEGER,
    "inpatient_beds_7_day_coverage" INTEGER,
    "total_icu_beds_7_day_coverage" INTEGER,
    "total_staffed_adult_icu_beds_7_day_coverage" INTEGER,
    "icu_beds_used_7_day_coverage" INTEGER,
    "staffed_adult_icu_bed_occupancy_7_day_coverage" INTEGER,
    -- Original name: staffed_icu_adult_patients_confirmed_and_suspected_covid_7_day_coverage
    "staffed_icu_adult_patients_covid_7_day_coverage" INTEGER,
    "staffed_icu_adult_patients_confirmed_covid_7_day_coverage" INTEGER,
    "total_patients_hospitalized_confirmed_influenza_7_day_coverage" INTEGER,
    "icu_patients_confirmed_influenza_7_day_coverage" INTEGER,
    -- Original name: "total_patients_hospitalized_confirmed_influenza_and_covid_7_day_coverage"
    "total_patients_hospitalized_confirmed_both_7_day_coverage" INTEGER,
    "previous_day_admission_adult_covid_confirmed_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_confirmed_18-19_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_confirmed_20-29_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_confirmed_30-39_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_confirmed_40-49_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_confirmed_50-59_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_confirmed_60-69_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_confirmed_70-79_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_confirmed_80+_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_confirmed_unknown_7_day_sum" INTEGER,
    "previous_day_admission_pediatric_covid_confirmed_7_day_sum" INTEGER,
    "previous_day_covid_ED_visits_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_suspected_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_suspected_18-19_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_suspected_20-29_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_suspected_30-39_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_suspected_40-49_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_suspected_50-59_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_suspected_60-69_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_suspected_70-79_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_suspected_80+_7_day_sum" INTEGER,
    "previous_day_admission_adult_covid_suspected_unknown_7_day_sum" INTEGER,
    "previous_day_admission_pediatric_covid_suspected_7_day_sum" INTEGER,
    "previous_day_total_ED_visits_7_day_sum" INTEGER,
    "previous_day_admission_influenza_confirmed_7_day_sum" INTEGER,
    "geocoded_hospital_address" TEXT,
    -- Added on May 3, 2021:
    "hhs_ids" TEXT,
    "previous_day_admission_adult_covid_confirmed_7_day_coverage" INTEGER,
    "previous_day_admission_pediatric_covid_confirmed_7_day_coverage" INTEGER,
    "previous_day_admission_adult_covid_suspected_7_day_coverage" INTEGER,
    "previous_day_admission_pediatric_covid_suspected_7_day_coverage" INTEGER,
    "previous_week_personnel_covid_vaccinated_doses_administered_7_day_max" INTEGER,
    "total_personnel_covid_vaccinated_doses_none_7_day_min" INTEGER,
    "total_personnel_covid_vaccinated_doses_one_7_day_max" INTEGER,
    "total_personnel_covid_vaccinated_doses_all_7_day_max" INTEGER,
    "previous_week_patients_covid_vaccinated_doses_one_7_day_max" INTEGER,
    "previous_week_patients_covid_vaccinated_doses_all_7_day_max" INTEGER,
    "is_corrected" BOOLEAN,
    PRIMARY KEY ("hospital_pk", "collection_week")
);


--
-- For privacy reasons, the HHS data set puts -999999 for values
-- that are less than four, but still reports zeroes as zeroes.
-- They do this superficially for the `*_7_day_avg` columns as for
-- the `*_7_day_sum`, which means that you can get more precise
-- averages by not using `*_7_day_avg` at all and instead dividing
-- the `*_7_day_sum` by the `*_7_day_coverage` (number of days the
-- facility reported in that week).
--
-- By imputing 0.0 and 4.0 respectively we can also obtain a lower
-- and upper bound for omitted sums, and we provide functions for
-- that as well.
--
CREATE FUNCTION hhs_avg(sum INTEGER, coverage INTEGER)
RETURNS DOUBLE PRECISION AS $$
    SELECT nullif(sum, -999999) :: DOUBLE PRECISION / coverage;
$$ LANGUAGE SQL;

CREATE FUNCTION hhs_lo(sum INTEGER, coverage INTEGER)
RETURNS DOUBLE PRECISION AS $$
    SELECT CASE WHEN sum = -999999
            THEN 0.0
            ELSE sum :: DOUBLE PRECISION / coverage
           END;
$$ LANGUAGE SQL;

CREATE FUNCTION hhs_hi(sum INTEGER, coverage INTEGER)
RETURNS DOUBLE PRECISION AS $$
    SELECT CASE WHEN sum = -999999
            THEN 4.0
            ELSE sum :: DOUBLE PRECISION / coverage
           END;
$$ LANGUAGE SQL;

CREATE FUNCTION estimate_hi(x DOUBLE PRECISION, y DOUBLE PRECISION)
RETURNS DOUBLE PRECISION AS $$
    SELECT CASE WHEN x = -999999 THEN y ELSE x END;
$$ LANGUAGE SQL;


CREATE MATERIALIZED VIEW hhs_hospital_history_cube AS
SELECT
	collection_week AS week_start,
	collection_week + 6 AS week_end,
	collection_week + 7 AS next_week,
	cmn.region region,
	cmn."name" municipality,
	fips_code,
	hospital_name,
	hospital_pk,

	hhs_avg(all_adult_hospital_inpatient_beds_7_day_sum,
	        all_adult_hospital_inpatient_beds_7_day_coverage)
		AS all_adult_hospital_inpatient_beds_7_day_avg,
	hhs_lo(all_adult_hospital_inpatient_beds_7_day_sum,
	       all_adult_hospital_inpatient_beds_7_day_coverage)
		AS all_adult_hospital_inpatient_beds_7_day_lo,
	hhs_hi(all_adult_hospital_inpatient_beds_7_day_sum,
	       all_adult_hospital_inpatient_beds_7_day_coverage)
		AS all_adult_hospital_inpatient_beds_7_day_hi,

	hhs_avg(all_adult_hospital_inpatient_bed_occupied_7_day_sum,
	        all_adult_hospital_inpatient_bed_occupied_7_day_coverage)
		AS all_adult_hospital_inpatient_bed_occupied_7_day_avg,
	hhs_lo(all_adult_hospital_inpatient_bed_occupied_7_day_sum,
	       all_adult_hospital_inpatient_bed_occupied_7_day_coverage)
		AS all_adult_hospital_inpatient_bed_occupied_7_day_lo,
	hhs_hi(all_adult_hospital_inpatient_bed_occupied_7_day_sum,
	       all_adult_hospital_inpatient_bed_occupied_7_day_coverage)
		AS all_adult_hospital_inpatient_bed_occupied_7_day_hi,

	hhs_avg(total_adult_patients_hospitalized_covid_7_day_sum,
	        total_adult_patients_hospitalized_covid_7_day_coverage)
		AS total_adult_patients_hospitalized_covid_7_day_avg,
	hhs_lo(total_adult_patients_hospitalized_covid_7_day_sum,
	       total_adult_patients_hospitalized_covid_7_day_coverage)
		AS total_adult_patients_hospitalized_covid_7_day_lo,
	hhs_hi(total_adult_patients_hospitalized_covid_7_day_sum,
	       total_adult_patients_hospitalized_covid_7_day_coverage)
		AS total_adult_patients_hospitalized_covid_7_day_hi,

	hhs_avg(total_adult_patients_hospitalized_confirmed_covid_7_day_sum,
	        total_adult_patients_hospitalized_confirmed_covid_7_day_coverage)
		AS total_adult_patients_hospitalized_confirmed_covid_7_day_avg,
	hhs_lo(total_adult_patients_hospitalized_confirmed_covid_7_day_sum,
	       total_adult_patients_hospitalized_confirmed_covid_7_day_coverage)
		AS total_adult_patients_hospitalized_confirmed_covid_7_day_lo,
	hhs_hi(total_adult_patients_hospitalized_confirmed_covid_7_day_sum,
	       total_adult_patients_hospitalized_confirmed_covid_7_day_coverage)
		AS total_adult_patients_hospitalized_confirmed_covid_7_day_hi,

	hhs_avg(total_staffed_adult_icu_beds_7_day_sum,
	        total_staffed_adult_icu_beds_7_day_coverage)
		AS total_staffed_adult_icu_beds_7_day_avg,
	hhs_lo(total_staffed_adult_icu_beds_7_day_sum,
	       total_staffed_adult_icu_beds_7_day_coverage)
		AS total_staffed_adult_icu_beds_7_day_lo,
	hhs_hi(total_staffed_adult_icu_beds_7_day_sum,
	       total_staffed_adult_icu_beds_7_day_coverage)
		AS total_staffed_adult_icu_beds_7_day_hi,

	hhs_avg(staffed_adult_icu_bed_occupancy_7_day_sum,
	        staffed_adult_icu_bed_occupancy_7_day_coverage)
		AS staffed_adult_icu_bed_occupancy_7_day_avg,
	hhs_lo(staffed_adult_icu_bed_occupancy_7_day_sum,
	       staffed_adult_icu_bed_occupancy_7_day_coverage)
		AS staffed_adult_icu_bed_occupancy_7_day_lo,
	hhs_hi(staffed_adult_icu_bed_occupancy_7_day_sum,
	       staffed_adult_icu_bed_occupancy_7_day_coverage)
		AS staffed_adult_icu_bed_occupancy_7_day_hi,

	hhs_avg(staffed_icu_adult_patients_covid_7_day_sum,
	        staffed_icu_adult_patients_covid_7_day_coverage)
		AS staffed_icu_adult_patients_covid_7_day_avg,
	hhs_lo(staffed_icu_adult_patients_covid_7_day_sum,
	       staffed_icu_adult_patients_covid_7_day_coverage)
		AS staffed_icu_adult_patients_covid_7_day_lo,
	hhs_hi(staffed_icu_adult_patients_covid_7_day_sum,
	       staffed_icu_adult_patients_covid_7_day_coverage)
		AS staffed_icu_adult_patients_covid_7_day_hi,

	hhs_avg(staffed_icu_adult_patients_confirmed_covid_7_day_sum,
	        staffed_icu_adult_patients_confirmed_covid_7_day_coverage)
		AS staffed_icu_adult_patients_confirmed_covid_7_day_avg,
	hhs_lo(staffed_icu_adult_patients_confirmed_covid_7_day_sum,
	       staffed_icu_adult_patients_confirmed_covid_7_day_coverage)
		AS staffed_icu_adult_patients_confirmed_covid_7_day_lo,
	hhs_hi(staffed_icu_adult_patients_confirmed_covid_7_day_sum,
	       staffed_icu_adult_patients_confirmed_covid_7_day_coverage)
		AS staffed_icu_adult_patients_confirmed_covid_7_day_hi

FROM hhs_hospital_history hhh
INNER JOIN canonical_municipal_names cmn
	USING (fips_code)
ORDER BY collection_week DESC, region, cmn."name", hospital_name;


CREATE VIEW products.icus_by_hospital AS
SELECT
	week_start,
	week_end,
	next_week,
	hospital_name,
	municipality,
	total_staffed_adult_icu_beds_7_day_lo,
	-- Occupied ICU beds can't be more than staffed ones:
	LEAST(staffed_adult_icu_bed_occupancy_7_day_hi,
		  total_staffed_adult_icu_beds_7_day_lo)
		AS staffed_adult_icu_bed_occupancy_7_day_hi,
	-- ICU COVID patients can't be more than either occupied
	-- or staffed beds:
	LEAST(staffed_icu_adult_patients_covid_7_day_hi,
		  staffed_adult_icu_bed_occupancy_7_day_hi,
		  total_staffed_adult_icu_beds_7_day_lo)
	  AS staffed_icu_adult_patients_covid_7_day_hi
FROM hhs_hospital_history_cube
ORDER BY week_start DESC, hospital_name;


CREATE VIEW products.icus_by_region AS
SELECT
	week_start,
	week_end,
	next_week,
	region,
	sum(total_staffed_adult_icu_beds_7_day_lo)
		AS total_staffed_adult_icu_beds_7_day_lo,
	sum(LEAST(staffed_adult_icu_bed_occupancy_7_day_hi,
		 	  total_staffed_adult_icu_beds_7_day_lo))
		AS staffed_adult_icu_bed_occupancy_7_day_hi,
	sum(LEAST(staffed_icu_adult_patients_covid_7_day_hi,
		      staffed_adult_icu_bed_occupancy_7_day_hi,
		      total_staffed_adult_icu_beds_7_day_lo))
	  AS staffed_icu_adult_patients_covid_7_day_hi
FROM hhs_hospital_history_cube
GROUP BY week_start, week_end, next_week, region
ORDER BY week_start DESC, region;
