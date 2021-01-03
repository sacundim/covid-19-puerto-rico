COPY hhs_hospital_history
FROM '/data/HHS/reported_hospital_capacity_admissions_facility_level_weekly_average_timeseries-PuertoRico.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';
ANALYZE VERBOSE hhs_hospital_history;
REFRESH MATERIALIZED VIEW hhs_hospital_history_municipal_cube;
ANALYZE VERBOSE hhs_hospital_history_municipal_cube;
