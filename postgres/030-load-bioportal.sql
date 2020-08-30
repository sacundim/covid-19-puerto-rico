SET datestyle = 'ISO, MDY';

COPY bioportal_tests (
    downloaded_at,
    raw_collected_date,
    raw_reported_date,
    age_range,
    test_type,
    result,
    raw_municipality,
    created_at
)
FROM PROGRAM 'for file in $(ls /data/bioportal/v1/minimal-info-unique-tests_V1_*.csv.bz2); do (cat "${file}" |bunzip2 |tail -n+2); done'
    CSV ENCODING 'UTF-8' NULL '';

COPY bioportal_tests (
    downloaded_at,
    patient_id,
    raw_collected_date,
    raw_reported_date,
    age_range,
    test_type,
    result,
    raw_municipality,
    created_at
)
FROM PROGRAM 'for file in $(ls /data/bioportal/v2/minimal-info-unique-tests_V2_*.csv.bz2); do (cat "${file}" |bunzip2 |tail -n+2); done'
    CSV ENCODING 'UTF-8' NULL '';

SET maintenance_work_mem='2GB';
CREATE INDEX ON bioportal_tests (downloaded_at, test_type, reported_date, collected_date, positive);
CREATE INDEX ON bioportal_tests (downloaded_at, test_type, collected_date, reported_date, positive);
ANALYZE VERBOSE bioportal_tests;

REFRESH MATERIALIZED VIEW bioportal_tritemporal_counts;
CREATE INDEX ON bioportal_tritemporal_counts (
    test_type, collected_date, bulletin_date
);
ANALYZE VERBOSE bioportal_tritemporal_counts;

REFRESH MATERIALIZED VIEW bioportal_tritemporal_deltas;
CREATE INDEX ON bioportal_tritemporal_deltas (
    test_type, bulletin_date, collected_date
);
ANALYZE VERBOSE bioportal_tritemporal_deltas;

REFRESH MATERIALIZED VIEW bioportal_collected_agg;
CREATE INDEX ON bioportal_collected_agg (
    test_type, bulletin_date, collected_date
);
ANALYZE VERBOSE bioportal_collected_agg;
