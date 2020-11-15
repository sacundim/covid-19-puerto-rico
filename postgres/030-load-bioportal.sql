COPY bioportal_tests (
    downloaded_at,
    patient_id,
    raw_collected_date,
    raw_reported_date,
    age_range,
    test_type,
    result,
    region,
    order_created_at,
    result_created_at
)
FROM PROGRAM 'for file in $(ls /data/bioportal/orders-basic/csv_v1/orders-basic_*.csv.bz2 |tail -n 1); do (cat "${file}" |bunzip2 |tail -n+2); done'
    CSV ENCODING 'UTF-8' NULL '';

SET maintenance_work_mem='2GB';
CREATE INDEX ON bioportal_tests (test_type, bulletin_date, collected_date, reported_date, positive);
ANALYZE VERBOSE bioportal_tests;


REFRESH MATERIALIZED VIEW bioportal_tritemporal_preagg;
CREATE INDEX ON bioportal_tritemporal_preagg (
    test_type, bulletin_date, collected_date, reported_date
);
ANALYZE VERBOSE bioportal_tritemporal_preagg;


REFRESH MATERIALIZED VIEW bioportal_tritemporal_agg;
CREATE INDEX ON bioportal_tritemporal_agg (
    test_type, bulletin_date, collected_date, reported_date
);
ANALYZE VERBOSE bioportal_tritemporal_agg;


REFRESH MATERIALIZED VIEW bioportal_collected_agg;
CREATE INDEX ON bioportal_collected_agg (
    test_type, bulletin_date, collected_date
);
ANALYZE VERBOSE bioportal_collected_agg;


REFRESH MATERIALIZED VIEW bioportal_reported_agg;
CREATE INDEX ON bioportal_reported_agg (
    test_type, bulletin_date, reported_date
);
ANALYZE VERBOSE bioportal_reported_agg;