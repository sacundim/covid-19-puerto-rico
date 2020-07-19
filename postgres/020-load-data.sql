COPY announcement
FROM '/data/cases/PuertoRico-bulletin.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bitemporal
FROM '/data/cases/PuertoRico-bitemporal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY canonical_municipal_names
FROM '/data/cases/Municipalities-canonical_names.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY municipal
FROM '/data/cases/Municipalities-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY age_groups_molecular
FROM '/data/cases/AgeGroups-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY age_groups_population
FROM '/data/cases/AgeGroups-population.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bioportal
FROM '/data/cases/PuertoRico-bioportal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY hospitalizations
FROM '/data/cases/PuertoRico-hospitalizations.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY prpht_molecular_raw
FROM '/data/cases/PRPHT-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

---------------------------------------------------------
DROP TABLE IF EXISTS bioportal_raw;
CREATE TABLE bioportal_raw (
    collectedDate TEXT,
    reportedDate TEXT,
    ageRange TEXT,
    testType TEXT,
    result TEXT,
    patientCity TEXT,
    createdAt TEXT
);

COPY bioportal_raw
FROM '/data/bioportal/minimal-info-unique-tests.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';



INSERT INTO bioportal_tests (datum_date, bulletin_date, created_at, municipality, positive)
WITH with_date_format AS (
    SELECT
        to_date(collectedDate, 'MM/DD/YYYY') AS collectedDate,
        to_date(reportedDate, 'MM/DD/YYYY') AS reportedDate,
        ageRange,
        testType,
        result,
        patientCity,
        to_timestamp(createdAt, 'MM/DD/YYYY HH24:MI') AS createdAt
    FROM bioportal_raw
    WHERE testType = 'Molecular'
), without_null_dates AS (
    SELECT
        CASE
            WHEN collectedDate >= '2020-01-01'
            THEN collectedDate
            -- Suggested by @rafalab. He uses two days as the value and says
            -- that's the average, but my spot check says 2.8 days. I use the
            -- createdAt instead of reportedDate though.
            ELSE date(createdAt) - INTERVAL '3 day'
        END AS datum_date,

        CASE
            WHEN reportedDate >= '2020-03-13'
            THEN reportedDate
            ELSE date(createdAt)
        END AS bulletin_date,

        createdAt created_at,

        CASE patientCity
            WHEN 'Rio Grande' THEN 'Río Grande'
            ELSE patientCity
        END AS municipality,

        COALESCE(result, '') LIKE '%Positive%'
            AS positive
    FROM with_date_format
)
SELECT
    CASE
        WHEN bulletin_date < datum_date
        THEN date(created_at) - INTERVAL '3 day'
        ELSE datum_date
    END AS datum_date,
    CASE
        WHEN bulletin_date < datum_date
        THEN date(created_at)
        ELSE bulletin_date
    END AS bulletin_date,
    created_at,
    municipality,
    positive
FROM without_null_dates;

CREATE INDEX ON bioportal_tests (datum_date, bulletin_date, municipality, positive);
CREATE INDEX ON bioportal_tests (datum_date, municipality, positive);
CREATE INDEX ON bioportal_tests (bulletin_date, municipality, positive);