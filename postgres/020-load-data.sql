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



INSERT INTO bioportal_tests (
    collected_date, reported_date, created_date, created_at, municipality, positive
)
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
        END AS collected_date,

        CASE
            WHEN reportedDate >= '2020-03-13'
            THEN reportedDate
            ELSE date(createdAt)
        END AS reported_date,

        -- I have have opted to use the `createdAt` field as the grouping field
        -- for daily data deltas because I see a TON of weird additions to older
        -- or newer `reportedAt`  values.  I've verified that `createdAt` is UTC
        -- time, Puerto Rico is UTC-4
        date(createdAt - INTERVAL '4 hour')
            AS created_date,

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
        WHEN created_date < collected_date
        THEN created_date - INTERVAL '3 day'
        ELSE collected_date
    END AS collected_date,

    CASE
        WHEN reported_date < collected_date
        THEN created_date
        ELSE reported_date
    END AS reported_date,

    created_date,
    created_at,
    municipality,
    positive
FROM without_null_dates;

CREATE INDEX ON bioportal_tests (collected_date, created_date, municipality, positive);
CREATE INDEX ON bioportal_tests (collected_date, municipality, positive);
CREATE INDEX ON bioportal_tests (created_date, municipality, positive);