COPY announcement
FROM '/data/clean/PuertoRico-bulletin.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bitemporal
FROM '/data/clean/PuertoRico-bitemporal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY canonical_municipal_names
FROM '/data/clean/Municipalities-canonical_names.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY municipal
FROM '/data/clean/Municipalities-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY age_groups_molecular
FROM '/data/clean/AgeGroups-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY age_groups_population
FROM '/data/clean/AgeGroups-population.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY bioportal
FROM '/data/clean/PuertoRico-bioportal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY hospitalizations
FROM '/data/clean/PuertoRico-hospitalizations.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

COPY prpht_molecular_raw
FROM '/data/clean/PRPHT-molecular.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

---------------------------------------------------------
CREATE TEMPORARY TABLE bioportal_raw (
    id BIGINT NOT NULL,
    collectedDate DATE,
    reportedDate DATE,
    ageRange TEXT,
    testType TEXT,
    result TEXT,
    patientCity TEXT,
    createdAt TIMESTAMP WITHOUT TIME ZONE
);

COPY bioportal_raw
FROM '/data/raw/bioportal.csv'
    CSV ENCODING 'UTF-8' HEADER NULL '';

INSERT INTO bioportal_tests
WITH cleaner AS (
    SELECT
        id,

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
    FROM bioportal_raw
    WHERE testType = 'Molecular'
)
SELECT
    id,
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
FROM cleaner;

CREATE INDEX ON bioportal_tests (datum_date, bulletin_date, municipality, positive);
CREATE INDEX ON bioportal_tests (datum_date, municipality, positive);
CREATE INDEX ON bioportal_tests (bulletin_date, municipality, positive);