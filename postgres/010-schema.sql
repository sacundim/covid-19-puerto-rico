CREATE TABLE bitemporal (
    bulletin_date DATE NOT NULL,
    datum_date DATE NOT NULL,
    confirmed_and_probable_cases INTEGER,
    confirmed_cases INTEGER,
    probable_cases INTEGER,
    deaths INTEGER,
    PRIMARY KEY (bulletin_date, datum_date)
);

CREATE TABLE bulletin (
    bulletin_date DATE NOT NULL,
    cumulative_positive_results INTEGER,
    cumulative_negative_results INTEGER,
    cumulative_pending_results INTEGER,
    cumulative_inconclusive_results INTEGER,
    cumulative_confirmed_deaths INTEGER,
    cumulative_certified_deaths INTEGER,
    cumulative_cases INTEGER,
    cumulative_confirmed_cases INTEGER,
    cumulative_probable_cases INTEGER,
    PRIMARY KEY (bulletin_date)
);

CREATE VIEW bitemporal_analysis AS
SELECT
    bulletin_date,
    datum_date,

    confirmed_and_probable_cases,
    sum(confirmed_and_probable_cases) OVER bu
        AS cumulative_confirmed_and_probable_cases,
    confirmed_and_probable_cases - coalesce(lag(confirmed_and_probable_cases) OVER datum, 0)
        AS delta_confirmed_and_probable_cases,
    (confirmed_and_probable_cases - coalesce(lag(confirmed_and_probable_cases) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_confirmed_and_probable_cases,

    confirmed_cases,
    sum(confirmed_cases) OVER bu
        AS cumulative_confirmed_cases,
    confirmed_cases - coalesce(lag(confirmed_cases) OVER datum, 0)
        AS delta_confirmed_cases,
    (confirmed_cases - coalesce(lag(confirmed_cases) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_confirmed_cases,

    probable_cases,
    sum(probable_cases) OVER bu
        AS cumulative_probable_cases,
    probable_cases - coalesce(lag(probable_cases) OVER datum, 0)
        AS delta_probable_cases,
    (probable_cases - coalesce(lag(probable_cases) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_probable_cases,

    deaths,
    sum(deaths) OVER bu
        AS cumulative_deaths,
    deaths - coalesce(lag(deaths) OVER datum, 0)
        AS delta_deaths,
    (deaths - coalesce(lag(deaths) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_deaths
FROM bitemporal
WINDOW
    bu AS (PARTITION BY bulletin_date ORDER BY datum_date),
    datum AS (PARTITION BY datum_date ORDER BY bulletin_date);

CREATE VIEW lateness_analysis AS
SELECT
    bulletin_date,
    cast(sum(lateness_confirmed_and_probable_cases) AS DOUBLE PRECISION)
        / nullif(sum(delta_confirmed_and_probable_cases), 0)
        AS lateness_confirmed_and_probable_cases,
    cast(sum(lateness_confirmed_cases) AS DOUBLE PRECISION)
        / nullif(sum(delta_confirmed_cases), 0)
        AS lateness_confirmed_cases,
    cast(sum(lateness_probable_cases) AS DOUBLE PRECISION)
        / nullif(sum(delta_probable_cases), 0)
        AS lateness_probable_cases,
    cast(sum(lateness_deaths) AS DOUBLE PRECISION)
        / nullif(sum(delta_deaths), 0)
        AS lateness_deaths
FROM bitemporal_analysis
GROUP BY bulletin_date;