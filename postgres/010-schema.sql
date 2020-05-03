CREATE TABLE bitemporal (
    bulletin_date DATE NOT NULL,
    datum_date DATE NOT NULL,
    confirmed_and_probable_cases INTEGER,
    confirmed_cases INTEGER,
    probable_cases INTEGER,
    deaths INTEGER,
    PRIMARY KEY (bulletin_date, datum_date)
);

COMMENT ON TABLE bitemporal IS
'Data from graphs in Department of Health bulletins since April 25,
organized in a bitemporal schema that records datums by both the date
as of which each bulletin reports data for and the dates that each
bulletin attributes values to.  Read this as: "As of [bulletin_date],
the Department of Health had attributed [metric] incidences to
[datum_date]."  Data points are counts for the datum day (not
cumulative sums).';

COMMENT ON COLUMN bitemporal.bulletin_date IS
'Date that the bulletin has data up to.  Note that bulletins are
published on the next day, and many collections use that publication
date instead of this date.';

COMMENT ON COLUMN bitemporal.datum_date IS
'Date that the data items are attributed to.  For cases this
is the data that the test sample was taken.  For deaths this is
the date of the actual death.';


CREATE TABLE announcement (
    bulletin_date DATE NOT NULL,
    cumulative_positive_results INTEGER,
    cumulative_negative_results INTEGER,
    cumulative_pending_results INTEGER,
    cumulative_inconclusive_results INTEGER,
    new_cases INTEGER,
    new_confirmed_cases INTEGER,
    new_probable_cases INTEGER,
    cumulative_cases INTEGER,
    cumulative_confirmed_cases INTEGER,
    cumulative_probable_cases INTEGER,
    cumulative_deaths INTEGER,
    cumulative_certified_deaths INTEGER,
    cumulative_confirmed_deaths INTEGER,
    PRIMARY KEY (bulletin_date)
);

COMMENT ON TABLE announcement IS
'Daily "headline" values announced in the bulletins, the ones that
normally make the news.  These are generally attributed to the date
that the Department of Health recorded them and not the date each
death happened, test administered, etc.';

COMMENT ON COLUMN announcement.cumulative_positive_results IS
'Positive test results.  No deduplication done by person.
Publication stopped on April 25.';

COMMENT ON COLUMN announcement.cumulative_negative_results IS
'Negative test results.  No deduplication done by person.
Publication stopped on April 22.';

COMMENT ON COLUMN announcement.cumulative_pending_results IS
'Pending test results.  No deduplication done by person.
Publication stopped on April 22.';

COMMENT ON COLUMN announcement.new_cases IS
'Unique confirmed or probable cases (deduplicated by person),
by date that they were announced (not date of test sample).';

COMMENT ON COLUMN announcement.new_confirmed_cases IS
'Unique confirmed cases (molecular test, deduplicated by person),
by date that they were announced (not date of test sample).';

COMMENT ON COLUMN announcement.new_probable_cases IS
'Unique probable cases (antibody test, deduplicated by person),
by date that they were announced (not date of test sample).';

COMMENT ON COLUMN announcement.cumulative_confirmed_deaths IS
'Deaths confirmed by a positive lab test, by date that they
were announced (not date of actual death).';

COMMENT ON COLUMN announcement.cumulative_certified_deaths IS
'Deaths not confirmed by a positive lab test, but for which a
doctor or coroner indicated COVID-19 as cause of death in the
death certificate.  Given by date that they were announced (not
date of actual death).  First reported April 8';


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