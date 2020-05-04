CREATE FUNCTION log2(x NUMERIC)
RETURNS NUMERIC AS $$
    SELECT log(2.0, x);
$$ LANGUAGE SQL;


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


CREATE VIEW bitemporal_agg AS
SELECT
    bulletin_date,
    datum_date,

    confirmed_and_probable_cases,
    sum(confirmed_and_probable_cases) OVER bulletin
        AS cumulative_confirmed_and_probable_cases,
    confirmed_and_probable_cases - coalesce(lag(confirmed_and_probable_cases) OVER datum, 0)
        AS delta_confirmed_and_probable_cases,
    (confirmed_and_probable_cases - coalesce(lag(confirmed_and_probable_cases) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_confirmed_and_probable_cases,

    confirmed_cases,
    sum(confirmed_cases) OVER bulletin
        AS cumulative_confirmed_cases,
    confirmed_cases - coalesce(lag(confirmed_cases) OVER datum, 0)
        AS delta_confirmed_cases,
    (confirmed_cases - coalesce(lag(confirmed_cases) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_confirmed_cases,

    probable_cases,
    sum(probable_cases) OVER bulletin
        AS cumulative_probable_cases,
    probable_cases - coalesce(lag(probable_cases) OVER datum, 0)
        AS delta_probable_cases,
    (probable_cases - coalesce(lag(probable_cases) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_probable_cases,

    deaths,
    sum(deaths) OVER bulletin
        AS cumulative_deaths,
    deaths - coalesce(lag(deaths) OVER datum, 0)
        AS delta_deaths,
    (deaths - coalesce(lag(deaths) OVER datum, 0))
        * (bulletin_date - datum_date) AS lateness_deaths
FROM bitemporal
WINDOW
    bulletin AS (PARTITION BY bulletin_date ORDER BY datum_date),
    datum AS (PARTITION BY datum_date ORDER BY bulletin_date);

COMMENT ON VIEW bitemporal_agg IS
'Useful aggregations/windows over the bitemporal table:

- Cumulative: Cumulative sums, partitioned by bulletin date;
- Delta: Current bulletin_date''s value for current datum_date
  minus previous bulletin_date''s value for previous datum_date;
- Lateness score: Delta * (bulletin_date - datum_date).';


-------------------------------------------------------------------------------
CREATE SCHEMA quality;

COMMENT ON SCHEMA quality IS
'Views to explore data quality issues in the data';


CREATE VIEW quality.mismatched_announcement_aggregates AS
WITH base AS (
	SELECT
		bulletin_date,
		lag(cumulative_cases) OVER bulletin
			AS previous_cumulative_cases,
		new_cases,
		cumulative_cases - lag(cumulative_cases) OVER bulletin
			AS computed_new_cases,
		cumulative_cases,
		lag(cumulative_cases) OVER bulletin + new_cases
			AS computed_cumulative_cases,

		lag(cumulative_confirmed_cases) OVER bulletin
			AS previous_cumulative_confirmed_cases,
		new_confirmed_cases,
		cumulative_confirmed_cases - lag(cumulative_confirmed_cases) OVER bulletin
			AS computed_new_confirmed_cases,
		cumulative_confirmed_cases,
		lag(cumulative_confirmed_cases) OVER bulletin + new_confirmed_cases
			AS computed_cumulative_confirmed_cases,

		lag(cumulative_probable_cases) OVER bulletin
			AS previous_cumulative_probable_cases,
		new_probable_cases,
		cumulative_probable_cases - lag(cumulative_probable_cases) OVER bulletin
			AS computed_new_probable_cases,
		cumulative_probable_cases,
		lag(cumulative_probable_cases) OVER bulletin + new_probable_cases
			AS computed_cumulative_probable_cases
	FROM announcement a
	WINDOW bulletin AS (ORDER BY bulletin_date))
SELECT *
FROM base
WHERE new_cases != computed_new_cases
OR new_confirmed_cases != computed_new_confirmed_cases
OR new_probable_cases != computed_new_probable_cases;

COMMENT ON TABLE quality.mismatched_announcement_aggregates IS
'Check whether the daily new cases and the cumulative figures
in the bulletins match. Which they don''t always do...';


-------------------------------------------------------------------------------
CREATE SCHEMA products;

COMMENT ON SCHEMA products IS
'Views with queries that are intended to be presentations of the data';

CREATE VIEW products.main_graph AS
SELECT
	ba.bulletin_date,
	ba.datum_date,
	ba.cumulative_confirmed_and_probable_cases AS confirmed_and_probable_cases,
	ba.cumulative_confirmed_cases AS confirmed_cases,
	ba.cumulative_probable_cases AS probable_cases,
	announcement.cumulative_positive_results AS positive_results,
	ba.cumulative_deaths AS deaths,
	announcement.cumulative_cases AS announced_cases,
	announcement.cumulative_deaths AS announced_deaths
FROM bitemporal_agg ba
FULL OUTER JOIN announcement
	ON announcement.bulletin_date = ba.datum_date;


CREATE VIEW products.daily_deltas AS
SELECT
    bulletin_date,
	datum_date,
	delta_confirmed_and_probable_cases,
	delta_confirmed_cases,
	delta_probable_cases,
	delta_deaths
FROM bitemporal_agg;


CREATE VIEW products.lateness AS
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
FROM bitemporal_agg
GROUP BY bulletin_date;

COMMENT ON VIEW products.lateness IS
'An estimate of how late on average new data for each bulletin
is, based on the `bitemporal_agg` view.';


CREATE VIEW doubling_times AS
SELECT
    bulletin_date,
    datum_date,
    window_size.days window_size_days,
    CAST(window_size.days AS NUMERIC)
    	/ NULLIF(log2(cumulative_confirmed_and_probable_cases)
    				- log2(LAG(cumulative_confirmed_and_probable_cases, window_size.days) OVER datum), 0)
    	AS cumulative_confirmed_and_probable_cases,
    CAST(window_size.days AS NUMERIC)
    	/ NULLIF(log2(cumulative_confirmed_cases) - log2(LAG(cumulative_confirmed_cases, window_size.days) OVER datum), 0)
    	AS cumulative_confirmed_cases,
    CAST(window_size.days AS NUMERIC)
    	/ NULLIF(log2(cumulative_probable_cases) - log2(LAG(cumulative_probable_cases, window_size.days) OVER datum), 0)
    	AS cumulative_probable_cases,
    CAST(window_size.days AS NUMERIC)
    	/ NULLIF(log2(cumulative_deaths) - log2(LAG(cumulative_deaths, window_size.days) OVER datum), 0)
    	AS cumulative_deaths
FROM bitemporal_agg
CROSS JOIN (VALUES (7), (14), (21)) AS window_size (days)
WINDOW datum AS (
	PARTITION BY bulletin_date, window_size.days
	ORDER BY datum_date);

COMMENT ON VIEW doubling_times IS
'How long it took values to double, expressed in fractional days.
Computed over windows of 7, 14 and 21 days.';
