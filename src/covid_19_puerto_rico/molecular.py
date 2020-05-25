#################################################################################
#
# Charts about molecular test data from 2020-05-20, which have their own logic
#

from abc import ABC, abstractmethod
import altair as alt
import datetime
import logging
import numpy as np
import pandas as pd
from pathlib import Path
import sqlalchemy
from sqlalchemy.sql import select, text, cast
from sqlalchemy.types import Float
from . import util

class AbstractDatelessChart(ABC):
    def __init__(self, engine, output_dir,
                 output_formats=frozenset(['json'])):
        self.engine = engine
        self.metadata = sqlalchemy.MetaData(engine)
        self.output_dir = output_dir
        self.output_formats = output_formats
        self.name = type(self).__name__

    def render(self, date_range):
        with self.engine.connect() as connection:
            df = self.fetch_data(connection)
        logging.info("%s dataframe: %s", self.name, util.describe_frame(df))

        Path(self.output_dir).mkdir(exist_ok=True)
        util.save_chart(self.make_chart(df),
                        f"{self.output_dir}/{self.name}",
                        self.output_formats)

    @abstractmethod
    def make_chart(self, df):
        pass

    @abstractmethod
    def fetch_data(self, connection):
        pass

class DailyMissingTests(AbstractDatelessChart):
    def make_chart(self, df):
        return alt.Chart(df).mark_bar().encode(
            x=alt.X('yearmonthdate(datum_date):T',
                    title='Fecha de toma de muestra',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('difference:Q', title='Positivos menos confirmados'),
            color=alt.condition(
                alt.datum.difference < 0,
                alt.value('orange'),
                alt.value('teal')
            ),
            tooltip=['datum_date', 'difference:Q']
        ).properties(
            width=575, height=280
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('bitemporal', self.metadata, autoload=True)
        query = select([
            table.c.datum_date,
            (table.c.positive_molecular_tests - table.c.confirmed_cases)\
                .label('difference')
        ]).where(table.c.bulletin_date == datetime.date(year=2020, month=5, day=20))
        return pd.read_sql_query(query, connection, parse_dates=["datum_date"])


class CumulativeMissingTests(AbstractDatelessChart):
    def make_chart(self, df):
        return alt.Chart(df).mark_line(point=True).encode(
            x=alt.X('yearmonthdate(datum_date):T',
                    title='Fecha de toma de muestra',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('difference:Q', title='Positivos menos confirmados'),
            tooltip=['datum_date', 'difference:Q']
        ).properties(
            width=575, height=280
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('cumulative_data', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.datum_date,
            (table.c.positive_molecular_tests - table.c.confirmed_cases)\
                .label('difference')
        ]).where(table.c.bulletin_date == datetime.date(year=2020, month=5, day=20))
        return pd.read_sql_query(query, connection, parse_dates=["datum_date"]).dropna()


class CumulativeTestsPerCapita(AbstractDatelessChart):
    def make_chart(self, df):
        return alt.Chart(df).mark_line(point=True).encode(
            x=alt.X('yearmonthdate(datum_date):T', title=None,
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('value', title=None),
            tooltip=['datum_date', 'variable',
                     alt.Tooltip(field='value',
                                 type='quantitative',
                                 format=".1f")]
        ).properties(
            width=575, height=175
        ).facet(
            row=alt.Row('variable', title=None)
        ).resolve_scale(
            y='independent'
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('cumulative_data', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.datum_date,
                        (table.c.molecular_tests / 3193.694).label("tests_per_thousand"),
                        (cast(table.c.molecular_tests, Float)
                         / cast(table.c.confirmed_cases, Float)).label('tests_per_case')])\
            .where(table.c.bulletin_date == datetime.date(year=2020, month=5, day=20))
        df = pd.read_sql_query(query, connection,
                               parse_dates=["datum_date"])
        df = df.rename(columns={
            'tests_per_thousand': 'Pruebas por 1,000',
            'tests_per_case': 'Pruebas por caso confirmado',
        })
        return pd.melt(df, ["datum_date"])

class NewTestsPerCapita(AbstractDatelessChart):
    def make_chart(self, df):
        return alt.Chart(df).mark_line(point=True).encode(
            x=alt.X('yearmonthdate(datum_date):T', title=None,
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y(alt.repeat('row'), type='quantitative'),
            tooltip=['datum_date',
                     alt.Tooltip(field=alt.repeat('row'),
                                 type='quantitative',
                                 format=".1f")]
        ).properties(
            width=575, height=175
        ).repeat(
            row=['Pruebas por millón', 'Pruebas por caso confirmado']
        ).resolve_scale(
            y='independent'
        )

    def fetch_data(self, connection):
        query = text("""WITH raw AS (
	SELECT 
		datum_date,
		count(datum_date) OVER datum AS count,
		CAST(sum(molecular_tests) OVER datum AS DOUBLE PRECISION) 
			/ count(datum_date) OVER datum molecular_tests,
		cast(sum(molecular_tests) OVER datum AS DOUBLE PRECISION)
			/ nullif(sum(confirmed_cases) OVER datum, 0)
			AS tests_per_confirmed_case
	FROM bitemporal b
	WHERE bulletin_date = '2020-05-20'
	WINDOW datum AS (ORDER BY datum_date RANGE '6 day' PRECEDING)
)
SELECT 
	datum_date,
	molecular_tests / 3.193694 AS molecular_tests_per_million,
	tests_per_confirmed_case
FROM raw 
WHERE count = 7
ORDER BY datum_date""")
        df = pd.read_sql_query(query, connection,
                               parse_dates=["datum_date"])
        return df.rename(columns={
            'molecular_tests_per_million': 'Pruebas por millón',
            'tests_per_confirmed_case': 'Pruebas por caso confirmado'
        })