#################################################################################
#
# Charts about molecular test data from 2020-05-20, which have their own logic
#

import altair as alt
import datetime
import pandas as pd
import sqlalchemy
from sqlalchemy import cast, Float
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.sql import select
from . import charts


class DailyMissingTests(charts.AbstractChart):
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
            width=575, height=265
        )

    def fetch_data(self, connection):
        cases = sqlalchemy.Table('bitemporal', self.metadata, autoload=True)
        tests = sqlalchemy.Table('bioportal_bitemporal', self.metadata, autoload=True)
        query = select([
            cases.c.bulletin_date,
            cases.c.datum_date,
            (tests.c.positive_molecular_tests - cases.c.confirmed_cases).label('difference')
        ]).select_from(
            tests.outerjoin(cases, tests.c.datum_date == cases.c.datum_date)
        ).where(tests.c.bulletin_date == datetime.date(year=2020, month=5, day=20))
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date", "datum_date"])


class CumulativeMissingTests(charts.AbstractChart):
    def make_chart(self, df):
        return alt.Chart(df).mark_area(line=True, point=True).encode(
            x=alt.X('yearmonthdate(datum_date):T',
                    title='Fecha de toma de muestra',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('difference:Q', title='Positivos menos confirmados'),
            tooltip=['datum_date', 'difference:Q']
        ).properties(
            width=575, height=265
        )

    def fetch_data(self, connection):
        cases = sqlalchemy.Table('bitemporal_agg', self.metadata, autoload=True)
        tests = sqlalchemy.Table('bioportal_bitemporal_agg', self.metadata, autoload=True)
        query = select([
            cases.c.bulletin_date,
            cases.c.datum_date,
            (tests.c.cumulative_positive_molecular_tests - cases.c.cumulative_confirmed_cases)\
                .label('difference')
        ]).select_from(
            tests.outerjoin(cases, tests.c.datum_date == cases.c.datum_date)
        ).where(tests.c.bulletin_date == datetime.date(year=2020, month=5, day=20))
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date", "datum_date"]).dropna()


class TestsBySampleDate(charts.AbstractChart):
    def make_chart(self, df):
        data_date = alt.Chart(df).mark_text(baseline='middle').encode(
            text=alt.Text('bulletin_date',
                          type='temporal',
                          aggregate='max',
                          timeUnit='yearmonthdate',
                          format='Datos hasta: %d de %B, %Y'),
        ).properties(
            width=575, height=40
        )

        sort_order = [
            'Pruebas nuevas por caso confirmado',
            'Pruebas acumuladas por caso confirmado (promedio 7 días)'
            'Pruebas nuevas diarias por mil habitantes (promedio 7 días)',
            'Pruebas acumuladas diarias por mil habitantes',
        ]
        trellis = alt.Chart(df).mark_line(point=True).encode(
            x=alt.X('yearmonthdate(datum_date):T',
                    title='Fecha de toma de muestra',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('value:Q', title=None, scale=alt.Scale(type='linear')),
            tooltip=['datum_date', 'variable:N',
                     alt.Tooltip(field='value',
                                 type='quantitative',
                                 format=".2f")]
        ).properties(
            width=575, height=100
        ).facet(
            columns=1,
            facet=alt.Facet('variable', title=None, sort=sort_order)
        ).resolve_scale(
            y='independent'
        )

        return alt.vconcat(data_date, trellis)

    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_sample_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.datum_date,
            table.c.new_tests_per_confirmed_case,
            table.c.cumulative_tests_per_confirmed_case,
            table.c.new_daily_tests_per_thousand,
            table.c.cumulative_daily_tests_per_thousand,
        ])
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'datum_date'])
        df = df.rename(columns={
            'new_tests_per_confirmed_case': 'Pruebas nuevas por caso confirmado  (promedio 7 días)',
            'cumulative_tests_per_confirmed_case': 'Pruebas acumuladas por caso confirmado',
            'new_daily_tests_per_thousand': 'Pruebas nuevas diarias por mil habitantes (promedio 7 días)',
            'cumulative_daily_tests_per_thousand': 'Pruebas acumuladas diarias por mil habitantes',
        })
        return pd.melt(df, ['bulletin_date', 'datum_date'])

    def filter_data(self, df, bulletin_date):
        chopped = df.loc[df['bulletin_date'] <= pd.to_datetime(bulletin_date)]
        max_date = chopped['bulletin_date'].max()
        return df.loc[df['bulletin_date'] == max_date]


class AbstractPositiveRate(charts.AbstractChart):
    def make_chart(self, df):
        data_date = alt.Chart(df).mark_text(baseline='middle').encode(
            text=alt.Text('bulletin_date',
                          type='temporal',
                          aggregate='max',
                          format='Datos hasta: %d de %B, %Y'),
        ).properties(
            width=575, height=40
        )

        lines = alt.Chart(df.dropna()).mark_line(
            point=True
        ).encode(
            x=alt.X('bulletin_date:T', title='Puerto Rico',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('value:Q', title=None, axis=alt.Axis(format='.2%')),
            color=alt.Color('Fuente:N', legend=alt.Legend(orient='top')),
            tooltip=['bulletin_date:T',
                     alt.Tooltip(field='value',
                                 type='quantitative',
                                 format=".2%")]
        )

        return lines.properties(
            width=575, height=150
        ).facet(
            columns=1,
            facet=alt.Facet('variable:N', title=None)
        ).resolve_scale(
            y='independent'
        )

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] <= pd.to_datetime(bulletin_date)]


class NewPositiveRate(AbstractPositiveRate):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_bulletin_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.source.label('Fuente'),
            table.c.bulletin_date,
            (table.c.smoothed_daily_positive_molecular_tests / table.c.smoothed_daily_tests)\
                .label('Moleculares positivas / total'),
            (table.c.smoothed_daily_confirmed_cases / table.c.smoothed_daily_tests)\
                .label('Casos confirmados / Moleculares total')
        ])
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date'])
        return pd.melt(df, ['Fuente', 'bulletin_date'])


class CumulativePositiveRate(AbstractPositiveRate):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_bulletin_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.source.label('Fuente'),
            table.c.bulletin_date,
            (cast(table.c.cumulative_positive_molecular_tests, DOUBLE_PRECISION)
                / table.c.cumulative_molecular_tests)\
                .label('Moleculares positivas / pruebas'),
            (cast(table.c.cumulative_confirmed_cases, DOUBLE_PRECISION)
                  / table.c.cumulative_molecular_tests)\
                .label('Casos confirmados / pruebas')
        ])
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date'])
        return pd.melt(df, ['Fuente', 'bulletin_date'])



class AbstractPerCapitaChart(charts.AbstractChart):
    POPULATION = 3_193_694
    POPULATION_THOUSANDS = POPULATION / 1_000.0
    POPULATION_MILLIONS = POPULATION / 1_000_000.0

    def make_chart(self, df):
        data_date = alt.Chart(df).mark_text(baseline='middle').encode(
            text=alt.Text('bulletin_date',
                          type='temporal',
                          aggregate='max',
                          format='Datos hasta: %d de %B, %Y'),
        ).properties(
            width=575, height=40
        )

        return alt.Chart(df.dropna()).transform_calculate(
            per_thousand=alt.datum.value / self.POPULATION_THOUSANDS
        ).mark_line(
            point=True
        ).encode(
            x=alt.X('bulletin_date:T', title='Puerto Rico',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('per_thousand:Q', title=None),
            color=alt.Color('Fuente:N', legend=alt.Legend(orient='top')),
            tooltip=['bulletin_date:T',
                     alt.Tooltip(field='per_thousand',
                                 type='quantitative',
                                 format=".2f")]
        ).properties(
            width=575, height=150
        )

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] <= pd.to_datetime(bulletin_date)]


class NewDailyTestsPerCapita(AbstractPerCapitaChart):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_bulletin_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.source.label('Fuente'),
            table.c.bulletin_date,
            table.c.smoothed_daily_tests.label('value')
        ])
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date"])

class CumulativeTestsPerCapita(AbstractPerCapitaChart):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_bulletin_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.source.label('Fuente'),
            table.c.bulletin_date,
            table.c.cumulative_molecular_tests.label('value')
        ])
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date"])
