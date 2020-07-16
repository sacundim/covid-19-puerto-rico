#################################################################################
#
# Charts about molecular test data from 2020-05-20, which have their own logic
#

import altair as alt
import datetime
import pandas as pd
import sqlalchemy
from sqlalchemy import cast
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
            tooltip=[alt.Tooltip('datum_date:T', title='Fecha de muestra'),
                     alt.Tooltip('variable:N', title='Variable'),
                     alt.Tooltip(field='value:Q', format=".2f", title='Pruebas')]
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
    ORDER = ['Salud (moleculares)',
             'Salud (serológicas)',
             'PRPHT (moleculares)']

    def make_chart(self, df):
        lines = alt.Chart(df.dropna()).transform_filter(
            alt.datum.value > 0.0
        ).mark_line(
            point='transparent'
        ).encode(
            x=alt.X('datum_date:T', title='Puerto Rico',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('value:Q', title=None,
                    scale=alt.Scale(type='log'),
                    axis=alt.Axis(format='.2%')),
            color=alt.Color('Fuente:N', sort=self.ORDER,
                            legend=alt.Legend(orient='top', title=None, offset=0)),
            tooltip=[alt.Tooltip('datum_date:T', title='Fecha de muestra'),
                     alt.Tooltip('value:Q', format=".2%", title='Tasa de positividad')]
        )

        return lines.properties(
            width=550, height=150
        ).facet(
            row=alt.Row('variable:N', title=None)
        ).resolve_scale(
            y='shared'
        )

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]


class NewPositiveRate(AbstractPositiveRate):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_datum_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.source.label('Fuente'),
            table.c.bulletin_date,
            table.c.datum_date,
            (table.c.smoothed_daily_positive_tests / table.c.smoothed_daily_tests)\
                .label('Positivas / pruebas'),
            (table.c.smoothed_daily_cases / table.c.smoothed_daily_tests)\
                .label('Casos / pruebas')
        ])
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'datum_date'])
        return pd.melt(df, ['Fuente', 'bulletin_date', 'datum_date'])


class CumulativePositiveRate(AbstractPositiveRate):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_datum_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.source.label('Fuente'),
            table.c.bulletin_date,
            table.c.datum_date,
            (cast(table.c.cumulative_positive_tests, DOUBLE_PRECISION)
                / table.c.cumulative_tests)\
                .label('Positivas / pruebas'),
            (cast(table.c.cumulative_cases, DOUBLE_PRECISION)
                  / table.c.cumulative_tests)\
                .label('Casos / pruebas')
        ])
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'datum_date'])
        return pd.melt(df, ['Fuente', 'bulletin_date', 'datum_date'])



class AbstractPerCapitaChart(charts.AbstractChart):
    POPULATION = 3_193_694
    POPULATION_THOUSANDS = POPULATION / 1_000.0
    POPULATION_MILLIONS = POPULATION / 1_000_000.0
    ORDER = ['Salud (moleculares)',
             'Salud (serológicas)',
             'PRPHT (moleculares)']

    def make_chart(self, df):
        return alt.Chart(df.dropna()).transform_calculate(
            per_thousand=alt.datum.value / self.POPULATION_THOUSANDS
        ).mark_line(
            point='transparent'
        ).encode(
            x=alt.X('datum_date:T', title='Puerto Rico',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('per_thousand:Q', title=None),
            color=alt.Color('Fuente:N', sort=self.ORDER,
                            legend=alt.Legend(orient='top', title=None)),
            tooltip=[alt.Tooltip('datum_date:T', title='Fecha de muestra'),
                     alt.Tooltip('per_thousand:Q', format=".2f",
                                 title='Pruebas por mil habitantes')]
        ).properties(
            width=600, height=125
        )

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]


class NewDailyTestsPerCapita(AbstractPerCapitaChart):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_datum_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.source.label('Fuente'),
            table.c.bulletin_date,
            table.c.datum_date,
            table.c.smoothed_daily_tests.label('value')
        ])
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date", "datum_date"])

class CumulativeTestsPerCapita(AbstractPerCapitaChart):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_datum_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.source.label('Fuente'),
            table.c.bulletin_date,
            table.c.datum_date,
            table.c.cumulative_tests.label('value')
        ])
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date", "datum_date"])


class CumulativeTestsVsCases(charts.AbstractChart):
    POPULATION_MILLIONS = 3.193_694
    ORDER = ['Salud (moleculares)',
             'Salud (serológicas)']

    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_datum_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.datum_date,
            table.c.source.label('Fuente'),
            table.c.cumulative_tests,
            table.c.cumulative_cases
        ])
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'datum_date'])

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]

    def make_chart(self, df):
        max_x, max_y = 2_600, 100_000

        main = alt.Chart(df.dropna()).transform_calculate(
            tests_per_million=alt.datum.cumulative_tests / self.POPULATION_MILLIONS,
            cases_per_million=alt.datum.cumulative_cases / self.POPULATION_MILLIONS,
            positive_rate=alt.datum.cumulative_cases / alt.datum.cumulative_tests
        ).mark_line(point='transparent').encode(
            y=alt.Y('tests_per_million:Q', scale=alt.Scale(domain=[0, max_y]),
                    title='Total de pruebas por millón de habitantes'),
            x=alt.X('cases_per_million:Q', scale=alt.Scale(domain=[0, max_x]),
                    title='Total de casos por millón de habitantes'),
            order=alt.Order('datum_date:T'),
            color=alt.Color('Fuente:N', sort=self.ORDER,
                            legend=alt.Legend(orient='top', title=None, offset=25)),
            tooltip=[alt.Tooltip('yearmonthdate(datum_date):T', title='Fecha de muestra'),
                     alt.Tooltip('Fuente:N'),
                     alt.Tooltip('tests_per_million:Q', format=",d",
                                 title='Pruebas por millón de habitantes'),
                     alt.Tooltip('cases_per_million:Q', format=",d",
                                 title='Casos por millón de habitantes'),
                     alt.Tooltip('positive_rate:Q', format=".2%",
                                 title='Tasa de positividad')]
        )

        return (self.make_ref_chart(max_x, max_y) + main).properties(
            width=540, height=216
        )

    def make_ref_chart(self, max_x, max_y):
        df = pd.DataFrame(
            # These are more hardcoded than they look, they are bound
            # to landscape aspect ratios
            [{'x': 0, 'key': 'point_five_pct', 'y': 0.0, 'positivity': 0.005},
             {'x': max_y * 0.005, 'key': 'point_five_pct', 'y': max_y, 'positivity': 0.005},
             {'x': 0, 'key': 'one_pct', 'y': 0.0, 'positivity': 0.01},
             {'x': max_y * 0.01, 'key': 'one_pct', 'y': max_y, 'positivity': 0.01},
             {'x': 0, 'key': 'two_pct', 'y': 0.0, 'positivity': 0.02},
             {'x': max_y * 0.02, 'key': 'two_pct', 'y': max_y, 'positivity': 0.02},
             {'x': 0, 'key': 'five_pct', 'y': 0.0, 'positivity': 0.05},
             {'x': max_x, 'key': 'five_pct', 'y': max_x / 0.05, 'positivity': 0.05}])

        lines = alt.Chart(df).mark_line(
            color='grey', strokeWidth=0.5, clip=True, strokeDash=[6, 4]
        ).encode(
            x=alt.X('x:Q'),
            y=alt.Y('y:Q'),
            detail='key:N'
        )

        text = alt.Chart(df).transform_filter(
            alt.datum.x > 0
        ).mark_text(
            color='grey',
            align='left',
            baseline='middle',
            size=14, dx=4, dy=-8
        ).encode(
            x=alt.X('x:Q'),
            y=alt.Y('y:Q'),
            text=alt.Text('positivity:Q', format='.1%')
        )

        return lines + text
