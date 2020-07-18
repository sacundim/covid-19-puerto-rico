#################################################################################
#
# Charts about molecular test data from 2020-05-20, which have their own logic
#

import altair as alt
import pandas as pd
import sqlalchemy
from sqlalchemy import cast, and_
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.sql import select
from . import charts


class DailyMissingTests(charts.AbstractChart):
    def make_chart(self, df):
        return alt.Chart(df).transform_calculate(
            difference=alt.datum.positive_molecular_tests - alt.datum.confirmed_cases
        ).mark_bar().encode(
            x=alt.X('yearmonthdate(datum_date):T',
                    title='Fecha de toma de muestra',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('difference:Q', title='Positivos menos confirmados'),
            color=alt.condition(
                alt.datum.difference < 0,
                alt.value('orange'),
                alt.value('teal')
            ),
            tooltip=[alt.Tooltip('datum_date:T', title='Fecha de muestra'),
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('positive_molecular_tests:Q', title='Pruebas positivas'),
                     alt.Tooltip('confirmed_cases:Q', title='Casos confirmados'),
                     alt.Tooltip('difference:Q', title='Positivos menos confirmados')]
        ).properties(
            width=575, height=265
        )

    def fetch_data(self, connection):
        cases = sqlalchemy.Table('bitemporal', self.metadata, autoload=True)
        tests = sqlalchemy.Table('bioportal_bitemporal_agg', self.metadata, autoload=True)
        query = select([
            cases.c.bulletin_date,
            cases.c.datum_date,
            tests.c.positive_molecular_tests,
            cases.c.confirmed_cases,
            (tests.c.positive_molecular_tests - cases.c.confirmed_cases).label('difference')
        ]).select_from(
            tests.outerjoin(
                cases,
                and_(tests.c.datum_date == cases.c.datum_date,
                     tests.c.bulletin_date == cases.c.bulletin_date))
        )
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date", "datum_date"])

    def filter_data(self, df, bulletin_date):
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        return df.loc[df['bulletin_date'] == effective_bulletin_date]



class CumulativeMissingTests(charts.AbstractChart):
    def make_chart(self, df):
        return alt.Chart(df).transform_calculate(
            difference=alt.datum.cumulative_positive_molecular_tests \
                        - alt.datum.cumulative_confirmed_cases
        ).mark_area(opacity=0.8).encode(
            x=alt.X('yearmonthdate(datum_date):T',
                    title='Fecha de toma de muestra',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('difference:Q', title='Positivos menos confirmados'),
            tooltip=[alt.Tooltip('datum_date:T', title='Fecha de muestra'),
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('cumulative_positive_molecular_tests:Q', title='Pruebas positivas'),
                     alt.Tooltip('cumulative_confirmed_cases:Q', title='Casos confirmados'),
                     alt.Tooltip('difference:Q', title='Positivos menos confirmados')]
        ).properties(
            width=575, height=265
        )

    def fetch_data(self, connection):
        cases = sqlalchemy.Table('bitemporal_agg', self.metadata, autoload=True)
        tests = sqlalchemy.Table('bioportal_bitemporal_agg', self.metadata, autoload=True)
        query = select([
            cases.c.bulletin_date,
            cases.c.datum_date,
            tests.c.cumulative_positive_molecular_tests,
            cases.c.cumulative_confirmed_cases
        ]).select_from(
            tests.outerjoin(
                cases,
                and_(tests.c.datum_date == cases.c.datum_date,
                     tests.c.bulletin_date == cases.c.bulletin_date))
        )
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date", "datum_date"]).dropna()

    def filter_data(self, df, bulletin_date):
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        return df.loc[df['bulletin_date'] == effective_bulletin_date]


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
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
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
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        return df.loc[df['bulletin_date'] == effective_bulletin_date]


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
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('per_thousand:Q', format=".2f",
                                 title='Pruebas por mil habitantes')]
        ).properties(
            width=600, height=125
        )

    def filter_data(self, df, bulletin_date):
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        return df.loc[df['bulletin_date'] == effective_bulletin_date]


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
            table.c.cumulative_cases,
            table.c.smoothed_daily_tests,
            table.c.smoothed_daily_cases
        ])
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'datum_date'])

    def filter_data(self, df, bulletin_date):
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        return df.loc[df['bulletin_date'] == effective_bulletin_date]

    def make_chart(self, df):
        max_x, max_y = 1_200, 120_000

        main = alt.Chart(df.dropna()).transform_calculate(
            tests_per_million=alt.datum.cumulative_tests / self.POPULATION_MILLIONS,
            cases_per_million=alt.datum.cumulative_cases / self.POPULATION_MILLIONS,
            positive_rate=alt.datum.cumulative_cases / alt.datum.cumulative_tests,
            smoothed_positive_rate=alt.datum.smoothed_daily_cases / alt.datum.smoothed_daily_tests,
            # We don't use this yet because Altair 4.1.0 doesn't support this channel:
#            angle=alt.expr.atan2(alt.datum.smoothed_daily_tests / max_y,
#                                 alt.datum.smoothed_daily_cases / max_x) * 57.2958,
            # Another one we're saving up for encodings in a future version of the chart:
#            distance=alt.expr.sqrt(alt.expr.pow(alt.datum.smoothed_daily_tests, 2) +
#                                   alt.expr.pow(alt.datum.smoothed_daily_cases, 2))
        ).mark_point().encode(
            y=alt.Y('tests_per_million:Q', scale=alt.Scale(domain=[0, max_y]),
                    title='Total de pruebas por millón de habitantes'),
            x=alt.X('cases_per_million:Q', scale=alt.Scale(domain=[0, max_x]),
                    title='Total de casos por millón de habitantes'),
            order=alt.Order('datum_date:T'),
            color=alt.Color('Fuente:N', sort=self.ORDER,
                            legend=alt.Legend(orient='top', title=None, offset=25)),
            tooltip=[alt.Tooltip('yearmonthdate(datum_date):T', title='Fecha de muestra'),
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('Fuente:N'),
                     alt.Tooltip('tests_per_million:Q', format=",d",
                                 title='Pruebas por millón'),
                     alt.Tooltip('cases_per_million:Q', format=",d",
                                 title='Casos por millón'),
                     alt.Tooltip('positive_rate:Q', format=".2%",
                                 title='Tasa de positividad (acumulada)'),
                     alt.Tooltip('smoothed_positive_rate:Q', format=".2%",
                                 title='Tasa de positividad (7 días, casos / pruebas)'),
                     ]
        )

        return (self.make_ref_chart(max_x, max_y) + main).properties(
            width=525, height=525
        )

    def make_ref_chart(self, max_x, max_y):
        def compute_points(name, positivity):
            top_x = max_y * positivity
            right_y = max_x / positivity
            return [
                {'x': 0, 'key': name, 'y': 0.0, 'positivity': positivity},
                {'x': min(max_x, top_x), 'key': name, 'y': min(max_y, right_y), 'positivity': positivity}
            ]

        df = pd.DataFrame(
            compute_points('point_five_pct', 0.005) + \
            compute_points('one_pct', 0.01) + \
            compute_points('two_pct', 0.02) + \
            compute_points('five_pct', 0.05)
        )

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
