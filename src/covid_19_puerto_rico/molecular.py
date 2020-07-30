#################################################################################
#
# Charts about molecular test data, which have their own logic
#

import altair as alt
import datetime
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.sql import select
from . import charts


class AbstractMolecularChart(charts.AbstractChart):
    def filter_data(self, df, created_date):
        return df.loc[df['created_date'] == pd.to_datetime(created_date)]


class NewPositiveRate(AbstractMolecularChart):
    SORT_ORDER = ['Positivas ÷ pruebas', 'Casos ÷ pruebas']

    def make_chart(self, df, created_date):
        return alt.Chart(df.dropna()).transform_filter(
            alt.datum.value > 0.0
        ).mark_line(
            point='transparent'
        ).encode(
            x=alt.X('collected_date:T', title='Fecha de muestra',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('value:Q', title='Positividad',
                    scale=alt.Scale(type='log'),
                    axis=alt.Axis(format='%')),
            color=alt.Color('variable:N', sort=self.SORT_ORDER,
                            legend=alt.Legend(orient='top', title=None)),
            strokeDash=alt.StrokeDash('created_date:T', sort='descending', legend=None),
            tooltip=[alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('created_date:T', title='Datos hasta'),
                     alt.Tooltip('value:Q', format=".2%", title='Tasa de positividad')]
        ).properties(
            width=585, height=250
        )

    def filter_data(self, df, created_date):
        effective_created_date = min(df['created_date'].max(), pd.to_datetime(created_date))
        week_ago = effective_created_date - datetime.timedelta(days=7)
        return df.loc[(df['created_date'] == effective_created_date)
                      | ((df['created_date'] == week_ago))]

    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_collected_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.created_date,
            table.c.collected_date,
            (table.c.smoothed_daily_positive_tests / table.c.smoothed_daily_tests)\
                .label('Positivas ÷ pruebas'),
            (table.c.smoothed_daily_cases / table.c.smoothed_daily_tests)\
                .label('Casos ÷ pruebas')
        ])
        df = pd.read_sql_query(query, connection, parse_dates=['created_date', 'collected_date'])
        return pd.melt(df, ['created_date', 'collected_date'])


class NewDailyTestsPerCapita(AbstractMolecularChart):
    POPULATION = 3_193_694
    POPULATION_THOUSANDS = POPULATION / 1_000.0

    def make_chart(self, df, created_date):
        return alt.Chart(df.dropna()).transform_calculate(
            per_thousand=alt.datum.value / self.POPULATION_THOUSANDS
        ).mark_line(
            point='transparent'
        ).encode(
            x=alt.X('collected_date:T', title='Fecha de toma de muestra',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('per_thousand:Q', title='Pruebas (por 1K)'),
            strokeDash=alt.StrokeDash('created_date:T', sort='descending', legend=None),
            tooltip=[alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('created_date:T', title='Datos hasta'),
                     alt.Tooltip('value:Q', format=".1f", title='Pruebas (promedio 7 días)'),
                     alt.Tooltip('per_thousand:Q', format=".2f",
                                 title='Pruebas por mil habitantes')]
        ).properties(
            width=585, height=250
        )

    def filter_data(self, df, created_date):
        effective_created_date = min(df['created_date'].max(), pd.to_datetime(created_date))
        week_ago = effective_created_date - datetime.timedelta(days=7)
        return df.loc[(df['created_date'] == effective_created_date)
                      | ((df['created_date'] == week_ago))]

    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_collected_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.created_date,
            table.c.collected_date,
            table.c.smoothed_daily_tests.label('value')
        ])
        return pd.read_sql_query(query, connection, parse_dates=["created_date", "collected_date"])


class CumulativeTestsVsCases(AbstractMolecularChart):
    POPULATION_MILLIONS = 3.193_694

    def fetch_data(self, connection):
        table = sqlalchemy.Table('tests_by_collected_date', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.created_date,
            table.c.collected_date,
            table.c.cumulative_tests,
            table.c.cumulative_cases,
            table.c.smoothed_daily_tests,
            table.c.smoothed_daily_cases
        ])
        return pd.read_sql_query(query, connection, parse_dates=['created_date', 'collected_date'])

    def filter_data(self, df, created_date):
        effective_created_date = min(df['created_date'].max(), pd.to_datetime(created_date))
        return df.loc[df['created_date'] == effective_created_date]

    def make_chart(self, df, created_date):
        max_x, max_y = 2_000, 140_000

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
            order=alt.Order('collected_date:T'),
            tooltip=[alt.Tooltip('yearmonthdate(collected_date):T', title='Fecha de muestra'),
                     alt.Tooltip('created_date:T', title='Datos hasta'),
                     alt.Tooltip('cumulative_tests:Q', format=",d",
                                 title='Pruebas moleculares'),
                     alt.Tooltip('cumulative_cases:Q', format=",d",
                                 title='Casos confirmados'),
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
            width=525, height=370
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


class MolecularCurrentDeltas(AbstractMolecularChart):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('bioportal_bitemporal_agg', self.metadata, autoload=True)
        query = select([table.c.created_date,
                        table.c.collected_date,
                        table.c.delta_molecular_tests.label('Pruebas'),
                        table.c.delta_positive_molecular_tests.label('Positivas')]
        )
        df = pd.read_sql_query(query, connection,
                               parse_dates=['created_date', 'collected_date'])
        return pd.melt(df, ['created_date', 'collected_date']).replace(0, np.NaN)

    def make_chart(self, df, created_date):
        base = alt.Chart(df).transform_joinaggregate(
            groupby=['variable'],
            min_value='min(value)',
            max_value='max(value)',
        ).transform_calculate(
            lo_mid_value='min(0, datum.min_value / 2.0)',
            hi_mid_value='max(0, datum.max_value / 2.0)'
        ).encode(
            x=alt.X('date(collected_date):O',
                    title="Día del mes", sort="descending",
                    axis=alt.Axis(format='%d')),
            y=alt.Y('month(collected_date):O',
                    title=None, sort="descending",
                    axis=alt.Axis(format='%B')),
            tooltip=[alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('created_date:T', title='Fecha de récord'),
                     alt.Tooltip('value:Q', title='Nuevas')]
        )

        heatmap = base.mark_rect().encode(
            color=alt.Color('value:Q', title=None, legend=None,
                            scale=alt.Scale(scheme="redgrey", domainMid=0,
                                            # WORKAROUND: Set the domain manually to forcibly
                                            # include zero or else we run into
                                            # https://github.com/vega/vega-lite/issues/6544
                                            domain=alt.DomainUnionWith(unionWith=[0])))
        )

        text = base.transform_filter(
            '(datum.value !== 0) & (datum.value !== null)'
        ).mark_text(fontSize=7).encode(
            text=alt.Text('value:Q'),
            color=alt.condition(
                '(datum.lo_mid_value < datum.value) & (datum.value < datum.hi_mid_value)',
                alt.value('black'),
                alt.value('white'))
        )

        return (heatmap + text).properties(
            width=585, height=70
        ).facet(
            columns=1,
            facet=alt.Facet('variable:N', title=None,
                            sort=['Pruebas', 'Positivas'])
        ).resolve_scale(color='independent')


class MolecularDailyDeltas(AbstractMolecularChart):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('bioportal_bitemporal_agg', self.metadata, autoload=True)
        query = select([table.c.created_date,
                        table.c.collected_date,
                        table.c.delta_molecular_tests.label('Pruebas'),
                        table.c.delta_positive_molecular_tests.label('Positivas')]
        )
        df = pd.read_sql_query(query, connection,
                               parse_dates=['created_date', 'collected_date'])
        return pd.melt(df, ['created_date', 'collected_date'])

    def filter_data(self, df, created_date):
        since_date = pd.to_datetime(created_date - datetime.timedelta(days=14))
        until_date = pd.to_datetime(created_date)
        filtered = df.loc[(since_date < df['created_date'])
                      & (df['created_date'] <= until_date)]\
            .replace(0, np.nan).dropna()
        return filtered

    def make_chart(self, df, created_date):
        base = alt.Chart(df).transform_joinaggregate(
            groupby=['variable'],
            min_value='min(value)',
            max_value='max(value)',
        ).transform_calculate(
            lo_mid_value='min(0, datum.min_value / 2.0)',
            hi_mid_value='max(0, datum.max_value / 2.0)'
        ).mark_rect().encode(
            x=alt.X('yearmonthdate(collected_date):O',
                    title='Fecha de toma de muestra', sort='descending',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('yearmonthdate(created_date):O',
                    title='Fecha de récord', sort='descending',
                    axis=alt.Axis(format='%d/%m')),
            tooltip=[alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('created_date:T', title='Fecha de récord en API'),
                     alt.Tooltip('value:Q', title='Nuevas')]
        )

        heatmap = base.mark_rect().encode(
            color=alt.Color('value:Q', title=None, legend=None,
                            scale=alt.Scale(scheme="redgrey", domainMid=0,
                                            # WORKAROUND: Set the domain manually to forcibly
                                            # include zero or else we run into
                                            # https://github.com/vega/vega-lite/issues/6544
                                            domain=alt.DomainUnionWith(unionWith=[0])))
        )

        text = base.mark_text(fontSize=2.75).encode(
            text=alt.Text('value:Q'),
            color=alt.condition(
                '(datum.lo_mid_value < datum.value) & (datum.value < datum.hi_mid_value)',
                alt.value('black'),
                alt.value('white'))
        )

        return (heatmap + text).properties(
            width=585, height=120
        ).facet(
            columns=1,
            facet=alt.Facet('variable:N', title=None,
                            sort=['Pruebas', 'Positivas'])
        ).resolve_scale(color='independent')


class MolecularLatenessDaily(AbstractMolecularChart):
    SORT_ORDER = ['Pruebas', 'Positivas']

    def fetch_data(self, connection):
        table = sqlalchemy.Table('molecular_lateness', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.created_date,
                        table.c.lateness_molecular_tests.label('Pruebas'),
                        table.c.lateness_positive_molecular_tests.label('Positivas')]
        )
        df = pd.read_sql_query(query, connection, parse_dates=['created_date'])
        return pd.melt(df, ['created_date'])

    def filter_data(self, df, created_date):
        since_date = pd.to_datetime(created_date - datetime.timedelta(days=8))
        until_date = pd.to_datetime(created_date)
        return df.loc[(since_date < df['created_date'])
                      & (df['created_date'] <= until_date)]

    def make_chart(self, df, created_date):
        bars = alt.Chart(df).mark_bar().encode(
            x=alt.X('value:Q', title='Rezago estimado (días)'),
            y=alt.Y('variable:N', title=None, sort=self.SORT_ORDER, axis=None),
            color=alt.Color('variable:N', sort=self.SORT_ORDER,
                            legend=alt.Legend(orient='bottom', title=None)),
            tooltip = [alt.Tooltip('created_date:T', title='Fecha de récord'),
                       alt.Tooltip('variable:N', title='Variable'),
                       alt.Tooltip('value:Q', format=".1f", title='Rezago promedio')]
        )

        text = bars.mark_text(
            align='right',
            baseline='middle',
            size=12,
            dx=-5
        ).encode(
            text=alt.Text('value:Q', format='.1f'),
            color = alt.value('white')
        )

        return (bars + text).properties(
            width=300
        ).facet(
            columns=2,
            facet=alt.Facet('created_date:T', sort='descending',
                            title='Fecha de récord')
        )

class MolecularLateness7Day(AbstractMolecularChart):
    SORT_ORDER = ['Pruebas', 'Positivas']

    def fetch_data(self, connection):
        table = sqlalchemy.Table('molecular_lateness', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.created_date,
            table.c.smoothed_lateness_molecular_tests.label('Pruebas'),
            table.c.smoothed_lateness_positive_molecular_tests.label('Positivas')]
        )
        df = pd.read_sql_query(query, connection, parse_dates=['created_date'])
        return pd.melt(df, ['created_date'])

    def filter_data(self, df, created_date):
        since_date = pd.to_datetime(created_date - datetime.timedelta(days=15))
        until_date = pd.to_datetime(created_date)
        return df.loc[(since_date < df['created_date'])
                      & (df['created_date'] <= until_date)]

    def make_chart(self, df, created_date):
        lines = alt.Chart(df).mark_line(
            strokeWidth=3,
            point=alt.OverlayMarkDef(size=50)
        ).encode(
            x=alt.X('yearmonthdate(created_date):O',
                    title="Fecha de récord",
                    axis=alt.Axis(format='%d/%m', titlePadding=10)),
            y=alt.Y('value:Q', title=None),
            color = alt.Color('variable', sort=self.SORT_ORDER, legend=None),
            tooltip=[alt.Tooltip('created_date:T', title='Fecha de récord'),
                     alt.Tooltip('variable:N', title='Variable'),
                     alt.Tooltip('value:Q', format=".1f", title='Rezago promedio')]
        )

        text = lines.mark_text(
            align='center',
            baseline='line-top',
            size=15,
            dy=10
        ).encode(
            text=alt.Text('value:Q', format='.1f')
        )

        return (lines + text).properties(
            width=575, height=37
        ).facet(
            row=alt.Row('variable', title=None, sort=self.SORT_ORDER)
        )
