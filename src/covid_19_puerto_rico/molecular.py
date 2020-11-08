#################################################################################
#
# Charts about molecular test data, which have their own logic
#

import altair as alt
import datetime
import numpy as np
import pandas as pd
import sqlalchemy
from covid_19_puerto_rico import util
from sqlalchemy import text
from sqlalchemy.sql import select
from . import charts


class AbstractMolecularChart(charts.AbstractChart):
    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]

class NewCases(AbstractMolecularChart):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('new_daily_cases', self.metadata,
                                 schema='covid_pr_etl', autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.bioportal_cases.label('Casos (Bioportal)'),
                        table.c.official_cases.label('Casos (oficial)'),
                        table.c.deaths.label('Muertes')])
        df = pd.read_sql_query(query, connection,
                               parse_dates=["bulletin_date", "datum_date"])
        return pd.melt(df, ["bulletin_date", "datum_date"])

    def filter_data(self, df, bulletin_date):
        week_ago = bulletin_date - datetime.timedelta(days=7)
        return df.loc[(df['bulletin_date'] == pd.to_datetime(bulletin_date))
                      | (df['bulletin_date'] == pd.to_datetime(week_ago))]

    def make_chart(self, df, bulletin_date):
        base = alt.Chart(df.dropna()).transform_window(
            groupby=['variable', 'bulletin_date'],
            sort=[{'field': 'datum_date'}],
            frame=[-6, 0],
            mean_value='mean(value)'
        ).transform_filter(
            alt.datum.mean_value > 0.0
        ).encode(
            x=alt.X('yearmonthdate(datum_date):T', title='Fecha de muestra o deceso',
                    axis=alt.Axis(format='%d/%m')),
            y = alt.Y('mean_value:Q', title='Casos nuevos (promedio 7 días)',
                      scale=alt.Scale(type='log')),
                tooltip = [
                alt.Tooltip('datum_date:T', title='Fecha muestra o muerte'),
                alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('mean_value:Q', format='.1f', title='Promedio 7 días')]
        )

        average = base.transform_filter(
            alt.datum.bulletin_date == util.altair_date_expr(bulletin_date)
        ).mark_line(point='transparent')

        week_ago = base.transform_filter(
            alt.datum.bulletin_date == util.altair_date_expr(bulletin_date - datetime.timedelta(days=7))
        ).mark_line(strokeDash=[6, 4], point='transparent')

        return (week_ago + average).encode(
            color=alt.Color('variable', title=None,
                            legend=alt.Legend(orient="top", labelLimit=250),
                            sort=['Casos (Bioportal)',
                                  'Casos (oficial)',
                                  'Muertes'])
        ).properties(
            width=585, height=400
        )


class ConfirmationsVsRejections(AbstractMolecularChart):
    """A more sophisticated version of the 'positive rate' concept, that uses
    Bioportal's `patientId` field to estimate how many tests are followups for
    persons who are already known to have tested positive."""

    SORT_ORDER = ['Oficial', 'Bioportal']

    def fetch_data(self, connection):
        table = sqlalchemy.Table('confirmed_vs_rejected', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.collected_date,
            table.c.rejections,
            table.c.cases.label('Casos oficiales'),
            table.c.novels.label('Bioportal')
        ])
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'collected_date'])
        return pd.melt(df, ['bulletin_date', 'collected_date', 'rejections'])

    def filter_data(self, df, bulletin_date):
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        week_ago = effective_bulletin_date - datetime.timedelta(days=7)
        return df.loc[(df['bulletin_date'] == effective_bulletin_date)
                      | ((df['bulletin_date'] == week_ago))]

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df.dropna()).transform_window(
            groupby=['bulletin_date', 'variable'],
            sort=[{'field': 'collected_date'}],
            frame=[-6, 0],
            sum_value='sum(value)',
            sum_rejections='sum(rejections)'
        ).transform_calculate(
            rate=alt.datum.sum_value / (alt.datum.sum_value + alt.datum.sum_rejections),
            ratio=alt.datum.sum_rejections / alt.datum.sum_value
        ).transform_filter(
            alt.datum.sum_value > 0
        ).mark_line(
            point='transparent'
        ).encode(
            x=alt.X('collected_date:T', title='Fecha de muestra'),
            y=alt.Y('rate:Q', title='% episodios que se confirma (7 días)',
                    scale=alt.Scale(type='log', domain=[0.001, 1.0]),
                    axis=alt.Axis(format='%')),
            color=alt.Color('variable:N', sort=self.SORT_ORDER,
                            legend=alt.Legend(orient='top', titleOrient='left',
                                              title='Según:', labelLimit=320)),
            strokeDash=alt.StrokeDash('bulletin_date:T', sort='descending', legend=None),
            tooltip=[alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('variable:N', title='Según'),
                     alt.Tooltip('ratio:Q', format=".1f", title='Rechazados / confirmados (7 días)'),
                     alt.Tooltip('rate:Q', format=".3p", title='% episodios que se confirma (7 días)')]
        ).properties(
            width=580, height=350
        )


class NaivePositiveRate(AbstractMolecularChart):
    SORT_ORDER = [
        'Positivas ÷ pruebas',
        'Casos ÷ pruebas'
    ]

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df.dropna()).transform_filter(
            alt.datum.value > 0.0
        ).mark_line(
            point='transparent'
        ).encode(
            x=alt.X('collected_date:T', title='Fecha de muestra',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('value:Q', title='Positividad',
                    scale=alt.Scale(type='log', domain=[0.001, 1.0]),
                    axis=alt.Axis(format='%')),
            color=alt.Color('variable:N', sort=self.SORT_ORDER,
                            legend=alt.Legend(orient='top', titleOrient='left',
                                              title='Método de cálculo:', labelLimit=320)),
            strokeDash=alt.StrokeDash('bulletin_date:T', sort='descending', legend=None),
            tooltip=[alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('variable:N', title='Método de cálculo'),
                     alt.Tooltip('value:Q', format=".2%", title='Tasa de positividad')]
        ).properties(
            width=580, height=350
        )

    def filter_data(self, df, bulletin_date):
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        week_ago = effective_bulletin_date - datetime.timedelta(days=7)
        return df.loc[(df['bulletin_date'] == effective_bulletin_date)
                      | ((df['bulletin_date'] == week_ago))]

    def fetch_data(self, connection):
        table = sqlalchemy.Table('naive_positive_rates', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.collected_date,
            (table.c.smoothed_daily_positives / table.c.smoothed_daily_tests)\
                .label('Positivas ÷ pruebas'),
            (table.c.smoothed_daily_cases / table.c.smoothed_daily_tests)\
                .label('Casos ÷ pruebas')
        ]).where(table.c.test_type == 'Molecular')
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'collected_date'])
        return pd.melt(df, ['bulletin_date', 'collected_date'])


class NewDailyTestsPerCapita(AbstractMolecularChart):
    POPULATION = 3_193_694
    POPULATION_THOUSANDS = POPULATION / 1_000.0
    TEST_TYPE_SORT_ORDER = ['Molecular', 'Serological', 'Antigens']
    DATE_TYPE_SORT_ORDER = ['Fecha de muestra', 'Fecha de reporte']

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df.dropna()).transform_calculate(
            per_thousand=alt.datum.value / self.POPULATION_THOUSANDS
        ).mark_line(
            point='transparent'
        ).encode(
            x=alt.X('date:T', title=None,
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('per_thousand:Q', title='Pruebas (por 1K)'),
            color=alt.Color('test_type:N', title='Tipo de prueba:', sort=self.TEST_TYPE_SORT_ORDER,
                            legend=alt.Legend(orient='bottom', titleOrient='top')),
            strokeDash=alt.StrokeDash('bulletin_date:T', sort='descending', legend=None),
            tooltip=[alt.Tooltip('test_type:N', title='Tipo de prueba'),
                     alt.Tooltip('date:T', title='Fecha'),
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('value:Q', format=",.1f", title='Pruebas (promedio 7 días)'),
                     alt.Tooltip('per_thousand:Q', format=".2f",
                                 title='Pruebas por mil habitantes')]
        ).properties(
            width=585, height=150
        ).facet(
            columns=1,
            facet=alt.Facet('date_type:N', title=None,
                            sort=self.DATE_TYPE_SORT_ORDER)
        )

    def filter_data(self, df, bulletin_date):
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        week_ago = effective_bulletin_date - datetime.timedelta(days=7)
        return df.loc[(df['bulletin_date'] == effective_bulletin_date)
                      | ((df['bulletin_date'] == week_ago))]

    def fetch_data(self, connection):
        table = sqlalchemy.Table('new_daily_tests', self.metadata, autoload=True)
        query = select([
            table.c.date_type,
            table.c.test_type,
            table.c.bulletin_date,
            table.c.date,
            table.c.smoothed_daily_tests.label('value')
        ])
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date", "date"])


class CumulativeTestsVsCases(AbstractMolecularChart):
    POPULATION_MILLIONS = 3.193_694

    def fetch_data(self, connection):
        table = sqlalchemy.Table('molecular_tests_vs_confirmed_cases', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.collected_date,
            table.c.cumulative_tests,
            table.c.cumulative_cases
        ])
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'collected_date'])

    def filter_data(self, df, bulletin_date):
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        return df.loc[df['bulletin_date'] == effective_bulletin_date]

    def make_chart(self, df, bulletin_date):
        max_x, max_y = 12_000, 300_000

        main = alt.Chart(df.dropna()).transform_calculate(
            tests_per_million=alt.datum.cumulative_tests / self.POPULATION_MILLIONS,
            cases_per_million=alt.datum.cumulative_cases / self.POPULATION_MILLIONS,
            case_positive_rate=alt.datum.cumulative_cases / alt.datum.cumulative_tests,
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
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('cumulative_tests:Q', format=",d",
                                 title='Pruebas moleculares'),
                     alt.Tooltip('cumulative_cases:Q', format=",d",
                                 title='Casos confirmados'),
                     alt.Tooltip('tests_per_million:Q', format=",d",
                                 title='Pruebas por millón'),
                     alt.Tooltip('cases_per_million:Q', format=",d",
                                 title='Casos por millón'),
                     alt.Tooltip('case_positive_rate:Q', format=".2%",
                                 title='Tasa de positividad (acumulada)')
                     ]
        )

        width = 525
        return (self.make_ref_chart(max_x, max_y) + main).properties(
            width=width, height=self.compute_height(width, max_x, max_y)
        )

    def compute_height(self, width, max_x, max_y):
        return int(((width / max_x) * max_y) / 36.6666666666)

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
            compute_points('five_pct', 0.05) + \
            compute_points('ten_pct', 0.10)
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
        table = sqlalchemy.Table('bioportal_collected_agg', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.collected_date,
                        table.c.delta_tests.label('Pruebas'),
                        table.c.delta_positive_tests.label('Positivas')]
        ).where(table.c.test_type == 'Molecular')
        df = pd.read_sql_query(query, connection,
                               parse_dates=['bulletin_date', 'collected_date'])
        return pd.melt(df, ['bulletin_date', 'collected_date']).replace(0, np.NaN)

    def make_chart(self, df, bulletin_date):
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
                     alt.Tooltip('bulletin_date:T', title='Fecha de récord'),
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
            width=580, height=70
        ).facet(
            columns=1,
            facet=alt.Facet('variable:N', title=None,
                            sort=['Pruebas', 'Positivas'])
        ).resolve_scale(color='independent')


class MolecularDailyDeltas(AbstractMolecularChart):
    def fetch_data(self, connection):
        table = sqlalchemy.Table('bioportal_collected_agg', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.collected_date,
                        table.c.delta_tests.label('Pruebas'),
                        table.c.delta_positive_tests.label('Positivas')]
        ).where(table.c.test_type == 'Molecular')
        df = pd.read_sql_query(query, connection,
                               parse_dates=['bulletin_date', 'collected_date'])
        return pd.melt(df, ['bulletin_date', 'collected_date'])

    def filter_data(self, df, bulletin_date):
        since_date = pd.to_datetime(bulletin_date - datetime.timedelta(days=14))
        until_date = pd.to_datetime(bulletin_date)
        filtered = df.loc[(since_date < df['bulletin_date'])
                      & (df['bulletin_date'] <= until_date)]\
            .replace(0, np.nan).dropna()
        return filtered

    def make_chart(self, df, bulletin_date):
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
            y=alt.Y('yearmonthdate(bulletin_date):O',
                    title='Fecha de récord', sort='descending',
                    axis=alt.Axis(format='%d/%m')),
            tooltip=[alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('bulletin_date:T', title='Fecha de récord en API'),
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
            width=580, height=120
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
        query = select([table.c.bulletin_date,
                        table.c.lateness_tests.label('Pruebas'),
                        table.c.lateness_positive_tests.label('Positivas')]
        ).where(table.c.test_type == 'Molecular')
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date'])
        return pd.melt(df, ['bulletin_date'])

    def filter_data(self, df, bulletin_date):
        since_date = pd.to_datetime(bulletin_date - datetime.timedelta(days=8))
        until_date = pd.to_datetime(bulletin_date)
        return df.loc[(since_date < df['bulletin_date'])
                      & (df['bulletin_date'] <= until_date)]

    def make_chart(self, df, bulletin_date):
        bars = alt.Chart(df).mark_bar().encode(
            x=alt.X('value:Q', title='Rezago estimado (días)'),
            y=alt.Y('variable:N', title=None, sort=self.SORT_ORDER, axis=None),
            color=alt.Color('variable:N', sort=self.SORT_ORDER,
                            legend=alt.Legend(orient='bottom', title=None)),
            tooltip = [alt.Tooltip('bulletin_date:T', title='Fecha de récord'),
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
            facet=alt.Facet('bulletin_date:T', sort='descending',
                            title='Fecha de récord')
        )

class MolecularLateness7Day(AbstractMolecularChart):
    SORT_ORDER = ['Pruebas', 'Positivas']

    def fetch_data(self, connection):
        table = sqlalchemy.Table('molecular_lateness', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.smoothed_lateness_tests.label('Pruebas'),
            table.c.smoothed_lateness_positive_tests.label('Positivas')]
        ).where(table.c.test_type == 'Molecular')
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date'])
        return pd.melt(df, ['bulletin_date'])

    def filter_data(self, df, bulletin_date):
        since_date = pd.to_datetime(bulletin_date - datetime.timedelta(days=15))
        until_date = pd.to_datetime(bulletin_date)
        return df.loc[(since_date < df['bulletin_date'])
                      & (df['bulletin_date'] <= until_date)]

    def make_chart(self, df, bulletin_date):
        lines = alt.Chart(df).mark_line(
            strokeWidth=3,
            point=alt.OverlayMarkDef(size=50)
        ).encode(
            x=alt.X('yearmonthdate(bulletin_date):O',
                    title="Fecha de récord",
                    axis=alt.Axis(format='%d/%m', titlePadding=10)),
            y=alt.Y('value:Q', title=None),
            color = alt.Color('variable', sort=self.SORT_ORDER, legend=None),
            tooltip=[alt.Tooltip('bulletin_date:T', title='Fecha de récord'),
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


class MolecularLatenessTiers(AbstractMolecularChart):
    SORT_ORDER = ['≤ 7 días', '≤ 14 días', '> 14 días']

    def fetch_data(self, connection):
        table = sqlalchemy.Table('molecular_lateness_tiers', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.tier,
            table.c.tier_order,
            table.c.count
        ])
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date'])

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] <= pd.to_datetime(bulletin_date)]

    def make_chart(self, df, bulletin_date):
        max_y = df.groupby(['bulletin_date'])['count'].sum()\
            .rolling(7).mean().max()

        base = alt.Chart(df).transform_joinaggregate(
            groupby=['bulletin_date'],
            total='sum(count)'
        ).transform_window(
            groupby=['tier'],
            sort=[{'field': 'bulletin_date'}],
            frame=[-6, 0],
            mean_count='mean(count)',
            mean_total='mean(total)'
        ).transform_calculate(
            percent=alt.datum.count / alt.datum.total,
            mean_percent=alt.datum.mean_count / alt.datum.mean_total
        ).mark_area(opacity=0.85).encode(
            color=alt.Color('tier:N', title='Renglón (días)', sort=self.SORT_ORDER,
                            legend=alt.Legend(orient='top', titleOrient='left', offset=5),
                            scale=alt.Scale(scheme='redyellowgreen', reverse=True)),
            order=alt.Order('tier_order:O'),
            tooltip=[alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                     alt.Tooltip('tier:N', title='Renglón'),
                     alt.Tooltip('count:Q', format=",d", title='Resultados en renglón (crudo)'),
                     alt.Tooltip('total:Q', format=",d", title='Total (crudo)'),
                     alt.Tooltip('mean_count:Q', format=".1f", title='Resultados en renglón (promedio 7)'),
                     alt.Tooltip('mean_total:Q', format=".1f", title='Total (promedio 7)'),
                     alt.Tooltip('mean_percent:Q', format=".1%", title='% de total (promedio 7)')]
        )

        absolute = base.encode(
            x=alt.X('bulletin_date:T', title=None, axis=alt.Axis(ticks=False, labels=False)),
            y=alt.Y('mean_count:Q', title='Resultados moleculares (promedio 7 días)',
                    scale=alt.Scale(domain=[0, max_y]),
                    axis=alt.Axis(labelExpr="if(datum.value > 0, datum.label, '')")),
        ).properties(
            width=575, height=275
        )

        normalized = base.encode(
            x=alt.X('bulletin_date:T', title='Fecha de boletín'),
            y=alt.Y('mean_count:Q', stack='normalize', title='% renglón',
                    axis=alt.Axis(format='%', labelExpr="if(datum.value < 1.0, datum.label, '')"))
        ).properties(
            width=575, height=75
        )

        return alt.vconcat(absolute, normalized, spacing=5)


class PublicHealthTrustLevels(AbstractMolecularChart):
    """COVID-19 alert levels (green, yellow, orange, red)
    as proposed by the Puerto Rico Public Health Trust."""

    POPULATION_100K = 31.936_94

    CASES_LEVELS = pd.DataFrame(data={
        'Nivel': ['Crítico', 'Alto', 'Medio', 'Bajo'],
        'Umbral': [100, 50, 10, 1],
        'Tope': [1000, 100, 50, 10],
        'Color': ['red', 'orange', 'yellow', 'green']
    })

    POSITIVITY_LEVELS = pd.DataFrame(data={
        'Nivel': ['Crítico', 'Alto', 'Medio', 'Bajo'],
        'Umbral': [0.1, 0.05, 0.03, 0.001],
        'Tope': [1.0, 0.1, 0.05, 0.03],
        'Color': ['red', 'orange', 'yellow', 'green']
    })

    def fetch_data(self, connection):
        query = text("""SELECT 
	bca.bulletin_date,
	bca.collected_date,
	bc.confirmed_cases,
	bca.tests,
	bca.positive_tests
FROM covid_pr_etl.bioportal_collected_agg bca
INNER JOIN covid_pr_etl.bulletin_cases bc
	ON bc.bulletin_date = bca.bulletin_date 
	AND bc.datum_date = bca.collected_date
WHERE bca.test_type = 'Molecular'
AND date_add('week', -11, bca.bulletin_date) <= bca.collected_date
AND bca.collected_date < date_add('week', -1, bca.bulletin_date)
ORDER BY bc.bulletin_date DESC, bc.datum_date DESC""")
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'collected_date'])

    def make_chart(self, df, bulletin_date):
        base = alt.Chart(df).transform_window(
            sort=[{'field': 'collected_date'}],
            frame=[-13, 0],
            sum_cases='sum(confirmed_cases)',
            sum_tests='sum(tests)',
            sum_positive_tests='sum(positive_tests)',
            groupby=['bulletin_date']
        ).transform_calculate(
            cases_per_100k=alt.datum.sum_cases / self.POPULATION_100K,
            cases_over_tests=alt.datum.sum_cases / alt.datum.sum_tests,
            positives_over_tests=alt.datum.sum_positive_tests / alt.datum.sum_tests
        ).transform_filter(
            alt.datum.collected_date >= util.altair_date_expr(bulletin_date - datetime.timedelta(weeks=9))
        )

        cases = base.mark_point(
            shape='square', color='black', clip=True
        ).encode(
            x=alt.X('collected_date:T', axis=alt.Axis(title=None, labels=False)),
            y=alt.Y('cases_per_100k:Q', title='Casos/100k',
                    scale=alt.Scale(domain=[1, 200], type='linear')),
            tooltip = [alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                       alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                       alt.Tooltip('cases_per_100k:Q', format=',.1f',
                                   title='Casos confirmados (14 días, por 100k)'),
                       alt.Tooltip('sum_cases:Q', format=',d',
                                   title='Casos confirmados (14 días, crudo)')]
        )

        positives_over_tests = base.mark_point(
            shape='circle', color='black', clip=True
        ).encode(
            x=alt.X('collected_date:T', title='Fecha de muestra'),
            y=alt.Y('positives_over_tests:Q', axis=alt.Axis(format='%'),
                    scale=alt.Scale(domain=[0.01, 0.2], type='linear'),
                    title='Positivad'),
            tooltip=[alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                     alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('positives_over_tests:Q', format='.1%',
                                 title='Positivas/pruebas (14 días)')]
        )

        cases_over_tests = base.mark_point(
            shape='diamond', color='black', clip=True
        ).encode(
            x=alt.X('collected_date:T', title='Fecha de muestra'),
            y=alt.Y('cases_over_tests:Q', title='Positivad',
                    scale=alt.Scale(domain=[0.01, 0.2], type='linear'),
                    axis=alt.Axis(format='%')),
            tooltip=[alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                     alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('cases_over_tests:Q', format='.1%',
                                 title='Casos/pruebas (14 días)')]
        )

        width, height = 585, 80
        return alt.vconcat(
            alt.layer(self.make_level_scale(self.CASES_LEVELS), cases).properties(
                width=width, height=height
            ),
            alt.layer(
                self.make_level_scale(self.POSITIVITY_LEVELS),
                positives_over_tests,
                cases_over_tests
            ).properties(
                width=width, height=height
            )
        )

    def make_level_scale(self, df):
        return alt.Chart(df).mark_rect(
            opacity=0.2, clip=True
        ).encode(
            y=alt.Y('Umbral:Q'),
            y2=alt.Y2('Tope:Q'),
            color=alt.Color('Color:N', scale=None)
        )