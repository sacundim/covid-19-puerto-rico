#################################################################################
#
# Charts about molecular test data, which have their own logic
#

import altair as alt
import datetime
import logging
import numpy as np
import pandas as pd
import sqlalchemy
from covid_19_puerto_rico import util
from sqlalchemy import func
from sqlalchemy import text
from sqlalchemy.sql import select, and_
from . import charts

# 2020 Census:
PUERTO_RICO_POPULATION = 3_285_874
PUERTO_RICO_POPULATION_100K = PUERTO_RICO_POPULATION / 1e5


class AbstractMolecularChart(charts.AbstractChart):
    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]


class RecentCases(AbstractMolecularChart):
    SORT_ORDER=['Pruebas', 'Casos', 'Hospitalizados', 'Muertes']

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('recent_daily_cases', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.tests.label('Pruebas'),
                        table.c.cases.label('Casos'),
                        table.c.hospitalized_currently.label('Hospitalizados'),
                        table.c.deaths.label('Muertes')])\
            .where(and_(min(bulletin_dates) - datetime.timedelta(days=7) <= table.c.bulletin_date,
                        table.c.bulletin_date <= max(bulletin_dates)))
        df = pd.read_sql_query(query, connection, parse_dates=["bulletin_date", "datum_date"])
        return pd.melt(df, ["bulletin_date", "datum_date"])

    def filter_data(self, df, bulletin_date):
        week_ago = bulletin_date - datetime.timedelta(days=7)
        return df.loc[(df['bulletin_date'] == pd.to_datetime(bulletin_date))
                      | (df['bulletin_date'] == pd.to_datetime(week_ago))]

    def make_chart(self, df, bulletin_date):
        base = alt.Chart(df).transform_window(
            groupby=['variable', 'bulletin_date'],
            sort=[{'field': 'datum_date'}],
            frame=[-6, 0],
            mean_7day='mean(value)',
            sum_7day = 'sum(value)'
        ).transform_window(
            groupby=['variable', 'bulletin_date'],
            sort=[{'field': 'datum_date'}],
            frame=[-13, 0],
            mean_14day = 'mean(value)',
            sum_14day='sum(value)'
        ).transform_calculate(
            mean_7day_100k=alt.datum.mean_7day / PUERTO_RICO_POPULATION_100K,
            sum_7day_100k=alt.datum.sum_7day / PUERTO_RICO_POPULATION_100K,
            mean_14day_100k=alt.datum.mean_14day / PUERTO_RICO_POPULATION_100K,
            sum_14day_100k=alt.datum.sum_14day / PUERTO_RICO_POPULATION_100K
        ).transform_filter(
            alt.datum.datum_date >= util.altair_date_expr(bulletin_date - datetime.timedelta(days=42))
        ).encode(
            x=alt.X('datum_date:T', title=None,
                    axis=alt.Axis(format='%-d/%-m', labelBound=True,
                                  labelAlign='right', labelOffset=4)),
            color=alt.Color('variable:N', legend=None, sort=self.SORT_ORDER,
                            scale=alt.Scale(range=['#54a24b', '#4c78a8', '#f58518', '#e45756'])),
            tooltip=[
                alt.Tooltip('datum_date:T', title='Fecha'),
                alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('value:Q', format=',d', title='Valor crudo'),
                alt.Tooltip('sum_14day:Q', format=',d', title='Suma 14 días'),
                alt.Tooltip('sum_14day_100k:Q', format=',.1f', title='Suma 14 días (/100k)'),
                alt.Tooltip('mean_14day:Q', format=',.1f', title='Promedio 14 días'),
                alt.Tooltip('mean_14day_100k:Q', format=',.1f', title='Promedio 14 días (/100k)'),
                alt.Tooltip('sum_7day:Q', format=',d', title='Suma 7 días'),
                alt.Tooltip('sum_7day_100k:Q', format=',.1f', title='Suma 7 días (/100k)'),
                alt.Tooltip('mean_7day:Q', format=',.1f', title='Promedio 7 días'),
                alt.Tooltip('mean_7day_100k:Q', format=',.1f', title='Promedio 7 días (/100k)')
            ]
        )

        bars = base.transform_filter(
            alt.datum.bulletin_date == util.altair_date_expr(bulletin_date)
        ).mark_bar(opacity=0.33).encode(
            y=alt.Y('value:Q', title=None,
                    axis=alt.Axis(minExtent=30, format='s', labelFlush=True,
                                  labelExpr="if(datum.value > 0, datum.label, '')"))
        )

        line = base.mark_line().encode(
            y=alt.Y('mean_7day:Q', title=None,
                    axis=alt.Axis(minExtent=40, format='s', labelFlush=True,
                                  labelExpr="if(datum.value > 0, datum.label, '')")),
            strokeDash=alt.StrokeDash('bulletin_date:T', sort='descending', title='Datos hasta',
                                      legend=alt.Legend(orient='bottom', titleOrient='left',
                                                        symbolSize=480, symbolStrokeWidth=2,
                                                        offset=10))
        )

        return alt.layer(bars, line).properties(
            width=285, height=112
        ).facet(
            columns=2,
            facet=alt.Facet('variable:N', title=None, sort=self.SORT_ORDER,
                            header=alt.Header(labelPadding=7.5))
        ).resolve_scale(
            y='independent'
        ).configure_facet(
            spacing=10
        ).configure_axis(
            labelFontSize=12
        )


class NewCases(AbstractMolecularChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('new_daily_cases', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.bioportal.label('Casos'),
                        # I don't trust the data for this one.
                        # Note for when/if I reenable: the color is
                        # '#f58518' (tableau10 orange)
                        table.c.hospital_admissions.label('Ingresos a hospital'),
                        table.c.hospitalized_currently.label('Ocupación hospital'),
                        table.c.in_icu_currently.label('Ocupación UCI'),
                        table.c.deaths.label('Muertes')])\
            .where(and_(min(bulletin_dates) - datetime.timedelta(days=7) <= table.c.bulletin_date,
                        table.c.bulletin_date <= max(bulletin_dates)))
        df = pd.read_sql_query(query, connection,
                               parse_dates=["bulletin_date", "datum_date"])
        return pd.melt(df, ["bulletin_date", "datum_date"])

    def filter_data(self, df, bulletin_date):
        week_ago = bulletin_date - datetime.timedelta(days=7)
        return df.loc[(df['bulletin_date'] == pd.to_datetime(bulletin_date))
                      | (df['bulletin_date'] == pd.to_datetime(week_ago))]

    def make_chart(self, df, bulletin_date):
        max_y = util.round_up_sig(df['value'].max())
        return alt.Chart(df.dropna()).transform_window(
            groupby=['variable', 'bulletin_date'],
            sort=[{'field': 'datum_date'}],
            frame=[-6, 0],
            mean_7day='mean(value)',
            sum_7day = 'sum(value)'
        ).transform_window(
            groupby=['variable', 'bulletin_date'],
            sort=[{'field': 'datum_date'}],
            frame=[-13, 0],
            mean_14day = 'mean(value)',
            sum_14day='sum(value)'
        ).transform_calculate(
            mean_7day_100k=alt.datum.mean_7day / PUERTO_RICO_POPULATION_100K,
            sum_7day_100k=alt.datum.sum_7day / PUERTO_RICO_POPULATION_100K,
            mean_14day_100k=alt.datum.mean_14day / PUERTO_RICO_POPULATION_100K,
            sum_14day_100k=alt.datum.sum_14day / PUERTO_RICO_POPULATION_100K
        ).transform_filter(
            alt.datum.mean_7day > 0.0
        ).mark_line().encode(
            x=alt.X('datum_date:T', timeUnit='yearmonthdate',
                    title='Fecha de suceso (muestra, hospitalización o deceso)',
                    axis=alt.Axis(
                        labelExpr="[timeFormat(datum.value, '%b'),"
                                  " timeFormat(datum.value, '%m') == '01'"
                                    " ? timeFormat(datum.value, '%Y')"
                                    " : '']")),
            y = alt.Y('mean_7day:Q', title='Promedio 7 días',
                      scale=alt.Scale(type='log', domain=[0.1, max_y]),
                      axis=alt.Axis(format=',')),
            tooltip = [
                alt.Tooltip('datum_date:T', title='Fecha muestra o muerte'),
                alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('sum_14day:Q', format=',d', title='Suma 14 días'),
                alt.Tooltip('sum_14day_100k:Q', format=',.1f', title='Suma 14 días (/100k)'),
                alt.Tooltip('mean_14day:Q', format=',.1f', title='Promedio 14 días'),
                alt.Tooltip('mean_14day_100k:Q', format=',.1f', title='Promedio 14 días (/100k)'),
                alt.Tooltip('sum_7day:Q', format=',d', title='Suma 7 días'),
                alt.Tooltip('sum_7day_100k:Q', format=',.1f', title='Suma 7 días (/100k)'),
                alt.Tooltip('mean_7day:Q', format=',.1f', title='Promedio 7 días'),
                alt.Tooltip('mean_7day_100k:Q', format=',.1f', title='Promedio 7 días (/100k)')],
            color=alt.Color('variable:N', title='Curva',
                            scale=alt.Scale(range=['#4c78a8', '#b279a2', '#eeca3b', '#f58518', '#e45756']),
                            legend=alt.Legend(orient='top', direction='horizontal', columns=2,
                                              labelLimit=250, symbolStrokeWidth=3, symbolSize=300),
                            sort=['Casos', 'Ingresos a hospital', 'Ocupación hospital',
                                  'Ocupación UCI', 'Muertes',]),
            strokeDash=alt.StrokeDash('bulletin_date:T', title='Datos hasta', sort='descending',
                                      legend=alt.Legend(orient='top', direction='vertical',
                                                        symbolSize=300, symbolStrokeWidth=2,
                                                        symbolStrokeColor='black'))
        ).properties(
            width=575, height=475
        )


class ConfirmationsVsRejections(AbstractMolecularChart):
    """A more sophisticated version of the 'positive rate' concept, that uses
    Bioportal's `patientId` field to estimate how many tests are followups for
    persons who are already known to have tested positive."""

    SORT_ORDER = ['Oficial', 'Bioestadísticas']

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('confirmed_vs_rejected', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.collected_date,
            table.c.rejections,
            table.c.novels
        ]).where(and_(min(bulletin_dates) - datetime.timedelta(days=7) <= table.c.bulletin_date,
                      table.c.bulletin_date <= max(bulletin_dates)))
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'collected_date'])

    def filter_data(self, df, bulletin_date):
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        week_ago = effective_bulletin_date - datetime.timedelta(days=7)
        return df.loc[(df['bulletin_date'] == effective_bulletin_date)
                      | ((df['bulletin_date'] == week_ago))]

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df.dropna()).transform_window(
            groupby=['bulletin_date'],
            sort=[{'field': 'collected_date'}],
            frame=[-6, 0],
            sum_novels='sum(novels)',
            sum_rejections='sum(rejections)'
        ).transform_calculate(
            rate=alt.datum.sum_novels / (alt.datum.sum_novels + alt.datum.sum_rejections),
            ratio=alt.datum.sum_rejections / alt.datum.sum_novels
        ).transform_filter(
            alt.datum.sum_novels > 0
        ).mark_line().encode(
            x=alt.X('collected_date:T', timeUnit='yearmonthdate', title='Fecha de muestra',
                    axis=alt.Axis(
                        labelExpr="[timeFormat(datum.value, '%b'),"
                                  " timeFormat(datum.value, '%m') == '01'"
                                    " ? timeFormat(datum.value, '%Y')"
                                    " : '']")),
            y=alt.Y('rate:Q', title='% episodios que se confirma (7 días)',
                    scale=alt.Scale(type='log', domain=[0.002, 0.5]),
                    axis=alt.Axis(format='%')),
            strokeDash=alt.StrokeDash('bulletin_date:T', sort='descending',
                                      legend=alt.Legend(orient='bottom-right', symbolSize=300,
                                                        symbolStrokeWidth=2, symbolStrokeColor='black',
                                                        direction='vertical', fillColor='white',
                                                        padding=7.5)),
            tooltip=[alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('ratio:Q', format=".1f", title='Descartados / confirmados (7 días)'),
                     alt.Tooltip('rate:Q', format=".3p", title='% episodios que se confirma (7 días)')]
        ).properties(
            width=580, height=350
        )


class NaivePositiveRate(AbstractMolecularChart):
    SORT_ORDER = ['Molecular', 'Antígeno']
    COLORS = ['#4c78a8', '#e45756']

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df).transform_window(
            groupby=['test_type', 'bulletin_date'],
            sort=[{'field': 'collected_date'}],
            frame=[-6, 0],
            sum_positives='sum(positives)',
            sum_tests='sum(tests)'
        ).transform_calculate(
            rate=alt.datum.sum_positives / alt.datum.sum_tests,
            ratio=alt.datum.sum_tests / alt.datum.sum_positives
        ).transform_filter(
            alt.datum.sum_positives > 0
        ).mark_line(clip=True).encode(
            x=alt.X('collected_date:T', timeUnit='yearmonthdate', title='Fecha de muestra',
                    axis=alt.Axis(
                        labelExpr="[timeFormat(datum.value, '%b'),"
                                  " timeFormat(datum.value, '%m') == '01'"
                                    " ? timeFormat(datum.value, '%Y')"
                                    " : '']")),
            y=alt.Y('rate:Q', title='Positividad',
                    scale=alt.Scale(type='log', domain=[0.003, 0.5]),
                    axis=alt.Axis(format='%')),
            color=alt.Color('test_type:N', sort=self.SORT_ORDER, scale=alt.Scale(range=self.COLORS),
                            legend=alt.Legend(orient='bottom', direction='vertical',
                                              padding=7.5, symbolStrokeWidth=3, symbolSize=250,
                                              title='Tipo de prueba', labelLimit=320)),
            strokeDash=alt.StrokeDash('bulletin_date:T', sort='descending',
                                      legend=alt.Legend(orient='bottom', direction='vertical',
                                                        padding=7.5,symbolStrokeWidth=2, symbolSize=250,
                                                        title='Datos hasta', labelLimit=320)),
            tooltip=[alt.Tooltip('test_type:O', title='Tipo de prueba'),
                     alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('ratio:Q', format=".1f", title='Razón de pruebas (7 días)'),
                     alt.Tooltip('rate:Q', format=".2%", title='Positividad (7 días)')]
        ).properties(
            width=580, height=400
        )

    def filter_data(self, df, bulletin_date):
        effective_bulletin_date = min(df['bulletin_date'].max(), pd.to_datetime(bulletin_date))
        week_ago = effective_bulletin_date - datetime.timedelta(days=7)
        return df.loc[(df['bulletin_date'] == effective_bulletin_date)
                      | ((df['bulletin_date'] == week_ago))]

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('naive_positive_rates', self.metadata, autoload=True)
        query = select([
            table.c.test_type,
            table.c.bulletin_date,
            table.c.collected_date,
            table.c.tests,
            table.c.positives
        ]).where(and_(min(bulletin_dates) - datetime.timedelta(days=7) <= table.c.bulletin_date,
                      table.c.bulletin_date <= max(bulletin_dates)))
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'collected_date'])


class NewTestSpecimens(AbstractMolecularChart):
    """The original tests chart, counting test specimens naïvely out of Bioportal."""
    POPULATION_THOUSANDS = PUERTO_RICO_POPULATION / 1_000.0
    TEST_TYPE_SORT_ORDER = ['Molecular', 'Antígeno', 'Casera', 'Serológica', 'Otro (¿inválido?)']
    TEST_TYPE_COLORS = ['#4c78a8', '#e45756', '#f58518', 'lightgray', 'gray']
    DATE_TYPE_SORT_ORDER = ['Fecha de muestra', 'Fecha de reporte']

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df.dropna()).transform_window(
            groupby=['test_type', 'bulletin_date', 'date_type', 'test_type'],
            sort=[{'field': 'date'}],
            frame=[-6, 0],
            mean_tests='mean(tests)'
        ).transform_calculate(
            per_thousand=alt.datum.mean_tests / self.POPULATION_THOUSANDS
        ).mark_line().encode(
            x=alt.X('date:T', timeUnit='yearmonthdate', title=None,
                    axis=alt.Axis(
                        labelExpr="[timeFormat(datum.value, '%b'),"
                                  " timeFormat(datum.value, '%m') == '01'"
                                    " ? timeFormat(datum.value, '%Y')"
                                    " : '']")),
            y=alt.Y('mean_tests:Q', title='Especímenes diarios',
                    # We use axis tick increments of 3k because the population of
                    # Puerto Rico is about 3M, and this way we can easily read a
                    # rough per-capita figure
                    axis=alt.Axis(format='s', values=list(range(0, 60001, 3000)))),
            color=alt.Color('test_type:N', title='Tipo de prueba',
                            sort=self.TEST_TYPE_SORT_ORDER,
                            scale=alt.Scale(range=self.TEST_TYPE_COLORS),
                            legend=alt.Legend(orient='bottom', titleOrient='top', direction='vertical',
                                              padding=7.5, symbolStrokeWidth=3, symbolSize=300, columns=2)),
            strokeDash=alt.StrokeDash('bulletin_date:T', sort='descending', title='Datos hasta',
                                      legend=alt.Legend(orient='bottom', titleOrient='top',
                                                        direction='vertical', padding=7.5,
                                                        symbolStrokeWidth=3, symbolSize=300)),
            tooltip=[alt.Tooltip('test_type:N', title='Tipo de prueba'),
                     alt.Tooltip('date:T', title='Fecha'),
                     alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('mean_tests:Q', format=",.1f", title='Especímenes (promedio 7 días)'),
                     alt.Tooltip('per_thousand:Q', format=".2f",
                                 title='Especímenes por mil habitantes')]
        ).properties(
            width=585, height=175
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

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('new_daily_tests', self.metadata, autoload=True)
        query = select([
            table.c.date_type,
            table.c.test_type,
            table.c.bulletin_date,
            table.c.date,
            table.c.tests
        ]).where(and_(min(bulletin_dates) - datetime.timedelta(days=7) <= table.c.bulletin_date,
                      table.c.bulletin_date <= max(bulletin_dates)))
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date", "date"])


class MolecularCurrentDeltas(AbstractMolecularChart):
    def save_chart(self, chart, basename):
        """Vegafusion 1.3.0 throws a Rust panic on this chart"""
        self.save_chart_vegafusion(chart, basename)

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('molecular_deltas', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.collected_date,
                        table.c.delta_tests.label('Pruebas'),
                        table.c.delta_positive_tests.label('Positivas')]
        ).where(and_(min(bulletin_dates) <= table.c.bulletin_date,
                     table.c.bulletin_date <= max(bulletin_dates)))
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
            y=alt.Y('yearmonth(collected_date):O',
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
            '(datum.value !== 0) && (datum.value !== null)'
        ).mark_text(fontSize=6).encode(
            text=alt.Text('value:Q'),
            color=alt.condition(
                '(datum.lo_mid_value < datum.value) && (datum.value < datum.hi_mid_value)',
                alt.value('black'),
                alt.value('white'))
        )

        return (heatmap + text).properties(
            width=580, height=200
        ).facet(
            columns=1,
            facet=alt.Facet('variable:N', title=None,
                            sort=['Pruebas', 'Positivas'])
        ).resolve_scale(color='independent')


class MolecularDailyDeltas(AbstractMolecularChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('molecular_deltas', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.collected_date,
                        table.c.delta_tests.label('Pruebas'),
                        table.c.delta_positive_tests.label('Positivas')]
        ).where(and_(min(bulletin_dates) - datetime.timedelta(days=14) <= table.c.bulletin_date,
                     table.c.bulletin_date <= max(bulletin_dates)))
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

        text = base.mark_text(fontSize=3, angle=270).encode(
            text=alt.Text('value:Q'),
            color=alt.condition(
                '(datum.lo_mid_value < datum.value) && (datum.value < datum.hi_mid_value)',
                alt.value('black'),
                alt.value('white'))
        )

        return (heatmap + text).properties(
            width=580, height=150
        ).facet(
            columns=1,
            facet=alt.Facet('variable:N', title=None,
                            sort=['Pruebas', 'Positivas'])
        ).resolve_scale(color='independent')


class MolecularLatenessDaily(AbstractMolecularChart):
    SORT_ORDER = ['Pruebas', 'Positivas']

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('molecular_lateness', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.lateness_tests.label('Pruebas'),
                        table.c.lateness_positive_tests.label('Positivas')]
        ).where(and_(table.c.test_type == 'Molecular',
                     min(bulletin_dates) - datetime.timedelta(days=8) <= table.c.bulletin_date,
                     table.c.bulletin_date <= max(bulletin_dates)))
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

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('molecular_lateness', self.metadata,
                                 schema='products', autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.smoothed_lateness_tests.label('Pruebas'),
            table.c.smoothed_lateness_positive_tests.label('Positivas')]
        ).where(and_(table.c.test_type == 'Molecular',
                     min(bulletin_dates) - datetime.timedelta(days=15) <= table.c.bulletin_date,
                     table.c.bulletin_date <= max(bulletin_dates)))
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
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('molecular_lateness_tiers', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.tier,
            table.c.tier_order,
            table.c.count
        ]).where(table.c.bulletin_date <= max(bulletin_dates))
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
            color=alt.Color('tier:N', title='Renglón (días)',
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
                    axis=alt.Axis(format='s', labelExpr="if(datum.value > 0, datum.label, '')")),
        ).properties(
            width=575, height=275
        )

        normalized = base.encode(
            x=alt.X('bulletin_date:T', timeUnit='yearmonthdate', title='Fecha de boletín',
                    axis=alt.Axis(
                        labelExpr="[timeFormat(datum.value, '%b'),"
                                  " timeFormat(datum.value, '%m') == '01'"
                                    " ? timeFormat(datum.value, '%Y')"
                                    " : '']")),
            y=alt.Y('mean_count:Q', stack='normalize', title='% renglón',
                    axis=alt.Axis(format='%', labelExpr="if(datum.value < 1.0, datum.label, '')"))
        ).properties(
            width=575, height=75
        )

        return alt.vconcat(absolute, normalized, spacing=5)


class CaseFatalityRate(AbstractMolecularChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('lagged_cfr', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.collected_date,
            table.c.smoothed_cases,
            table.c.death_date,
            table.c.smoothed_deaths,
            table.c.lagged_cfr
        ]).where(table.c.bulletin_date <= max(bulletin_dates))
        return pd.read_sql_query(query, connection,
                                 parse_dates=['bulletin_date', 'collected_date', 'death_date'])

    def filter_data(self, df, bulletin_date):
        week_ago = bulletin_date - datetime.timedelta(days=7)
        return df.loc[(df['bulletin_date'] == pd.to_datetime(bulletin_date))
                      | ((df['bulletin_date'] == pd.to_datetime(week_ago)))]

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df).mark_line(clip=True).transform_filter(
            alt.datum.lagged_cfr > 0.0
        ).encode(
            x=alt.X('collected_date:T', timeUnit='yearmonthdate', title='Fecha de muestra',
                    axis=alt.Axis(
                        labelExpr="[timeFormat(datum.value, '%b'),"
                                  " timeFormat(datum.value, '%m') == '01'"
                                    " ? timeFormat(datum.value, '%Y')"
                                    " : '']")),
            y=alt.Y('lagged_cfr:Q', title='Letalidad (CFR, 14 días)',
                    axis=alt.Axis(format='%'), scale=alt.Scale(type='log', domain=[0.001, 0.2])),
            strokeDash=alt.StrokeDash('bulletin_date:T', sort='descending', title='Datos hasta',
                                      legend=alt.Legend(orient='top', titleOrient='left',
                                                        symbolStrokeWidth=3, symbolSize=300)),
            tooltip=[alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('collected_date:T', title='Fecha de muestras'),
                     alt.Tooltip('smoothed_cases:Q', format=",.1f", title='Casos (promedio 14 días)'),
                     alt.Tooltip('death_date:T', title='Fecha de deceso'),
                     alt.Tooltip('smoothed_deaths:Q', format=",.1f", title='Muertes (promedio 14 días)'),
                     alt.Tooltip('lagged_cfr:Q', format=".2%", title='Letalidad (CFR, 14 días)')]
        ).properties(
            width=585, height=325
        )


class Hospitalizations(AbstractMolecularChart):
    """Hospitalizations, based on HHS data we download."""

    SORT_ORDER = ['Hospitalizados', 'Cuidado intensivo']

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('hospitalizations', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.date,
            table.c.hospitalized_currently.label('Hospitalizados'),
            table.c.in_icu_currently.label('Cuidado intensivo'),
        ]).where(table.c.date <= max(bulletin_dates) + datetime.timedelta(days=1))
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'date'])
        return pd.melt(df, ['bulletin_date', 'date']).dropna()

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df).transform_window(
            sort=[{'field': 'date'}],
            frame=[-6, 0],
            mean_value='mean(value)',
            groupby=['variable']
        ).mark_line(point='transparent').encode(
            x=alt.X('date:T', timeUnit='yearmonthdate', title='Fecha',
                    axis=alt.Axis(
                        labelExpr="[timeFormat(datum.value, '%b'),"
                                  " timeFormat(datum.value, '%m') == '01'"
                                    " ? timeFormat(datum.value, '%Y')"
                                    " : '']")),
            y=alt.Y('mean_value:Q', title='Promedio 7 días',
                    scale=alt.Scale(type='log', domain=[3, 1000]),
                    axis=alt.Axis(format='s')),
            color=alt.Color('variable:N', title=None,
                            sort=self.SORT_ORDER,
                            legend=alt.Legend(orient='top')),
            tooltip=[
                alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                alt.Tooltip('date:T', title='Fecha'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('value:Q', title='Valor'),
                alt.Tooltip('mean_value:Q', title='Promedio 7 días', format=',.1f')
            ]
        ).properties(
            width=575, height=350
        )

class RecentHospitalizations(AbstractMolecularChart):
    """Hospitalizations, based on PRDoH data we scrape."""

    SORT_ORDER = ['COVID', 'No COVID', 'Disponibles']
    COLORS = ['#d4322c', '#f58518', '#a4d86e']

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('prdoh_hospitalizations', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.date.label('Fecha'),
            table.c.age.label('Edad'),
            table.c.resource.label('Tipo'),
            table.c.total.label('Total'),
            table.c.covid.label('COVID'),
            table.c.nocovid.label('No COVID'),
            table.c.disp.label('Disponibles')
        ])
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'Fecha'])
        return pd.melt(df, ['bulletin_date', 'Fecha', 'Edad', 'Tipo', 'Total'])

    def make_chart(self, df, bulletin_date):
        area = alt.Chart(df).transform_calculate(
            order="if(datum.variable == 'COVID', 0, if(datum.variable == 'No COVID', 1, 2))",
            pct=alt.datum.value / alt.datum['Total']
        ).mark_area(opacity=0.85).encode(
            x=alt.X('Fecha:T', title=None,
                    axis=alt.Axis(format='%-d/%-m', labelBound=True,
                                  labelAlign='right', labelOffset=4)),
            y=alt.Y('value:Q', title=None, stack=True,
                    axis=alt.Axis(minExtent=30, format='s', labelFlush=True,
                                  labelExpr="if(datum.value > 0, datum.label, '')")),
            color=alt.Color('variable:N', title=None, sort=self.SORT_ORDER,
                            legend=alt.Legend(orient='top', columns=3, labelLimit=250),
                            scale=alt.Scale(range=self.COLORS)),
            order=alt.Order('order:O'),
            tooltip=[
                alt.Tooltip('Fecha:T'),
                alt.Tooltip('Edad:N'),
                alt.Tooltip('Tipo:N'),
                alt.Tooltip('Total:Q', format=',d'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('value:Q', format=',d', title='Valor'),
                alt.Tooltip('pct:Q', format='.1%', title='% del total')
            ]
        )

        threshold = alt.Chart(df).transform_calculate(
            threshold=alt.datum['Total'] * 0.70
        ).mark_line(color='gray', strokeWidth=1, strokeDash=[5, 3]).encode(
            x=alt.X('Fecha:T', title=None),
            y=alt.Y('threshold:Q', title=None)
        )

        return alt.layer(area, threshold).properties(
            width=275, height=175
        ).facet(
            row=alt.Row('Tipo:N', title=None),
            column=alt.Column('Edad:N', title=None)
        ).resolve_scale(
            y='independent'
        ).configure_facet(
            spacing=10
        ).configure_axis(
            labelFontSize=12
        )


class AgeGroups(AbstractMolecularChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('cases_by_age_5y', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.collected_date,
            table.c.youngest,
            table.c.cases,
            table.c.cases_1m
        ]).where(and_(min(bulletin_dates) <= table.c.bulletin_date,
                      table.c.bulletin_date <= max(bulletin_dates)))
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'collected_date'])

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]

    def make_chart(self, df, bulletin_date):
        WIDTH = 600
        return alt.Chart(df).transform_window(
            groupby=['youngest', 'bulletin_date'],
            sort=[{'field': 'collected_date'}],
            frame=[-6, 0],
            mean_cases='mean(cases)',
            mean_cases_1m='mean(cases_1m)'
        ).transform_impute(
            impute='mean_cases_1m',
            key='collected_date',
            groupby=['youngest'],
            value=0
        ).transform_calculate(
            oldest='if(datum.youngest < 85, datum.youngest + 4, null)',
            edades="if(datum.oldest == null, '≤ ' + datum.youngest, datum.youngest + ' a ' + datum.oldest)"
        ).mark_rect().encode(
            x=alt.X('collected_date:T', timeUnit='yearmonthdate', title='Fecha de muestra',
                    axis=alt.Axis(
                        labelExpr="[timeFormat(datum.value, '%b'),"
                                  " timeFormat(datum.value, '%m') == '01'"
                                    " ? timeFormat(datum.value, '%Y')"
                                    " : '']")),
            y=alt.Y('youngest:O', title='Edad',
                    axis=alt.Axis(labelBaseline='alphabetic',
                                  labelOverlap=True, tickBand='extent')),
            color=alt.Color('mean_cases_1m:Q', title='Casos diarios por millón', sort='descending',
                            scale=alt.Scale(scheme='spectral', reverse=True, type='symlog', constant=25),
                            legend=alt.Legend(orient='top', gradientLength=WIDTH,
                                              labelOverlap='greedy', labelSeparation=5,
                                              values=[10, 25, 50, 100, 250, 500, 1000, 2500, 5000])),
            tooltip=[
                alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                alt.Tooltip('edades:N', title='Edad'),
                alt.Tooltip('mean_cases:Q', format='.1f', title='Casos diarios (7 días)'),
                alt.Tooltip('mean_cases_1m:Q', format='.1f', title='Casos (7 días, por millón)')
            ]
        ).properties(
            width=WIDTH, height=225
        )


class RecentAgeGroups(AbstractMolecularChart):
    WIDTH = 260
    HEIGHT = 175
    DAYS=168

    def save_chart(self, chart, basename):
        """Vegafusion 1.3.0 throws a Rust panic on this chart"""
        self.save_chart_vegafusion(chart, basename)

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('recent_age_groups', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.collected_date,
            table.c.youngest,
            table.c.population,
            table.c.antigens.label('Antígenos'),
            table.c.molecular.label('Moleculares'),
            table.c.positive_antigens,
            table.c.positive_molecular,
            table.c.cases.label('Casos'),
            table.c.deaths.label('Muertes'),
            table.c.antigens_cases.label('Casos por antígeno'),
            table.c.molecular_cases.label('Casos por molecular'),
        ]).where(and_(min(bulletin_dates) <= table.c.bulletin_date,
                      table.c.bulletin_date <= max(bulletin_dates)))
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'collected_date'])

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]

    def make_chart(self, df, bulletin_date):
        return alt.vconcat(
            self.make_cases_charts(df, bulletin_date),
            self.make_tests_chart(df, bulletin_date),
            self.make_case_test_charts(df, bulletin_date),
            self.make_positivity_chart(df, bulletin_date),
            spacing=40
        ).resolve_scale(
            color='independent'
        )

    def make_tests_chart(self, df, bulletin_date):
        return alt.Chart(df).transform_fold(
            ['Antígenos', 'Moleculares'], as_=['variable', 'tests']
        ).transform_calculate(
            oldest='if(datum.youngest < 85, datum.youngest + 4, null)',
            edades="if(datum.oldest == null, '≤ ' + datum.youngest, datum.youngest + ' a ' + datum.oldest)",
            tests_1m=(alt.datum.tests / alt.datum.population) * 1e6
        ).transform_window(
            groupby=['youngest', 'bulletin_date', 'variable'],
            sort=[{'field': 'collected_date'}],
            frame=[-6, 0],
            mean_tests='mean(tests)',
            mean_tests_1m='mean(tests_1m)'
        ).transform_filter(
            alt.datum.collected_date >= util.altair_date_expr(bulletin_date - datetime.timedelta(days=self.DAYS))
        ).mark_rect().encode(
            x=alt.X('collected_date:T', timeUnit='yearmonthdate', title=None,
                    axis=alt.Axis(format='%-d/%-m')),
            y=alt.Y('youngest:O', title=None,
                    axis=alt.Axis(labelBaseline='alphabetic',
                                  labelOverlap=True, tickBand='extent')),
            color=alt.Color('mean_tests_1m:Q',
                            title='Pruebas (personas diarias por millón del grupo etario)',
                            scale=alt.Scale(scheme='spectral', type='log'),
                            legend=alt.Legend(orient='top', gradientLength=self.WIDTH * 2 + 70,
                                              labelOverlap=True, labelSeparation=5,
                                              titleLimit=self.WIDTH * 2 + 70)),
            tooltip=[
                alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                alt.Tooltip('edades:N', title='Edad'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('mean_tests:Q', format=',.1f', title='Pruebas diarias (7 días)'),
                alt.Tooltip('mean_tests_1m:Q', format=',d', title='Pruebas diarias (7 días, por millón)')
            ]
        ).properties(
            width=self.WIDTH, height=self.HEIGHT
        ).facet(
            columns=2,
            facet=alt.Facet('variable:N', title=None,
                            sort=['Antígenos', 'Moleculares'],
                            header=alt.Header(orient='right'))
        ).resolve_scale(
            x='independent',
            y='independent'
        )

    def make_cases_charts(self, df, bulletin_date):
        return alt.Chart(df).transform_fold(
            ['Casos', 'Muertes'], as_=['variable', 'incidences']
        ).transform_calculate(
            oldest='if(datum.youngest < 85, datum.youngest + 4, null)',
            edades="if(datum.oldest == null, '≤ ' + datum.youngest, datum.youngest + ' a ' + datum.oldest)",
            incidences_1m=(alt.datum.incidences / alt.datum.population) * 1e6
        ).transform_window(
            groupby=['youngest', 'bulletin_date', 'variable'],
            sort=[{'field': 'collected_date'}],
            frame=[-6, 0],
            mean_incidences='mean(incidences)',
            mean_incidences_1m='mean(incidences_1m)'
        ).transform_filter(
            alt.datum.collected_date >= util.altair_date_expr(bulletin_date - datetime.timedelta(days=self.DAYS))
        ).mark_rect().encode(
            x=alt.X('collected_date:T', timeUnit='yearmonthdate', title=None,
                    axis=alt.Axis(format='%-d/%-m')),
            y=alt.Y('youngest:O', title=None,
                    axis=alt.Axis(labelBaseline='alphabetic',
                                  labelOverlap=True, tickBand='extent')),
            color=alt.Color('mean_incidences_1m:Q', title='Diarios (por millón del grupo etario)',
                            scale=alt.Scale(scheme='spectral', reverse=True, type='symlog', constant=25),
                            legend=alt.Legend(orient='top', gradientLength=self.WIDTH,
                                              labelOverlap=True, labelSeparation=5,
                                              titleLimit=self.WIDTH)),
            tooltip=[
                alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                alt.Tooltip('edades:N', title='Edad'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('mean_incidences:Q', format=',.1f', title='Diarios (7 días)'),
                alt.Tooltip('mean_incidences_1m:Q', format=',d', title='Diarios (7 días, por millón)')
            ]
        ).properties(
            width=self.WIDTH, height=self.HEIGHT
        ).facet(
            columns=2,
            facet=alt.Facet('variable:N', title=None,
                            sort=['Casos', 'Muertes'],
                            header=alt.Header(orient='right'))
        ).resolve_scale(
            color='independent',
            x='independent',
            y='independent'
        )

    def make_case_test_charts(self, df, bulletin_date):
        return alt.Chart(df).transform_fold(
            ['Casos por antígeno', 'Casos por molecular'], as_=['variable', 'cases']
        ).transform_calculate(
            oldest='if(datum.youngest < 85, datum.youngest + 4, null)',
            edades="if(datum.oldest == null, '≤ ' + datum.youngest, datum.youngest + ' a ' + datum.oldest)",
            cases_1m=(alt.datum.cases / alt.datum.population) * 1e6
        ).transform_window(
            groupby=['youngest', 'bulletin_date', 'variable'],
            sort=[{'field': 'collected_date'}],
            frame=[-6, 0],
            mean_cases='mean(cases)',
            mean_cases_1m='mean(cases_1m)'
        ).transform_filter(
            alt.datum.collected_date >= util.altair_date_expr(bulletin_date - datetime.timedelta(days=self.DAYS))
        ).mark_rect().encode(
            x=alt.X('collected_date:T', timeUnit='yearmonthdate', title=None,
                    axis=alt.Axis(format='%-d/%-m')),
            y=alt.Y('youngest:O', title=None,
                    axis=alt.Axis(labelBaseline='alphabetic',
                                  labelOverlap=True, tickBand='extent')),
            color=alt.Color('mean_cases_1m:Q',
                            title='Casos diarios por tipo de prueba (por millón del grupo etario)',
                            scale=alt.Scale(scheme='spectral', reverse=True, type='symlog', constant=25),
                            legend=alt.Legend(orient='top', gradientLength=self.WIDTH * 2 + 70, labelOverlap=True,
                                              labelSeparation=5, titleLimit=self.WIDTH * 2 + 70,
                                              values=[10, 25, 50, 100, 250, 500, 1000, 2500])),
            tooltip=[
                alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                alt.Tooltip('edades:N', title='Edad'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('mean_cases:Q', format=',.1f', title='Diarios (7 días)'),
                alt.Tooltip('mean_cases_1m:Q', format=',d', title='Diarios (7 días, por millón)')
            ]
        ).properties(
            width=self.WIDTH, height=self.HEIGHT
        ).facet(
            columns=2,
            facet=alt.Facet('variable:N', title=None,
                            sort=['Casos por antígeno',
                                  'Casos por molecular'],
                            header=alt.Header(orient='right'))
        ).resolve_scale(
            x='independent',
            y='independent'
        )

    def make_positivity_chart(self, df, bulletin_date):
        return alt.Chart(df).transform_window(
            groupby=['youngest', 'bulletin_date', 'variable'],
            sort=[{'field': 'collected_date'}],
            frame=[-6, 0],
            sum_positive_antigens='sum(positive_antigens)',
            sum_antigens='sum(Antígenos)',
            sum_positive_molecular='sum(positive_molecular)',
            sum_molecular = 'sum(Moleculares)',
        ).transform_filter(
            alt.datum.collected_date >= util.altair_date_expr(bulletin_date - datetime.timedelta(days=self.DAYS))
        ).transform_calculate(
            oldest='if(datum.youngest < 85, datum.youngest + 4, null)',
            edades="if(datum.oldest == null, '≤ ' + datum.youngest, datum.youngest + ' a ' + datum.oldest)"
        ).transform_calculate(
            calculate=alt.datum.sum_positive_antigens / alt.datum.sum_antigens,
            as_='Antígenos'
        ).transform_calculate(
            calculate=alt.datum.sum_positive_molecular / alt.datum.sum_molecular,
            as_='Moleculares'
        ).transform_fold(
            ['Antígenos', 'Moleculares'], as_=['variable', 'value']
        ).mark_rect().encode(
            x=alt.X('collected_date:T', timeUnit='yearmonthdate', title=None,
                    axis=alt.Axis(format='%-d/%-m')),
            y=alt.Y('youngest:O', title=None,
                    axis=alt.Axis(labelBaseline='alphabetic',
                                  labelOverlap=True, tickBand='extent')),
            color=alt.Color('value:Q', title='Positividad (positivas / pruebas)',
                            sort='descending', scale=alt.Scale(scheme='spectral', type='sqrt'),
                            legend=alt.Legend(orient='top', gradientLength=self.WIDTH, format='%',
                                              labelOverlap=True, labelSeparation=5, titleLimit=self.WIDTH)),
            tooltip=[
                alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                alt.Tooltip('edades:N', title='Edad'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('value:Q', format='.3p', title='Positividad (7 días)')
            ]
        ).properties(
            width=self.WIDTH, height=self.HEIGHT
        ).facet(
            columns=2,
            facet=alt.Facet('variable:N', title=None,
                            sort=['Antígenos', 'Moleculares'],
                            header=alt.Header(orient='right'))
        ).resolve_scale(
            color='independent',
            x='independent',
            y='independent'
        )


class VaccinationMap(AbstractMolecularChart):
    FULL_WIDTH = 600
    FULL_HEIGHT = 250
    HALF_WIDTH = 280
    HALF_HEIGHT = 150

    def geography(self):
        return alt.InlineData(values=util.get_geojson_resource('municipalities.topojson'),
                              format=alt.TopoDataFormat(type='topojson', feature='municipalities'))

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('municipal_vaccinations', self.metadata, autoload=True)
        query = select([
            table.c.local_date,
            table.c.municipio,
            table.c.fips_code,
            table.c.pop2020,
            table.c.salud_total_dosis1,
            table.c.salud_total_dosis1_pct,
            table.c.salud_dosis1,
            table.c.salud_total_dosis2,
            table.c.salud_total_dosis2_pct,
            table.c.salud_dosis2,
            table.c.salud_total_dosis3,
            table.c.salud_total_dosis3_pct,
            table.c.salud_dosis3,
            table.c.salud_total_dosis,
            table.c.salud_total_dosis_per_100,
            table.c.salud_dosis,
        ])
        df = pd.read_sql_query(query, connection, parse_dates=["local_date"])

        # We really just do this to undo Pandas' null int to float conversion. See:
        #
        #     https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
        for col in ['salud_total_dosis1', 'salud_dosis1',
                    'salud_total_dosis2', 'salud_dosis2',
                    'salud_total_dosis3', 'salud_dosis3',
                    'salud_total_dosis', 'salud_dosis']:
            df[col] = df[col].astype('Int64')

        self.save_csv(df)
        return df

    def filter_data(self, df, bulletin_date):
        since_date = pd.to_datetime(bulletin_date - datetime.timedelta(days=7))
        until_date = pd.to_datetime(bulletin_date)
        return df.loc[(since_date < df['local_date'])
                      & (df['local_date'] <= until_date)]

    def make_chart(self, df, bulletin_date):
        cumulative = self.make_cumulative_chart(df, bulletin_date)
        rate = self.make_daily_rate_chart(df)
        return alt.vconcat(cumulative, rate).configure_view(
            strokeWidth=0
        ).configure_concat(
            spacing=40
        ).resolve_scale(
            color='independent'
        )

    def make_cumulative_chart(self, df, bulletin_date):
        complete = self.make_cumulative_subchart(
            df, bulletin_date, 'salud_total_dosis2', 'Personas con régimen completo'
        )
        boosted = self.make_cumulative_subchart(
            df, bulletin_date, 'salud_total_dosis3', 'Personas con refuerzo'
        )
        return alt.hconcat(complete, boosted).resolve_scale(
            color='shared'
        )

    def make_cumulative_subchart(self, df, bulletin_date, variable, title):
        DOMAIN_MID = 0.775

        return alt.Chart(df).transform_filter(
            alt.datum.local_date == util.altair_date_expr(bulletin_date)
        ).transform_calculate(
            pct=alt.datum[variable] / alt.datum.pop2020
        ).transform_lookup(
            lookup='municipio',
            from_=alt.LookupData(self.geography(), 'properties.NAME', ['type', 'geometry'])
        ).mark_geoshape(stroke='black', strokeWidth=0.25).project(
            type='mercator'
        ).encode(
            color=alt.Color('pct:Q', type='quantitative',
                            scale=alt.Scale(type='linear', scheme='blueorange', reverse=True,
                                            domain=alt.DomainUnionWith(unionWith=[1.0]),
                                            domainMid=DOMAIN_MID, clamp=True),
                            legend=alt.Legend(orient='top', titleLimit=400, titleOrient='top',
                                              title=title, format='.0%',
                                              gradientLength=self.FULL_WIDTH)),
            tooltip=[alt.Tooltip(field='local_date', type='temporal', title='Fecha'),
                     alt.Tooltip(field='municipio', type='nominal', title='Municipio'),
                     alt.Tooltip(field='pop2020', type='quantitative', format=',d', title='Población'),
                     alt.Tooltip(field=variable, title=title, type='quantitative', format=',d'),
                     alt.Tooltip(field='pct', type='quantitative', format='.1%', title='Porciento')]
        ).properties(
            width=self.HALF_WIDTH,
            height=self.HALF_HEIGHT
        )

    def make_daily_rate_chart(self, df):
        rate1 = self.make_daily_rate_subchart(
            df, 'salud_dosis1', 'Velocidad 1ra dosis', 0.007
        ).properties(
            width=self.HALF_WIDTH, height=self.HALF_HEIGHT
        )
        rate3 = self.make_daily_rate_subchart(
            df, 'salud_dosis3', 'Velocidad refuerzo', 0.007
        ).properties(
            width=self.HALF_WIDTH, height=self.HALF_HEIGHT
        )
        rate2 = self.make_daily_rate_subchart(
            df, 'salud_dosis2', 'Velocidad completos', 0.007
        ).properties(
            width=self.HALF_WIDTH, height=self.HALF_HEIGHT
        )
        rate_all = self.make_daily_rate_subchart(
            df, 'salud_dosis', 'Velocidad (todas)', 0.015
        ).properties(
            width=self.HALF_WIDTH, height=self.HALF_HEIGHT
        )
        return alt.concat(
            rate2, rate3,
            rate1, rate_all,
            columns=2
        ).resolve_scale(
            color='independent'
        )

    def make_daily_rate_subchart(self, df, variable, title, domain_hi):
        return alt.Chart(df).transform_calculate(
            dosis_pct=alt.datum[variable] / alt.datum.pop2020
        ).transform_aggregate(
            groupby=['municipio'],
            min_local_date='min(local_date)',
            max_local_date='max(local_date)',
            pop2020='mean(pop2020)',
            mean_dosis=f'mean({variable})',
            mean_rate_pct='mean(dosis_pct)'
        ).transform_lookup(
            lookup='municipio',
            from_=alt.LookupData(self.geography(), 'properties.NAME', ['type', 'geometry'])
        ).mark_geoshape(stroke='black', strokeWidth=0.25).project(
            type='mercator'
        ).encode(
            color=alt.Color('mean_rate_pct:Q', type='quantitative',
                            scale=alt.Scale(type='linear', scheme='blueorange', reverse=True,
                                            # We pick 0.7% as the domain top value because, looking
                                            # at Our World in Data, 1.4 doses/day/100 looks like a common
                                            # top rate that many countries have approached or achieved,
                                            # and almost none exceeded.  So half that because we have
                                            # a first doses and a final doses map.
                                            domain=[0, domain_hi], clamp=True),
                            legend=alt.Legend(orient='top', titleLimit=self.HALF_WIDTH, titleOrient='top',
                                              # labelSeparation=10,
                                              offset=-15, gradientLength=self.HALF_WIDTH,
                                              title=title, format='%')),
            tooltip=[alt.Tooltip(field='min_local_date', type='temporal', title='Desde'),
                     alt.Tooltip(field='max_local_date', type='temporal', title='Hasta'),
                     alt.Tooltip(field='municipio', type='nominal', title='Municipio'),
                     alt.Tooltip(field='pop2020', type='quantitative', format=',d', title='Población'),
                     alt.Tooltip(field='mean_dosis', type='quantitative', format=',.1f',
                                 title='Dosis diarias (promedio 7 días)'),
                     alt.Tooltip(field='mean_rate_pct', type='quantitative', format=',.2p',
                                 title='Dosis diarias como % de habitantes (promedio 7 días)')]
        )




class MunicipalVaccination(AbstractMolecularChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('municipal_vaccinations', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.municipio,
            table.c.population,
            table.c.total_dosis1.label('Primera dosis'),
            table.c.total_dosis2.label('Segunda dosis')
        ]).where(table.c.bulletin_date <= max(bulletin_dates))
        df = pd.read_sql_query(query, connection, parse_dates=["bulletin_date"])
        return pd.melt(df, ['bulletin_date', 'municipio', 'population'])

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] <= pd.to_datetime(bulletin_date)]

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df).mark_area().transform_calculate(
            pct=alt.datum.value / alt.datum.population
        ).encode(
            x=alt.X('bulletin_date:T', title='Fecha'),
            y=alt.Y('pct:Q', title='% de población', axis=alt.Axis(format='%')),
            color=alt.Color('variable:N', title='Dosis')
        ).properties(
            width=250, height=175
        ).facet(
            column=alt.Column('variable:N', title=None),
            row=alt.Row('municipio:N', title=None)
        )


class MunicipalSPLOM(AbstractMolecularChart):
    VARIABLES = [
        'Población', 'Ingreso',
        '<$10k', '≥$200k',
        '% blanco', '% negro',
        '1 dosis', '2 dosis',
        'Casos/1k', 'Pruebas/1k',
        'Antígenos/1k', '% +antígenos',
        'Moleculares/1k', '% +molecular'
    ]

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('municipal_splom', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.municipio,
            table.c.population.label('Población'),
            table.c.households_median.label('Ingreso'),
            table.c.households_lt_10k_pct.label('<$10k'),
            table.c.households_gte_200k_pct.label('≥$200k'),
            table.c.white_alone_pct.label('% blanco'),
            table.c.black_alone_pct.label('% negro'),
            table.c.total_dosis1_pct.label('1 dosis'),
            table.c.total_dosis2_pct.label('2 dosis'),
            table.c.cumulative_cases_1k.label('Casos/1k'),
            table.c.cumulative_specimens_1k.label('Pruebas/1k'),
            table.c.cumulative_antigens_1k.label('Antígenos/1k'),
            table.c.cumulative_antigen_positivity.label('% +antígenos'),
            table.c.cumulative_molecular_1k.label('Moleculares/1k'),
            table.c.cumulative_molecular_positivity.label('% +molecular'),
        ]).where(and_(min(bulletin_dates) <= table.c.bulletin_date,
                      table.c.bulletin_date <= max(bulletin_dates)))
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date"])

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df).mark_point().encode(
            x=alt.X(alt.repeat("column"), type='quantitative', axis=alt.Axis(labels=False)),
            y=alt.Y(alt.repeat("row"), type='quantitative', axis=alt.Axis(labels=False)),
            color=alt.Color('municipio:N', title='Municipio', legend=alt.Legend(symbolLimit=78)),
            tooltip=[
                alt.Tooltip(field='bulletin_date', type='temporal', title='Fecha'),
                alt.Tooltip(field='municipio', type='nominal', title='Municipio'),
                alt.Tooltip(field='Población', type='quantitative', format=',d'),
                alt.Tooltip(field='Ingreso', type='quantitative', format=',d'),
                alt.Tooltip(field='<$10k', type='quantitative', format=',.1%'),
                alt.Tooltip(field='≥$200k', type='quantitative', format=',.1%'),
                alt.Tooltip(field='% negro', type='quantitative', format=',.1%'),
                alt.Tooltip(field='% blanco', type='quantitative', format=',.1%'),
                alt.Tooltip(field='1 dosis', type='quantitative', format=',.1%'),
                alt.Tooltip(field='2 dosis', type='quantitative', format=',.1%'),
                alt.Tooltip(field='Casos/1k', type='quantitative', format=',.1f'),
                alt.Tooltip(field='Pruebas/1k', type='quantitative', format=',.1f'),
                alt.Tooltip(field='Moleculares/1k', type='quantitative', format=',.1f'),
                alt.Tooltip(field='% +molecular', type='quantitative', format=',.1%'),
                alt.Tooltip(field='Antígenos/1k', type='quantitative', format=',.1f'),
                alt.Tooltip(field='% +antígenos', type='quantitative', format=',.1%'),
            ]
        ).properties(
            width=100, height=100
        ).repeat(
            row=self.VARIABLES,
            column=list(reversed(self.VARIABLES))
        )


class EncounterLag(AbstractMolecularChart):
    WIDTH = 575
    SORT_ORDER = [
        'Pruebas (todas)', 'Pruebas (antígenos)', 'Pruebas (moleculares)',
        'Casos (todos)', 'Casos (antígenos)', 'Casos (moleculares)'
    ]

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('encounter_lag', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.age_gte,
            table.c.age_lt,
            table.c.delta_encounters.label('Pruebas_Todas'),
            table.c.delta_cases.label('Casos_Todas'),
            table.c.delta_antigens.label('Pruebas_Antígenos'),
            table.c.delta_antigens_cases.label('Casos_Antígenos'),
            table.c.delta_molecular.label('Pruebas_Moleculares'),
            table.c.delta_molecular_cases.label('Casos_Moleculares')
        ]).where(
            and_(min(bulletin_dates) - datetime.timedelta(days=49) <= table.c.bulletin_date,
                 table.c.bulletin_date <= max(bulletin_dates))
        )
        df1 = pd.read_sql_query(query, connection, parse_dates=['bulletin_date'])
        df2 = pd.wide_to_long(
            df1, ['Casos', 'Pruebas'],
            i=['bulletin_date', 'age_gte', 'age_lt'],
            j='test_type', sep='_', suffix='.*'
        )
        return pd.melt(df2.reset_index(), ['bulletin_date', 'test_type', 'age_gte', 'age_lt'])

    def filter_data(self, df, bulletin_date):
        since_date = pd.to_datetime(bulletin_date - datetime.timedelta(days=49))
        until_date = pd.to_datetime(bulletin_date)
        return df.loc[(since_date < df['bulletin_date'])
                      & (df['bulletin_date'] <= until_date)]

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df).transform_joinaggregate(
            groupby=['bulletin_date'],
            whole_bulletin_1d='sum(value)'
        ).transform_window(
            groupby=['age_lt'],
            sort=[{'field': 'bulletin_date'}],
            frame=[-6, 0],
            mean_whole_bulletin_7d='mean(whole_bulletin_1d)',
            mean_delta_value_7d='mean(value)'
        ).transform_calculate(
            range="if(datum.age_lt - 1 == datum.age_gte, datum.age_gte, datum.age_gte + ' a ' + (datum.age_lt - 1))",
            collected_since="timeOffset('day', datum.bulletin_date, -datum.age_lt + 1)",
            collected_until="timeOffset('day', datum.bulletin_date, -datum.age_gte)",
            smoothed_pct=alt.datum.mean_delta_value_7d / alt.datum.mean_whole_bulletin_7d
        ).transform_filter(
            alt.datum.bulletin_date >= util.altair_date_expr(bulletin_date - datetime.timedelta(days=42))
        ).mark_area(opacity=0.85).encode(
            x=alt.X('bulletin_date:T', timeUnit='yearmonthdate', title='Fecha de datos',
                    axis=alt.Axis(format='%-d/%-m', labelOverlap=True, labelSeparation=5, labelFontSize=11)),
            y=alt.Y('mean_delta_value_7d:Q', stack='normalize', title=None,
                    axis=alt.Axis(labelFontSize=11, orient='right', format='%')),
            order=alt.Order('age_gte:O'),
            color=alt.Color('age_gte:Q', title='Rezago entre muestra y Bioestadísticas (días)',
                            scale=alt.Scale(scheme='redyellowgreen', reverse=True,
                                            domain=[0, 14], domainMid=2, clamp=True),
                            legend=alt.Legend(orient='top', gradientLength=self.WIDTH,
                                              titleLimit=self.WIDTH, format='d')),
            tooltip=[
                alt.Tooltip('bulletin_date:T', title='Fecha de datos'),
                alt.Tooltip('collected_since:T', title='Muestras desde'),
                alt.Tooltip('collected_until:T', title='Muestras hasta'),
                alt.Tooltip('test_type:N', title='Tipo de prueba'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('range:N', title='Rezago (días)'),
                alt.Tooltip('value:Q', format=',d', title='Añadidos (crudo)'),
                alt.Tooltip('mean_delta_value_7d:Q', format=',.1f', title='Añadidos (promedio 7 días)'),
                alt.Tooltip('smoothed_pct:Q', format='.1%', title='Porciento (7 días)')
            ]
        ).properties(
            width=175, height=110
        ).facet(
            column=alt.Column('test_type:N', title=None,
                              sort=['Todas', 'Moleculares', 'Antígenos']),
            row=alt.Row('variable:N', title=None, sort=['Pruebas', 'Casos'],
                        header=alt.Header(orient='left'))
        ).resolve_scale(
            y='shared'
        )


class MunicipalTestingScatter(AbstractMolecularChart):
    WIDTH = 575

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('municipal_testing_scatterplot', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.municipality,
            table.c.abbreviation,
            table.c.population,
            table.c.daily_specimens,
            table.c.daily_antigens,
            table.c.antigens_positivity,
            table.c.daily_molecular,
            table.c.molecular_positivity,
        ]).where(
            and_(min(bulletin_dates) <= table.c.bulletin_date,
                 table.c.bulletin_date <= max(bulletin_dates))
        )
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date'])

    def make_chart(self, df, bulletin_date):
        molecular_positivity = \
            (df['molecular_positivity'] * df['daily_molecular']).sum() \
                / df['daily_molecular'].sum()

        base = alt.Chart(df).transform_calculate(
            collected_since="timeOffset('day', datum.bulletin_date, -20)",
            daily_specimens_1k='1000 * datum.daily_specimens / datum.population',
            daily_antigens_1k='1000 * datum.daily_antigens / datum.population',
            daily_molecular_1k='1000 * datum.daily_molecular / datum.population',
            daily_molecular_pct='datum.daily_molecular / datum.daily_specimens'
        ).encode(
            x=alt.X('daily_specimens_1k:Q', title='Especímenes por millar (promedio 21 días)',
                    scale=alt.Scale(type='log', nice=False), axis=alt.Axis(labelFlush=False)),
            y=alt.Y('daily_molecular_pct:Q', title='% de volumen en moleculares',
                    scale=alt.Scale(type='log', nice=False), axis=alt.Axis(format='%')),
            tooltip=[
                alt.Tooltip('municipality:N', title='Municipio'),
                alt.Tooltip('population:Q', format=',d', title='Población'),
                alt.Tooltip('collected_since:T', title='Muestras desde'),
                alt.Tooltip('bulletin_date:T', title='Muestras hasta'),
                alt.Tooltip('daily_specimens:Q', format=',.1f', title='Especímenes diarios'),
                alt.Tooltip('daily_specimens_1k:Q', format=',.1f', title='Especímenes diarios (/1k)'),
                alt.Tooltip('daily_antigens:Q', format=',.1f', title='Antígenos diarios'),
                alt.Tooltip('daily_antigens_1k:Q', format=',.1f', title='Antígenos diarios (/1k)'),
                alt.Tooltip('antigens_positivity:Q', format='.1%', title='Positividad de antígenos'),
                alt.Tooltip('daily_molecular:Q', format=',.1f', title='Moleculares diarias'),
                alt.Tooltip('daily_molecular_1k:Q', format=',.1f', title='Moleculares diarias (/1k)'),
                alt.Tooltip('molecular_positivity:Q', format='.1%', title='Positividad de moleculares'),
                alt.Tooltip('daily_molecular_pct:Q', format=',.1%', title='% de volumen en moleculares')
            ]
        )

        scatter = base.mark_point(filled=True, size=90).encode(
            color=alt.Color('molecular_positivity:Q', title='Positividad de moleculares (21 días)',
                            scale=alt.Scale(scheme='turbo', domainMid=molecular_positivity),
                            legend=alt.Legend(orient='top', format='%', labelSeparation=25,
                                              gradientLength=self.WIDTH, titleLimit=self.WIDTH))
        )

        text = base.mark_text(
            align='center', fontWeight='bold', fontSize=9, angle=0, dy=11
        ).encode(
            text=alt.Text('abbreviation:N')
        )

        mean_specimens = alt.Chart(df).transform_aggregate(
            sum_population='sum(population)',
            sum_daily_specimens='sum(daily_specimens)'
        ).transform_calculate(
            sum_daily_specimens_1k='1000 * datum.sum_daily_specimens / datum.sum_population'
        ).mark_rule(strokeWidth=0.5, strokeDash=[3, 2], clip=True).encode(
            x=alt.X('sum_daily_specimens_1k:Q')
        )

        pct_molecular = alt.Chart(df).transform_aggregate(
            sum_daily_molecular='sum(daily_molecular)',
            sum_daily_specimens = 'sum(daily_specimens)'
        ).transform_calculate(
            pct_molecular='datum.sum_daily_molecular / datum.sum_daily_specimens'
        ).mark_rule(strokeWidth=0.5, strokeDash=[3, 2], clip=True).encode(
            y=alt.Y('pct_molecular:Q')
        )

        return alt.layer(mean_specimens, pct_molecular, text, scatter).properties(
            width=self.WIDTH, height=self.WIDTH
        )