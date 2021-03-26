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
from sqlalchemy import text
from sqlalchemy.sql import select, and_
from . import charts


class AbstractMolecularChart(charts.AbstractChart):
    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]


class RecentCases(AbstractMolecularChart):
    POPULATION_100K = 31.93694
    SORT_ORDER=['Pruebas', 'Casos', 'Admisiones a hospital', 'Muertes']

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('recent_daily_cases', self.metadata,
                                 schema='covid_pr_etl', autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.tests.label('Pruebas'),
                        table.c.cases.label('Casos'),
                        table.c.admissions.label('Admisiones a hospital'),
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
            mean_7day_100k=alt.datum.mean_7day / self.POPULATION_100K,
            sum_7day_100k=alt.datum.sum_7day / self.POPULATION_100K,
            mean_14day_100k=alt.datum.mean_14day / self.POPULATION_100K,
            sum_14day_100k=alt.datum.sum_14day / self.POPULATION_100K
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
    POPULATION_100K = 31.93694

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('new_daily_cases', self.metadata,
                                 schema='covid_pr_etl', autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.rejections.label('Descartados'),
                        table.c.bioportal.label('Casos'),
                        # I don't trust the data for this one.
                        # Note for when/if I reenable: the color is
                        # '#f58518' (tableau10 orange)
                        # table.c.hospital_admissions.label('Hospitalizados'),
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
            mean_7day_100k=alt.datum.mean_7day / self.POPULATION_100K,
            sum_7day_100k=alt.datum.sum_7day / self.POPULATION_100K,
            mean_14day_100k=alt.datum.mean_14day / self.POPULATION_100K,
            sum_14day_100k=alt.datum.sum_14day / self.POPULATION_100K
        ).transform_filter(
            alt.datum.mean_7day > 0.0
        ).mark_line().encode(
            x=alt.X('yearmonthdate(datum_date):T', title='Fecha de muestra o deceso',
                    axis=alt.Axis(format='%d/%m')),
            y = alt.Y('mean_7day:Q', title='Nuevos (promedio 7 días)',
                      scale=alt.Scale(type='log'), axis=alt.Axis(format=',')),
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
            color=alt.Color('variable:N', title=None,
                            scale=alt.Scale(range=['#54a24b', '#4c78a8', '#e45756']),
                            legend=alt.Legend(orient='top', labelLimit=250,
                                              symbolStrokeWidth=3, symbolSize=300),
                            sort=['Descartados',
                                  'Casos',
                                  'Muertes',]),
            strokeDash=alt.StrokeDash('bulletin_date:T', title='Datos hasta', sort='descending',
                                      legend=alt.Legend(orient='bottom-right', symbolSize=300,
                                                        symbolStrokeWidth=2, symbolStrokeColor='black',
                                                        direction='vertical', fillColor='white',
                                                        padding=7.5))
        ).properties(
            width=575, height=475
        )


class ConfirmationsVsRejections(AbstractMolecularChart):
    """A more sophisticated version of the 'positive rate' concept, that uses
    Bioportal's `patientId` field to estimate how many tests are followups for
    persons who are already known to have tested positive."""

    SORT_ORDER = ['Oficial', 'Bioportal']

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
            x=alt.X('collected_date:T', title='Fecha de muestra'),
            y=alt.Y('rate:Q', title='% episodios que se confirma (7 días)',
                    scale=alt.Scale(type='log', domain=[0.002, 0.2]),
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
            x=alt.X('collected_date:T', title='Fecha de muestra',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('rate:Q', title='Positividad',
                    scale=alt.Scale(type='log', domain=[0.003, 0.2]),
                    axis=alt.Axis(format='%')),
            color=alt.Color('test_type:N', sort=self.SORT_ORDER, scale=alt.Scale(range=self.COLORS),
                            legend=alt.Legend(orient='bottom-right', legendX=50, legendY=4,
                                              fillColor='white', padding=7.5,
                                              symbolStrokeWidth=3, symbolSize=250,
                                              title='Tipo de prueba', labelLimit=320)),
            strokeDash=alt.StrokeDash('bulletin_date:T', sort='descending',
                                      legend=alt.Legend(orient='bottom-right',
                                                        fillColor='white', padding=7.5,
                                                        symbolStrokeWidth=2, symbolSize=250,
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
    POPULATION = 3_193_694
    POPULATION_THOUSANDS = POPULATION / 1_000.0
    TEST_TYPE_SORT_ORDER = ['Molecular', 'Serológica', 'Antígeno']
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
            x=alt.X('date:T', title=None,
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('mean_tests:Q', title='Especímenes diarios',
                    # We use axis tick increments of 3k because the population of
                    # Puerto Rico is about 3M, and this way we can easily read a
                    # rough per-capita figure
                    axis=alt.Axis(format='s', values=[0, 3000, 6000, 9000, 12000, 15000])),
            color=alt.Color('test_type:N', title='Tipo de prueba', sort=self.TEST_TYPE_SORT_ORDER,
                            legend=alt.Legend(orient='bottom', titleOrient='top', direction='vertical',
                                              padding=7.5, symbolStrokeWidth=3, symbolSize=300)),
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
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('bioportal_collected_agg', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.collected_date,
                        table.c.delta_tests.label('Pruebas'),
                        table.c.delta_positive_tests.label('Positivas')]
        ).where(and_(table.c.test_type == 'Molecular',
                     min(bulletin_dates) <= table.c.bulletin_date,
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
            '(datum.value !== 0) & (datum.value !== null)'
        ).mark_text(fontSize=7).encode(
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


class MolecularDailyDeltas(AbstractMolecularChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('bioportal_collected_agg', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.collected_date,
                        table.c.delta_tests.label('Pruebas'),
                        table.c.delta_positive_tests.label('Positivas')]
        ).where(and_(table.c.test_type == 'Molecular',
                     min(bulletin_dates) - datetime.timedelta(days=14) <= table.c.bulletin_date,
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
            x=alt.X('bulletin_date:T', title='Fecha de boletín'),
            y=alt.Y('mean_count:Q', stack='normalize', title='% renglón',
                    axis=alt.Axis(format='%', labelExpr="if(datum.value < 1.0, datum.label, '')"))
        ).properties(
            width=575, height=75
        )

        return alt.vconcat(absolute, normalized, spacing=5)


class CaseFatalityRate(AbstractMolecularChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('lagged_cfr', self.metadata,
                                 schema='covid_pr_etl', autoload=True)
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
            x=alt.X('collected_date:T', title='Fecha de muestras'),
            y=alt.Y('lagged_cfr:Q', title='Letalidad (CFR, 14 días)',
                    axis=alt.Axis(format='%'), scale=alt.Scale(type='log', domain=[0.007, 0.2])),
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
            width=585, height=275
        )


class Hospitalizations(AbstractMolecularChart):
    """Hospitalizations, based on HHS data we download."""

    SORT_ORDER = ['Hospitalizados', 'Cuidado intensivo']

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('hospitalizations', self.metadata,
                                 schema='covid_pr_etl', autoload=True)
        query = select([
            table.c.date,
            table.c.hospitalized_currently.label('Hospitalizados'),
            table.c.in_icu_currently.label('Cuidado intensivo'),
        ]).where(table.c.date <= max(bulletin_dates))
        df = pd.read_sql_query(query, connection, parse_dates=['date'])
        return pd.melt(df, ['date']).dropna()

    def filter_data(self, df, bulletin_date):
        return df.loc[df['date'] <= pd.to_datetime(bulletin_date)]

    def make_chart(self, df, bulletin_date):
        return alt.Chart(df).transform_window(
            sort=[{'field': 'date'}],
            frame=[-6, 0],
            mean_value='mean(value)',
            groupby=['variable']
        ).mark_line(point='transparent').encode(
            x=alt.X('date:T', title='Fecha'),
            y=alt.Y('mean_value:Q', title='Promedio 7 días',
                    scale=alt.Scale(type='log', domain=[5, 1000]),
                    axis=alt.Axis(format='s')),
            color=alt.Color('variable:N', title=None,
                            sort=self.SORT_ORDER,
                            legend=alt.Legend(orient='top')),
            tooltip=[
                alt.Tooltip('date:T', title='Fecha'),
                alt.Tooltip('variable:N', title='Variable'),
                alt.Tooltip('value:Q', title='Valor'),
                alt.Tooltip('mean_value:Q', title='Promedio 7 días', format=',.1f')
            ]
        ).properties(
            width=575, height=350
        )


class AgeGroups(AbstractMolecularChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('cases_by_age', self.metadata,
                                 schema='covid_pr_etl', autoload=True)
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
                    axis=alt.Axis(labelBaseline='line-bottom', tickBand='extent')),
            color=alt.Color('mean_cases_1m:Q', title='Casos diarios por millón',
                            scale=alt.Scale(scheme='lightmulti', type='sqrt'),
                            legend=alt.Legend(orient='top', gradientLength=WIDTH)),
            tooltip=[
                alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                alt.Tooltip('collected_date:T', title='Fecha de muestra'),
                alt.Tooltip('edades:N', title='Edad'),
                alt.Tooltip('mean_cases:Q', format='.1f', title='Casos diarios (7 días)'),
                alt.Tooltip('mean_cases_1m:Q', format='.1f', title='Casos (7 días, por millón)')
            ]
        ).properties(
            width=WIDTH, height=350
        )
