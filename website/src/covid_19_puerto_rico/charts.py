from abc import ABC, abstractmethod
import altair as alt
import datetime
import logging
import numpy as np
import pandas as pd
from pathlib import Path
import sqlalchemy
from sqlalchemy.sql import select, text, and_

from . import util


class AbstractChart(ABC):
    def __init__(self, engine, output_dir,
                 output_formats=frozenset(['json'])):
        self.engine = engine
        self.metadata = sqlalchemy.MetaData(engine)
        self.output_dir = output_dir
        self.output_formats = output_formats
        self.name = type(self).__name__

    def __call__(self, bulletin_dates):
        self.render(bulletin_dates)
        return self.__class__.__name__

    def render(self, bulletin_dates):
        with self.engine.connect() as connection:
            df = self.fetch_data(connection, bulletin_dates)
        logging.info("%s dataframe: %s", self.name, util.describe_frame(df))

        logging.info(f'Writing {self.name} charts to {self.output_dir}...')
        for bulletin_date in bulletin_dates:
            self.render_bulletin_date(df, bulletin_date)

    def render_bulletin_date(self, df, bulletin_date):
        bulletin_dir = Path(f'{self.output_dir}/{bulletin_date}')
        bulletin_dir.mkdir(exist_ok=True)
        filtered = self.filter_data(df, bulletin_date)
        util.save_chart(self.make_chart(filtered, bulletin_date),
                        f"{bulletin_dir}/{bulletin_date}_{self.name}",
                        self.output_formats)

    def save_csv(self, df):
        """Utility method to save a CSV file to a standardized location."""
        csv_dir = Path(f'{self.output_dir}/csv')
        csv_dir.mkdir(exist_ok=True)
        csv_file = f'{csv_dir}/{self.name}.csv'
        df.to_csv(csv_file, index=False)
        logging.info('Wrote %s', csv_file)

    @abstractmethod
    def make_chart(self, df, bulletin_date):
        pass

    @abstractmethod
    def fetch_data(self, connection, bulletin_dates):
        pass

    def filter_data(self, df, bulletin_date):
        """Filter dataframe according to given bulletin_date.  May want to override."""
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]


class AbstractMismatchChart(AbstractChart):
    def filter_data(self, df, bulletin_date):
        until = pd.to_datetime(bulletin_date)
        return df.loc[df['bulletin_date'] <= until]

    def make_chart(self, df, bulletin_date):
        base = alt.Chart(df).encode(
            x=alt.X('date(bulletin_date):O',
                    title="Día del mes", sort="descending",
                    axis=alt.Axis(format='%d')),
            y=alt.Y('yearmonth(bulletin_date):O',
                    title=None, sort="descending",
                    axis=alt.Axis(format='%B')),
            tooltip=['bulletin_date:T', 'value']
        )

        heatmap = base.mark_rect().encode(
            color=alt.Color('value:Q', title=None, legend=None,
                            scale=alt.Scale(scheme="redgrey", domainMid=0,
                                            # WORKAROUND: Set the domain manually to forcibly
                                            # include zero or else we run into
                                            # https://github.com/vega/vega-lite/issues/6544
                                            domain=alt.DomainUnionWith(unionWith=[0])))
        )

        text = base.mark_text(fontSize=9).encode(
            text=alt.Text('value:Q'),
            color=util.heatmap_text_color(df, 'value')
        )

        return (heatmap + text).properties(
            width=575, height=120
        ).facet(
            columns=1,
            facet=alt.Facet('variable', title=None,
                            sort=['Confirmados',
                                  'Probables',
                                  'Muertes'])
        )


class ConsecutiveBulletinMismatch(AbstractMismatchChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('mismatched_announcement_aggregates', self.metadata,
                                 schema='quality', autoload=True)
        query = select([table.c.bulletin_date,
                        (table.c.cumulative_confirmed_cases
                           - table.c.computed_cumulative_confirmed_cases)\
                           .label('confirmed_cases_mismatch'),
                        (table.c.cumulative_probable_cases
                           - table.c.computed_cumulative_probable_cases)\
                           .label('probable_cases_mismatch'),
                        (table.c.cumulative_deaths
                           - table.c.computed_cumulative_deaths)\
                           .label('deaths_mismatch'),
                        ]).where(table.c.bulletin_date <= max(bulletin_dates))
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date'])
        df = df.rename(columns={
            'confirmed_cases_mismatch': 'Confirmados',
            'probable_cases_mismatch': 'Probables',
            'deaths_mismatch': 'Muertes'
        })
        return pd.melt(df, ['bulletin_date']).dropna()



class BulletinChartMismatch(AbstractMismatchChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('mismatched_announcement_and_chart', self.metadata,
                                 schema='quality', autoload=True)
        query = select([table.c.bulletin_date,
                        (table.c.cumulative_confirmed_cases - table.c.sum_confirmed_cases)\
                           .label('confirmed_cases_mismatch'),
                        (table.c.cumulative_probable_cases - table.c.sum_probable_cases)\
                           .label('probable_cases_mismatch'),
                        (table.c.cumulative_deaths - table.c.sum_deaths)\
                           .label('deaths_mismatch'),
                        ]).where(table.c.bulletin_date <= max(bulletin_dates))
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date'])
        df = df.rename(columns={
            'confirmed_cases_mismatch': 'Confirmados',
            'probable_cases_mismatch': 'Probables',
            'deaths_mismatch': 'Muertes'
        })
        return pd.melt(df, ['bulletin_date']).dropna()


class AbstractLateness(AbstractChart):
    def fetch_data_for_table(self, connection, table, min_bulletin_date, max_bulletin_date):
        query = select([table.c.bulletin_date,
                        table.c.confirmed_cases_additions,
                        table.c.probable_cases_additions,
                        table.c.deaths_additions]
        ).where(and_(min_bulletin_date <= table.c.bulletin_date,
                     table.c.bulletin_date <= max_bulletin_date))
        df = pd.read_sql_query(query, connection,
                               parse_dates=["bulletin_date"])
        df = df.rename(columns={
            'confirmed_cases_additions': 'Confirmados',
            'probable_cases_additions': 'Probables',
            'deaths_additions': 'Muertes'
        })
        return pd.melt(df, "bulletin_date")


class LatenessDaily(AbstractLateness):
    def make_chart(self, df, bulletin_date):
        sort_order = ['Confirmados',
                      'Probables',
                      'Muertes']
        bars = alt.Chart(df).mark_bar().encode(
            x=alt.X('value', title="Rezago estimado (días)"),
            y=alt.Y('variable', title=None, sort=sort_order, axis=None),
            color=alt.Color('variable', sort=sort_order,
                            legend=alt.Legend(orient='bottom', title=None)),
            tooltip=[alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
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
            width=300,
        ).facet(
            columns=2,
            facet=alt.Facet("bulletin_date", sort="descending", title="Fecha del boletín")
        )


    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('lateness_daily', self.metadata,
                                 schema='products', autoload=True)
        min_bulletin_date = min(bulletin_dates) - datetime.timedelta(days=8)
        max_bulletin_date = max(bulletin_dates)
        return self.fetch_data_for_table(connection, table, min_bulletin_date, max_bulletin_date)

    def filter_data(self, df, bulletin_date):
        since_date = pd.to_datetime(bulletin_date - datetime.timedelta(days=8))
        until_date = pd.to_datetime(bulletin_date)
        return df.loc[(since_date < df['bulletin_date'])
                      & (df['bulletin_date'] <= until_date)]


class Lateness7Day(AbstractLateness):
    def make_chart(self, df, bulletin_date):
        sort_order = ['Confirmados',
                      'Probables',
                      'Muertes']
        lines = alt.Chart(df).mark_line(
            strokeWidth=3,
            point=alt.OverlayMarkDef(size=50)
        ).encode(
            x=alt.X('yearmonthdate(bulletin_date):O',
                    title="Fecha boletín",
                    axis=alt.Axis(format='%d/%m', titlePadding=10)),
            y=alt.Y('value:Q', title=None),
            color = alt.Color('variable', sort=sort_order, legend=None),
            tooltip=[alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
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
            width=600, height=33
        ).facet(
            columns=1,
            facet=alt.Facet('variable', title=None, sort=sort_order)
        )

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('lateness_7day', self.metadata,
                                 schema='products', autoload=True)
        min_bulletin_date = min(bulletin_dates) - datetime.timedelta(days=8)
        max_bulletin_date = max(bulletin_dates)
        return self.fetch_data_for_table(connection, table, min_bulletin_date, max_bulletin_date)

    def filter_data(self, df, bulletin_date):
        since_date = pd.to_datetime(bulletin_date - datetime.timedelta(days=15))
        until_date = pd.to_datetime(bulletin_date)
        return df.loc[(since_date < df['bulletin_date'])
                      & (df['bulletin_date'] <= until_date)]


class CurrentDeltas(AbstractChart):
    def make_chart(self, df, bulletin_date):
        base = alt.Chart(df).encode(
            x=alt.X('date(datum_date):O',
                    title="Día del mes", sort="descending",
                    axis=alt.Axis(format='%d')),
            y=alt.Y('yearmonth(datum_date):O',
                    title=None, sort="descending",
                    axis=alt.Axis(format='%B')),
            tooltip=[alt.Tooltip('datum_date:T', title='Fecha de muestra o muerte'),
                     alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                     alt.Tooltip('value:Q', title='Casos añadidos (o restados)')]
        )

        heatmap = base.mark_rect().encode(
            color=alt.Color('value:Q', title=None, legend=None,
                            scale=alt.Scale(scheme="redgrey", domainMid=0,
                                            # WORKAROUND: Set the domain manually to forcibly
                                            # include zero or else we run into
                                            # https://github.com/vega/vega-lite/issues/6544
                                            domain=alt.DomainUnionWith(unionWith=[0])))
        )

        text = base.mark_text(fontSize=7).encode(
            text=alt.Text('value:Q'),
            color=util.heatmap_text_color(df, 'value')
        ).transform_filter("(datum.value !== 0) & (datum.value !== null)")

        return (heatmap + text).properties(
            width=580, height=200
        ).facet(
            columns=1,
            facet=alt.Facet('variable', title=None,
                            sort=['Confirmados',
                                  'Probables',
                                  'Muertes'])
        )

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('bulletin_cases', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.delta_confirmed_cases,
                        table.c.delta_probable_cases,
                        table.c.delta_deaths]
        ).where(and_(min(bulletin_dates) <= table.c.bulletin_date,
                     table.c.bulletin_date <= max(bulletin_dates)))
        df = pd.read_sql_query(query, connection,
                               parse_dates=["bulletin_date", "datum_date"])
        df = df.rename(columns={
            'delta_confirmed_cases': 'Confirmados',
            'delta_probable_cases': 'Probables',
            'delta_deaths': 'Muertes'
        })
        return pd.melt(df, ["bulletin_date", "datum_date"]) \
            .replace(0, np.nan)


class DailyDeltas(AbstractChart):
    def make_chart(self, df, bulletin_date):
        base = alt.Chart(df).mark_rect().encode(
            x=alt.X('yearmonthdate(datum_date):O',
                    title="Fecha evento", sort="descending",
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('yearmonthdate(bulletin_date):O',
                    title=None, sort="descending",
                    axis=alt.Axis(format='%d/%m')),
            tooltip=[alt.Tooltip('datum_date:T', title='Fecha de muestra o muerte'),
                     alt.Tooltip('bulletin_date:T', title='Fecha de boletín'),
                     alt.Tooltip('value:Q', title='Casos añadidos (o restados)')]
        )

        heatmap = base.mark_rect().encode(
            color=alt.Color('value:Q', title=None, legend=None,
                            scale=alt.Scale(scheme="redgrey", domainMid=0,
                                            # WORKAROUND: Set the domain manually to forcibly
                                            # include zero or else we run into
                                            # https://github.com/vega/vega-lite/issues/6544
                                            domain=alt.DomainUnionWith(unionWith=[0])))
        )

        text = base.mark_text(fontSize=3).encode(
            text=alt.Text('value:Q'),
            color=util.heatmap_text_color(df, 'value')
        )

        return (heatmap + text).properties(
            width=585, height=120
        ).facet(
            columns=1,
            facet=alt.Facet('variable', title=None,
                            sort=['Confirmados',
                                  'Probables',
                                  'Muertes'])
        )

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('bulletin_cases', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.delta_confirmed_cases,
                        table.c.delta_probable_cases,
                        table.c.delta_deaths]
        ).where(and_(min(bulletin_dates) - datetime.timedelta(days=14) <= table.c.bulletin_date,
                     table.c.bulletin_date <= max(bulletin_dates)))
        df = pd.read_sql_query(query, connection,
                               parse_dates=["bulletin_date", "datum_date"])
        df = df.rename(columns={
            'delta_confirmed_cases': 'Confirmados',
            'delta_probable_cases': 'Probables',
            'delta_deaths': 'Muertes'
        })
        return pd.melt(df, ["bulletin_date", "datum_date"])

    def filter_data(self, df, bulletin_date):
        since_date = pd.to_datetime(bulletin_date - datetime.timedelta(days=14))
        until_date = pd.to_datetime(bulletin_date)
        filtered = df.loc[(since_date < df['bulletin_date'])
                      & (df['bulletin_date'] <= until_date)]\
            .replace(0, np.nan)\
            .dropna()
        return filtered


class WeekdayBias(AbstractChart):
    def make_chart(self, df, bulletin_date):
        confirmed = self.one_variable(df, 'Confirmados', 'Día muestra', 'oranges')
        probable = self.one_variable(df, 'Probables', 'Día muestra', 'reds')
        deaths = self.one_variable(df, 'Muertes', 'Día muerte', 'teals')

        data_date = alt.Chart(df).mark_text(baseline='middle').encode(
            text=alt.Text('bulletin_date',
                          type='temporal',
                          aggregate='max',
                          timeUnit='yearmonthdate',
                          format='Datos hasta: %A %d de %B, %Y'),
        ).properties(
            width=330, height=40
        )

        row1 = alt.hconcat(confirmed, probable, spacing=20).resolve_scale(
            color='independent'
        )

        return alt.vconcat(row1, data_date, deaths, center=True).resolve_scale(
            color='independent'
        )

    def one_variable(self, df, variable,
                     axis_title,
                     color_scheme):
        base = alt.Chart(df).transform_filter(
            alt.datum.variable == variable
        ).transform_filter(
            alt.datum.value > 0
        ).encode(
            color=alt.Color('sum(value):Q', title=None,
                            scale=alt.Scale(type='log', base=2, scheme=color_scheme))
        )

        heatmap = base.mark_rect().encode(
            x=alt.X('day(datum_date):O', title=axis_title),
            y=alt.Y('day(bulletin_date):O', title='Día boletín'),
            tooltip=[alt.Tooltip('variable:N', title='Variable'),
                     alt.Tooltip('day(bulletin_date):O', title='Día de boletín'),
                     alt.Tooltip('day(datum_date):O', title='Día de muestra o muerte'),
                     alt.Tooltip('value:Q', aggregate='sum', title='Casos')]
        )

        right = base.mark_bar().encode(
            x=alt.X('sum(value):Q', title=None, axis=None),
            y=alt.Y('day(bulletin_date):O', title=None, axis=None),
            tooltip=[alt.Tooltip('variable:N', title='Variable'),
                     alt.Tooltip('day(bulletin_date):O', title='Día de boletín'),
                     alt.Tooltip('value:Q', aggregate='sum', title='Casos')]
        )

        top = base.mark_bar().encode(
            x=alt.X('day(datum_date):O', title=None, axis=None),
            y=alt.Y('sum(value):Q', title=None, axis=None),
            tooltip=[alt.Tooltip('variable:N', title='Variable'),
                     alt.Tooltip('day(datum_date):O', title='Día de muestra o muerte'),
                     alt.Tooltip('value:Q', aggregate='sum', title='Casos')]
        )

        heatmap_size = 150
        histogram_size = 40
        return alt.vconcat(
            top.properties(
                width=heatmap_size, height=histogram_size,
                # This title should logically belong to the whole chart,
                # but assigning it to the concat chart anchors it wrong.
                # See: https://altair-viz.github.io/user_guide/generated/core/altair.TitleParams.html
                title=alt.TitleParams(
                    text=variable,
                    anchor='middle',
                    align='center',
                    fontSize=14,
                    fontWeight='normal'
                )
            ),
            alt.hconcat(
                heatmap.properties(
                    width=heatmap_size, height=heatmap_size
                ),
                right.properties(
                    width=histogram_size, height=heatmap_size
                ),
                spacing=3),
            spacing=3
        )

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('weekday_bias', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.delta_confirmed_cases.label('Confirmados'),
                        table.c.delta_probable_cases.label('Probables'),
                        table.c.delta_deaths.label('Muertes')])\
            .where(and_(min(bulletin_dates) - datetime.timedelta(days=22) <= table.c.bulletin_date,
                        table.c.bulletin_date <= max(bulletin_dates)))
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'datum_date'])
        return pd.melt(df, ['bulletin_date', 'datum_date']).dropna()

    def filter_data(self, df, bulletin_date):
        # We exclude the current bulletin_date because this chart's
        # main use is to compare the current bulletin's data to trends
        # established **before** it.
        cutoff_date = bulletin_date - datetime.timedelta(days=1)
        since_date = pd.to_datetime(cutoff_date - datetime.timedelta(days=21))
        until_date = pd.to_datetime(cutoff_date)
        return df.loc[(since_date < df['bulletin_date'])
                          & (df['bulletin_date'] <= until_date)]


class Municipal(AbstractChart):
    def make_chart(self, df, bulletin_date):
        WIDTH = 525
        return alt.Chart(df).transform_calculate(
            new_cases_1m='1e6 * datum.new_cases / datum.pop2020'
        ).transform_impute(
            impute='new_cases_1m',
            groupby=['bulletin_date', 'municipality'],
            key='sample_date',
            value=0
        ).transform_window(
            groupby=['bulletin_date', 'municipality'],
            sort=[{'field': 'sample_date'}],
            frame=[-6, 0],
            mean_cases='mean(new_cases)',
            mean_cases_1m='mean(new_cases_1m)'
        ).transform_window(
            groupby=['bulletin_date', 'municipality'],
            sort=[{'field': 'sample_date'}],
            frame=[-20, 0],
            mean_cases_1m_21day='sum(new_cases_1m)'
        ).transform_window(
            groupby=['bulletin_date', 'municipality'],
            sort=[{'field': 'sample_date'}],
            frame=[None, None],
            order_value='last_value(mean_cases_1m_21day)'
        ).transform_filter(
            alt.datum.sample_date >= util.altair_date_expr(bulletin_date - datetime.timedelta(days=84))
        ).mark_rect().encode(
            x=alt.X('sample_date:T', timeUnit='yearmonthdate', title='Fecha de muestra',
                    axis=alt.Axis(format='%-d/%-m', labelFontSize=10,
                                  labelBound=True, labelAlign='right', labelOffset=4)),
            y=alt.Y('municipality:N', title=None,
                    axis=alt.Axis(tickBand='extent', labelFontSize=10),
                    sort=alt.Sort(op='sum', field='order_value', order='descending')),
            color=alt.Color('mean_cases_1m:Q', title='Casos diarios por millón',
                            scale=alt.Scale(scheme='spectral', reverse=True, type='symlog', constant=25),
                            legend=alt.Legend(orient='top', gradientLength=WIDTH,
                                              labelOverlap='greedy', labelSeparation=5,
                                              values=[10, 25, 50, 100, 250, 500, 1000])),
            tooltip=[alt.Tooltip('bulletin_date:T', title='Datos hasta'),
                     alt.Tooltip('sample_date:T', title='Fecha de muestra'),
                     alt.Tooltip('municipality:N', title='Municipio'),
                     alt.Tooltip('pop2020:Q', format=',d', title='Population'),
                     alt.Tooltip('new_cases:Q', format=',d', title='Casos crudos'),
                     alt.Tooltip('mean_cases:Q', format=',.1f', title='Casos diarios (prom. 7)'),
                     alt.Tooltip('mean_cases_1m:Q', format=',d', title='Casos por millón (prom. 7)')]
        ).properties(
            width=WIDTH
        ).facet(
            columns=1,
            row=alt.Row('region:N', title=None,
                        header=alt.Header(orient='right'),
                        # TODO: compute and use a regional population weighted mean for sorting
                        sort=alt.Sort(op='mean', field='order_value', order='descending'))
        ).resolve_scale(
            x='independent', y='independent'
        )

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('cases_municipal_agg', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.sample_date,
                        table.c.region,
                        table.c.municipality,
                        table.c.pop2020,
                        table.c.new_cases])\
            .where(and_(min(bulletin_dates) - datetime.timedelta(days=97) <= table.c.sample_date,
                        table.c.sample_date <= max(bulletin_dates)))
        return pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'sample_date'])


class MunicipalMap(AbstractChart):
    WIDTH = 600

    def make_chart(self, df, bulletin_date):
        new_cases = self.make_cases_chart(df)
        growth = self.make_trend_chart(df)
        return alt.vconcat(new_cases, growth).configure_view(
            strokeWidth=0
        ).configure_concat(
            spacing=40
        ).resolve_scale(
            color='independent'
        )

    def make_cases_chart(self, df):
        return self.make_subchart(
            df,
            alt.Color('daily_cases_100k', type='quantitative',
                      scale=alt.Scale(type='sqrt', scheme='redgrey', reverse=True,
                                      clamp=True, domainMid=0,
                                      # WORKAROUND: Set the domain manually to forcibly
                                      # include zero or else we run into
                                      # https://github.com/vega/vega-lite/issues/6544
                                      #
                                      # Also, we union with 40 so that the scale always
                                      # goes at least that high, but if values exceed
                                      # that then we let the data determine the top
                                      # of the domain.
                                      domain=alt.DomainUnionWith(unionWith=[0, 40])),
                      legend=alt.Legend(orient='top', titleLimit=400, titleOrient='top',
                                        title='Casos diarios (por 100k de población, promedio 7 días)',
                                        offset=-15, labelSeparation=10,
                                        format=',~r', gradientLength=self.WIDTH)))

    def make_trend_chart(self, df):
        return self.make_subchart(
            df,
            alt.Color('trend:Q', type='quantitative', sort='descending',
                      scale=alt.Scale(type='symlog', scheme='redgrey',
                                      domainMid=0.0, clamp=True,
                                      domain=alt.DomainUnionWith(unionWith=[-1.0, 10.0])),
                      legend=alt.Legend(orient='top', titleLimit=400, titleOrient='top',
                                        title='Cambio (7 días más recientes vs. 7 anteriores)',
                                        offset=-15, labelSeparation=10,
                                        format='+,.0%', gradientLength=self.WIDTH)))


    def make_subchart(self, df, color):
        return alt.Chart(df).transform_lookup(
            lookup='municipality',
            from_=alt.LookupData(self.geography(), 'properties.NAME', ['type', 'geometry'])
        ).transform_calculate(
            daily_cases=alt.datum.new_7day_cases / 7.0,
            daily_cases_100k=((alt.datum.new_7day_cases * 1e5) / alt.datum.pop2020) / 7.0,
            trend='(datum.new_7day_cases / if(datum.previous_7day_cases == 0, 1, datum.previous_7day_cases)) - 1.0'
        ).mark_geoshape(stroke='black', strokeWidth=0.25).encode(
            color=color,
            tooltip=[alt.Tooltip(field='bulletin_date', type='temporal', title='Fecha de boletín'),
                     alt.Tooltip(field='municipality', type='nominal', title='Municipio'),
                     alt.Tooltip(field='pop2020', type='quantitative', format=',d', title='Población'),
                     alt.Tooltip(field='daily_cases', type='quantitative', format=',.1f',
                                 title='Casos (prom. 7 días)'),
                     alt.Tooltip(field='daily_cases_100k', type='quantitative', format=',.1f',
                                 title='Casos/100k (prom. 7 días)'),
                     alt.Tooltip(field='trend', type='quantitative', format='+,.0%', title='Cambio')]
        ).properties(
            width=self.WIDTH,
            height=250
        )


    def geography(self):
        return alt.InlineData(values=util.get_geojson_resource('municipalities.topojson'),
                              format=alt.TopoDataFormat(type='topojson', feature='municipalities'))

    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('municipal_map', self.metadata, autoload=True)
        query = select([
            table.c.bulletin_date,
            table.c.municipality,
            table.c.pop2020,
            table.c.new_7day_cases,
            table.c.previous_7day_cases
        ]).where(and_(min(bulletin_dates) <= table.c.bulletin_date,
                      table.c.bulletin_date <= max(bulletin_dates)))
        return pd.read_sql_query(query, connection, parse_dates=["bulletin_date"])

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]


class LatenessTiers(AbstractChart):
    def fetch_data(self, connection, bulletin_dates):
        table = sqlalchemy.Table('lateness_tiers', self.metadata, autoload=True)
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
                     alt.Tooltip('count:Q', format=",d", title='Casos en renglón (crudo)'),
                     alt.Tooltip('total:Q', format=",d", title='Casos total (crudo)'),
                     alt.Tooltip('mean_count:Q', format=".1f", title='Casos en renglón (promedio 7)'),
                     alt.Tooltip('mean_total:Q', format=".1f", title='Casos total (promedio 7)'),
                     alt.Tooltip('mean_percent:Q', format=".1%", title='% de total (promedio 7)')]
        )

        absolute = base.encode(
            x=alt.X('bulletin_date:T', title=None, axis=alt.Axis(ticks=False, labels=False)),
            y=alt.Y('mean_count:Q', title='Casos confirmados (promedio 7 días)',
                    scale=alt.Scale(domain=[0, max_y]),
                    axis=alt.Axis(labelExpr="if(datum.value > 0, datum.label, '')")),
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
