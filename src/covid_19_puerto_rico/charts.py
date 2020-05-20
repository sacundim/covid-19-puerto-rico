from abc import ABC, abstractmethod
import altair as alt
import datetime
import logging
import numpy as np
import pandas as pd
from pathlib import Path
import sqlalchemy
from sqlalchemy.sql import select, text
from . import util

class AbstractChart(ABC):
    def __init__(self, engine, output_dir,
                 output_formats=frozenset(['json'])):
        self.engine = engine
        self.metadata = sqlalchemy.MetaData(engine)
        self.output_dir = output_dir
        self.output_formats = output_formats
        self.name = type(self).__name__

    def render(self, bulletin_dates):
        with self.engine.connect() as connection:
            df = self.fetch_data(connection)
        logging.info("%s dataframe: %s", self.name, util.describe_frame(df))

        for bulletin_date in bulletin_dates:
            self.render_bulletin_date(df, bulletin_date)

    def render_bulletin_date(self, df, bulletin_date):
        bulletin_dir = Path(f'{self.output_dir}/{bulletin_date}')
        bulletin_dir.mkdir(exist_ok=True)
        util.save_chart(self.make_chart(self.filter_data(df, bulletin_date)),
                        f"{bulletin_dir}/{bulletin_date}_{self.name}",
                        self.output_formats)

    @abstractmethod
    def make_chart(self, df):
        pass

    @abstractmethod
    def fetch_data(self, connection):
        pass

    def filter_data(self, df, bulletin_date):
        """Filter dataframe according to given bulletin_date.  May want to override."""
        return df.loc[df['bulletin_date'] == pd.to_datetime(bulletin_date)]



class Cumulative(AbstractChart):
    def make_chart(self, df):
        return alt.Chart(df).mark_line(point=True).encode(
            x=alt.X('yearmonthdate(datum_date):T', title=None,
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('value', title=None, scale=alt.Scale(type='log')),
            color=alt.Color('variable', title=None,
                            legend=alt.Legend(orient="top", labelLimit=250, columns=2),
                            sort=['Casos confirmados (fecha muestra)',
                                  'Pruebas positivas (fecha boletín)',
                                  'Casos (fecha boletín)',
                                  'Casos probables (fecha muestra)',
                                  'Muertes (fecha muerte)',
                                  'Muertes (fecha boletín)']),
            tooltip=['datum_date', 'variable', 'value']
        ).properties(
            width=575, height=275
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('cumulative_data', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.confirmed_cases,
                        table.c.probable_cases,
                        table.c.positive_results,
                        table.c.announced_cases,
                        table.c.deaths,
                        table.c.announced_deaths])
        df = pd.read_sql_query(query, connection,
                               parse_dates=["bulletin_date", "datum_date"])
        df = df.rename(columns={
            'confirmed_cases': 'Casos confirmados (fecha muestra)',
            'probable_cases': 'Casos probables (fecha muestra)',
            'positive_results': 'Pruebas positivas (fecha boletín)',
            'announced_cases': 'Casos (fecha boletín)',
            'deaths': 'Muertes (fecha muerte)',
            'announced_deaths': 'Muertes (fecha boletín)'
        })
        return pd.melt(df, ["bulletin_date", "datum_date"])


class NewCases(AbstractChart):
    def make_chart(self, df):
        base = alt.Chart(df.dropna()).encode(
            x=alt.X('yearmonthdate(datum_date):T', title=None,
                    axis=alt.Axis(format='%d/%m'))
        )

        scatter = base.transform_filter(
            # Needed because log(0) = negative infinity, and this
            # messes up the axis scale
            alt.datum.value > 0
        ).mark_point(opacity=0.5).encode(
            y=alt.Y('value:Q', title=None, scale=alt.Scale(type='log')),
            tooltip=['datum_date', 'variable', 'value']
        )

        average = base.transform_window(
            frame=[-6, 0],
            mean_value='mean(value)',
            groupby=['variable']
        ).mark_line(strokeWidth=3).encode(
            y=alt.Y('mean_value:Q', title=None, scale=alt.Scale(type='log'))
        )

        return (average + scatter).encode(
            color=alt.Color('variable', title=None,
                            legend=alt.Legend(orient="top", labelLimit=250),
                            sort=['Confirmados',
                                  'Probables',
                                  'Muertes'])
        ).properties(
            width=600, height=400
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('bitemporal', self.metadata, autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.confirmed_cases,
                        table.c.probable_cases,
                        table.c.deaths])
        df = pd.read_sql_query(query, connection,
                               parse_dates=["bulletin_date", "datum_date"])
        df = df.rename(columns={
            'confirmed_cases': 'Confirmados',
            'probable_cases': 'Probables',
            'deaths': 'Muertes'
        })
        return pd.melt(df, ["bulletin_date", "datum_date"])


class AbstractLateness(AbstractChart):
    def fetch_data_for_table(self, connection, table):
        query = select([table.c.bulletin_date,
                        table.c.confirmed_and_probable_cases,
                        table.c.confirmed_cases,
                        table.c.probable_cases,
                        table.c.deaths]
        )
        df = pd.read_sql_query(query, connection,
                               parse_dates=["bulletin_date"])
        df = df.rename(columns={
            'confirmed_and_probable_cases': 'Confirmados y probables',
            'confirmed_cases': 'Confirmados',
            'probable_cases': 'Probables',
            'deaths': 'Muertes'
        })
        return pd.melt(df, "bulletin_date")

    def filter_data(self, df, bulletin_date):
        since_date = pd.to_datetime(bulletin_date - datetime.timedelta(days=8))
        until_date = pd.to_datetime(bulletin_date)
        return df.loc[(since_date < df['bulletin_date'])
                      & (df['bulletin_date'] <= until_date)]


class LatenessDaily(AbstractLateness):
    def make_chart(self, df):
        sort_order = ['Confirmados y probables',
                      'Confirmados',
                      'Probables',
                      'Muertes']
        bars = alt.Chart(df).mark_bar().encode(
            x=alt.X('value', title="Rezago estimado (días)"),
            y=alt.Y('variable', title=None, sort=sort_order, axis=None),
            color=alt.Color('variable', sort=sort_order,
                            legend=alt.Legend(orient='bottom', title=None)),
            tooltip=['variable', 'bulletin_date',
                     alt.Tooltip(field='value',
                                 type='quantitative',
                                 format=".1f")]
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


    def fetch_data(self, connection):
        table = sqlalchemy.Table('lateness_daily', self.metadata,
                                 schema='products', autoload=True)
        return self.fetch_data_for_table(connection, table)


class Lateness7Day(AbstractLateness):
    def make_chart(self, df):
        sort_order = ['Confirmados y probables',
                      'Confirmados',
                      'Probables',
                      'Muertes']
        lines = alt.Chart(df).mark_line(
            strokeWidth=3,
            point=alt.OverlayMarkDef(size=50)
        ).encode(
            x=alt.X('yearmonthdate(bulletin_date):O',
                    title="Fecha boletín",
                    axis=alt.Axis(format='%d/%m', titlePadding=10)),
            y=alt.Y('value:Q', title="Rezago (días)"),
            color = alt.Color('variable', sort=sort_order, legend=None),
            tooltip=['variable', 'bulletin_date',
                     alt.Tooltip(field='value',
                                 type='quantitative',
                                 format=".1f")]
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
            width=275, height=75
        ).facet(
            columns=2, spacing = 40,
            facet=alt.Facet('variable', title=None, sort=sort_order)
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('lateness_7day', self.metadata,
                                 schema='products', autoload=True)
        return self.fetch_data_for_table(connection, table)


class Doubling(AbstractChart):
    def make_chart(self, df):
        return alt.Chart(df.dropna()).mark_line(clip=True).encode(
            x=alt.X('datum_date:T',
                    title='Fecha del evento',
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('value', title=None,
                    scale=alt.Scale(type='log', domain=(1, 100))),
            color=alt.Color('variable', legend=None)
        ).properties(
            width=175,
            height=120

        ).facet(
            row=alt.Row('variable', title=None,
                        sort=['Confirmados y probables',
                              'Confirmados',
                              'Probables',
                              'Muertes']),
            column=alt.Column('window_size_days:O', title='Ancho de ventana (días)')
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('doubling_times', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.datum_date,
                        table.c.bulletin_date,
                        table.c.window_size_days,
                        table.c.cumulative_confirmed_and_probable_cases,
                        table.c.cumulative_confirmed_cases,
                        table.c.cumulative_probable_cases,
                        table.c.cumulative_deaths]
        )
        df = pd.read_sql_query(query, connection,
                               parse_dates=["bulletin_date", "datum_date"])
        df = df.rename(columns={
            'cumulative_confirmed_and_probable_cases': 'Confirmados y probables',
            'cumulative_confirmed_cases': 'Confirmados',
            'cumulative_probable_cases': 'Probables',
            'cumulative_deaths': 'Muertes'
        })
        return pd.melt(df, ["bulletin_date", "datum_date", "window_size_days"])


class DailyDeltas(AbstractChart):
    def make_chart(self, df):
        base = alt.Chart(df).encode(
            x=alt.X('yearmonthdate(datum_date):O',
                    title="Fecha evento", sort="descending",
                    axis=alt.Axis(format='%d/%m')),
            y=alt.Y('yearmonthdate(bulletin_date):O',
                    title="Fecha boletín", sort="descending",
                    axis=alt.Axis(format='%d/%m')),
            tooltip=['bulletin_date:T', 'datum_date:T', 'value']
        )

        heatmap = base.mark_rect().encode(
            color=alt.Color('value:Q', title=None, legend=None,
                            scale=alt.Scale(scheme="redgrey", domainMid=0))
        )

        text = base.mark_text(color='white', size=8).encode(
            text=alt.Text('value:Q'),
            color=alt.condition(
                alt.FieldRangePredicate(field='value', range=[0, 15]),
                alt.value('black'),
                alt.value('white')
            )
        )

        return (heatmap + text).properties(
            width=570
        ).facet(
            row=alt.Row('variable', title=None,
                        sort=['Confirmados y probables',
                              'Confirmados',
                              'Probables',
                              'Muertes'])
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('daily_deltas', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.delta_confirmed_and_probable_cases,
                        table.c.delta_confirmed_cases,
                        table.c.delta_probable_cases,
                        table.c.delta_deaths]
        )
        df = pd.read_sql_query(query, connection,
                               parse_dates=["bulletin_date", "datum_date"])
        df = df.rename(columns={
            'delta_confirmed_and_probable_cases': 'Confirmados y probables',
            'delta_confirmed_cases': 'Confirmados',
            'delta_probable_cases': 'Probables',
            'delta_deaths': 'Muertes'
        })
        return pd.melt(df, ["bulletin_date", "datum_date"])

    def filter_data(self, df, bulletin_date):
        since_date = pd.to_datetime(bulletin_date - datetime.timedelta(days=7))
        until_date = pd.to_datetime(bulletin_date)
        filtered = df.loc[(since_date < df['bulletin_date'])
                      & (df['bulletin_date'] <= until_date)]\
            .replace(0, np.nan)\
            .dropna()
        return filtered


class WeekdayBias(AbstractChart):
    def make_chart(self, df):
        total = self.one_variable(df, 'Confirmados y probables')
        confirmed = self.one_variable(df, 'Confirmados')
        probable = self.one_variable(df, 'Probables')
        deaths = self.one_variable(df, 'Muertes')

        row1 = alt.hconcat(total, confirmed, spacing=20).resolve_scale(
            color='independent'
        )
        row2 = alt.hconcat(probable, deaths, spacing=20).resolve_scale(
            color='independent'
        )
        return alt.vconcat(row1, row2, spacing=40).resolve_scale(
            color='independent'
        )

    def one_variable(self, df, variable):
        base = alt.Chart(df).transform_filter(
            alt.datum.variable == variable
        ).encode(
            color=alt.Color('mean(value):Q', title=None,
                            scale=alt.Scale(scheme="blues"))
        )

        heatmap = base.mark_rect().encode(
            x=alt.X('day(datum_date):O', title='Día muestra o muerte'),
            y=alt.Y('day(bulletin_date):O', title='Día boletín'),
            tooltip=['variable', 'day(bulletin_date):O', 'day(datum_date):O',
                     alt.Tooltip(field='value',
                                 type='quantitative',
                                 aggregate='mean',
                                 format=".1f")]
        ).properties(
            width=160, height=160
        )

        right = base.mark_bar().encode(
            x=alt.X('mean(value):Q', title=None, axis=None),
            y=alt.Y('day(bulletin_date):O', title=None, axis=None),
            tooltip=['variable', 'day(bulletin_date):O',
                     alt.Tooltip(field='value',
                                 type='quantitative',
                                 aggregate='mean',
                                 format=".1f")]
        ).properties(
            width=40, height=160
        )

        top = base.mark_bar().encode(
            x=alt.X('day(datum_date):O', title=None, axis=None),
            y=alt.Y('mean(value):Q', title=None, axis=None),
            tooltip = ['variable', 'day(datum_date):O',
                       alt.Tooltip(field='value',
                                   type='quantitative',
                                   aggregate='mean',
                                   format=".1f")]
        ).properties(
            width=160, height=40,
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
        )

        return alt.vconcat(
            top,
            alt.hconcat(heatmap, right, spacing=3),
            spacing=3
        )

    def fetch_data(self, connection):
        query = text("""SELECT 
	ba.bulletin_date,
	ba.datum_date,
	ba.delta_confirmed_and_probable_cases,
	ba.delta_confirmed_cases,
	ba.delta_probable_cases,
	ba.delta_deaths
FROM bitemporal_agg ba 
WHERE ba.datum_date >= ba.bulletin_date - INTERVAL '14' DAY
AND ba.bulletin_date >= (
	SELECT min(bulletin_date)
	FROM bitemporal_agg
	WHERE delta_confirmed_and_probable_cases IS NOT NULL
	AND delta_confirmed_cases IS NOT NULL
	AND delta_probable_cases IS NOT NULL
	AND delta_deaths IS NOT NULL)""")
        df = pd.read_sql_query(query, connection, parse_dates=['bulletin_date', 'datum_date'])
        df = df.rename(columns={
            'delta_confirmed_and_probable_cases': 'Confirmados y probables',
            'delta_confirmed_cases': 'Confirmados',
            'delta_probable_cases': 'Probables',
            'delta_deaths': 'Muertes'
        })
        return pd.melt(df, ['bulletin_date', 'datum_date']).dropna()

    def filter_data(self, df, bulletin_date):
        return df.loc[df['bulletin_date'] <= pd.to_datetime(bulletin_date)]

