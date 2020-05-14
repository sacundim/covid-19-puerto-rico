from abc import ABC, abstractmethod
import altair as alt
import datetime
import logging
import numpy as np
import pandas as pd
from pathlib import Path
import sqlalchemy
from sqlalchemy.sql import select, and_
from . import util

class AbstractChart(ABC):
    def __init__(self, engine, args):
        self.engine = engine
        self.metadata = sqlalchemy.MetaData(engine)
        self.output_dir = args.output_dir
        self.output_formats = args.output_formats
        self.name = type(self).__name__

    def render(self, bulletin_date):
        with self.engine.connect() as connection:
            df = self.fetch_data(connection, bulletin_date)
        logging.info("%s dataframe: %s", self.name, util.describe_frame(df))

        bulletin_dir = Path(f'{self.output_dir}/{bulletin_date}')
        bulletin_dir.mkdir(exist_ok=True)
        util.save_chart(self.make_chart(df),
                        f"{bulletin_dir}/{bulletin_date}_{self.name}",
                        self.output_formats)

    @abstractmethod
    def make_chart(self, df):
        pass

    @abstractmethod
    def fetch_data(self, connection, bulletin_date):
        pass


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
                                  'Muertes (fecha actual)',
                                  'Muertes (fecha boletín)']),
            tooltip=['datum_date', 'variable', 'value']
        ).properties(
            width=575, height=275
        )

    def fetch_data(self, connection, bulletin_date):
        table = sqlalchemy.Table('cumulative_data', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.datum_date,
                        table.c.confirmed_cases,
                        table.c.probable_cases,
                        table.c.positive_results,
                        table.c.announced_cases,
                        table.c.deaths,
                        table.c.announced_deaths])\
            .where(table.c.bulletin_date == bulletin_date)
        df = pd.read_sql_query(query, connection)
        df = df.rename(columns={
            'confirmed_cases': 'Casos confirmados (fecha muestra)',
            'probable_cases': 'Casos probables (fecha muestra)',
            'positive_results': 'Pruebas positivas (fecha boletín)',
            'announced_cases': 'Casos (fecha boletín)',
            'deaths': 'Muertes (fecha actual)',
            'announced_deaths': 'Muertes (fecha boletín)'
        })
        return util.fix_and_melt(df, "datum_date")

class AbstractLateness(AbstractChart):
    def fetch_data_for_table(self, connection, bulletin_date, table, days=8):
        query = select([table.c.bulletin_date,
                        table.c.confirmed_and_probable_cases,
                        table.c.confirmed_cases,
                        table.c.probable_cases,
                        table.c.deaths]
        ).where(
            and_(bulletin_date - datetime.timedelta(days=days) < table.c.bulletin_date,
                 table.c.bulletin_date <= bulletin_date)
        )
        df = pd.read_sql_query(query, connection)
        df = df.rename(columns={
            'confirmed_and_probable_cases': 'Confirmados y probables',
            'confirmed_cases': 'Confirmados',
            'probable_cases': 'Probables',
            'deaths': 'Muertes'
        })
        return util.fix_and_melt(df, "bulletin_date")

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


    def fetch_data(self, connection, bulletin_date):
        table = sqlalchemy.Table('lateness_daily', self.metadata,
                                 schema='products', autoload=True)
        return self.fetch_data_for_table(connection, bulletin_date, table)


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

    def fetch_data(self, connection, bulletin_date):
        table = sqlalchemy.Table('lateness_7day', self.metadata,
                                 schema='products', autoload=True)
        return self.fetch_data_for_table(connection, bulletin_date, table, days=8)


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

    def fetch_data(self, connection, bulletin_date):
        table = sqlalchemy.Table('doubling_times', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.datum_date,
                        table.c.window_size_days,
                        table.c.cumulative_confirmed_and_probable_cases,
                        table.c.cumulative_confirmed_cases,
                        table.c.cumulative_probable_cases,
                        table.c.cumulative_deaths]
        ).where(
            table.c.bulletin_date == bulletin_date
        )
        df = pd.read_sql_query(query, connection)
        df = df.rename(columns={
            'cumulative_confirmed_and_probable_cases': 'Confirmados y probables',
            'cumulative_confirmed_cases': 'Confirmados',
            'cumulative_probable_cases': 'Probables',
            'cumulative_deaths': 'Muertes'
        })
        return pd.melt(util.fix_date_columns(df, "datum_date"),
                       ["datum_date", "window_size_days"])


class DailyDeltas(AbstractChart):
    def make_chart(self, df):
        filtered = df \
            .replace(0, np.nan) \
            .dropna()
        logging.info("df info: %s", util.describe_frame(filtered))

        base = alt.Chart(filtered).encode(
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

        text = base.mark_text(color='white').encode(
            text=alt.Text('value:Q'),
            color=alt.condition(
                alt.FieldRangePredicate(field='value', range=[0, 15]),
                alt.value('black'),
                alt.value('white')
            )
        )

        return (heatmap + text).properties(
            width=550
        ).facet(
            row=alt.Row('variable', title=None,
                        sort=['Confirmados y probables',
                              'Confirmados',
                              'Probables',
                              'Muertes'])
        )

    def fetch_data(self, connection, bulletin_date):
        table = sqlalchemy.Table('daily_deltas', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.delta_confirmed_and_probable_cases,
                        table.c.delta_confirmed_cases,
                        table.c.delta_probable_cases,
                        table.c.delta_deaths]
        ).where(
            and_(bulletin_date - datetime.timedelta(days=7) < table.c.bulletin_date,
                 table.c.bulletin_date <= bulletin_date)
        )
        df = pd.read_sql_query(query, connection)
        df = df.rename(columns={
            'delta_confirmed_and_probable_cases': 'Confirmados y probables',
            'delta_confirmed_cases': 'Confirmados',
            'delta_probable_cases': 'Probables',
            'delta_deaths': 'Muertes'
        })
        return util.fix_and_melt(df, "bulletin_date", "datum_date")
