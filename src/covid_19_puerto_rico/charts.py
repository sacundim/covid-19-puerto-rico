from abc import ABC, abstractmethod
import altair as alt
import datetime
import numpy as np
from pathlib import Path
from sqlalchemy.sql import select, and_
from .util import *

class AbstractChart(ABC):
    def __init__(self, engine, args):
        self.engine = engine
        self.metadata = sqlalchemy.MetaData(engine)
        self.output_dir = args.output_dir
        self.output_formats = args.output_formats
        self.name = type(self).__name__

    def execute(self, bulletin_date):
        with self.engine.connect() as connection:
            df = self.fetch_data(connection, bulletin_date)
        logging.info("%s dataframe: %s", self.name, describe_frame(df))

        bulletin_dir = Path(f'{self.output_dir}/{bulletin_date}')
        bulletin_dir.mkdir(exist_ok=True)
        save_chart(self.make_chart(df), f"{bulletin_dir}/{self.name}", self.output_formats)

    @abstractmethod
    def make_chart(self, df):
        pass

    @abstractmethod
    def fetch_data(self, connection, bulletin_date):
        pass


class Cumulative(AbstractChart):
    def make_chart(self, df):
        return alt.Chart(df).mark_line(point=True).encode(
            x=alt.X('yearmonthdate(datum_date):T', title=None),
            y=alt.Y('value', title=None, scale=alt.Scale(type='log')),
            color=alt.Color('variable', title=None,
                            legend=alt.Legend(orient="top", labelLimit=250)),
            tooltip=['datum_date', 'variable', 'value']
        ).properties(
            title="Los conteos acumulados que se anuncian cada día vs. revisiones posteriores",
            width=1200,
            height=800
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
        return fix_and_melt(df, "datum_date")

class AbstractLateness(AbstractChart):
    def fetch_data_for_table(self, connection, bulletin_date, table):
        query = select([table.c.bulletin_date,
                        table.c.confirmed_and_probable_cases,
                        table.c.confirmed_cases,
                        table.c.probable_cases,
                        table.c.deaths]
        ).where(
            and_(bulletin_date - datetime.timedelta(days=7) < table.c.bulletin_date,
                 table.c.bulletin_date <= bulletin_date)
        )
        df = pd.read_sql_query(query, connection)
        df = df.rename(columns={
            'confirmed_and_probable_cases': 'Confirmados y probables',
            'confirmed_cases': 'Confirmados',
            'probable_cases': 'Probables',
            'deaths': 'Muertes'
        })
        return fix_and_melt(df, "bulletin_date")

class LatenessDaily(AbstractLateness):
    def make_chart(self, df):
        sort_order = ['Confirmados y probables',
                      'Confirmados',
                      'Probables',
                      'Muertes']
        bars = alt.Chart(df).mark_bar().encode(
            y=alt.Y('value', title="Rezago estimado (días)"),
            x=alt.X('variable', title=None, sort=sort_order, axis=alt.Axis(labels=False)),
            color=alt.Color('variable', sort=sort_order,
                            legend=alt.Legend(orient='bottom', title=None)),
            tooltip=['variable', 'bulletin_date',
                     alt.Tooltip(field='value',
                                 type='quantitative',
                                 format=".1f")]
        )

        text = bars.mark_text(
            align='center',
            baseline='top',
            size=12,
            dy=5
        ).encode(
            text=alt.Text('value:Q', format='.1f'),
            color = alt.value('white')
        )

        return (bars + text).properties(
            width=150,
            height=600
        ).facet(
            column=alt.X("bulletin_date", sort="descending", title="Fecha del boletín")
        ).properties(
            title="Estimado de rezagos (día a día)"
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
                    title="Fecha boletín", axis=alt.Axis(titlePadding=10)),
            y=alt.Y('value:Q', title="Rezago estimado (días)"),
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
            width=500, height=300
        ).facet(
            columns=2, spacing = 40,
            facet=alt.Facet('variable', title=None, sort=sort_order)
        ).properties(
            title="Tendencia de los rezagos (ventanas de 7 días)"
        )

    def fetch_data(self, connection, bulletin_date):
        table = sqlalchemy.Table('lateness_7day', self.metadata,
                                 schema='products', autoload=True)
        return self.fetch_data_for_table(connection, bulletin_date, table)


class Doubling(AbstractChart):
    def make_chart(self, df):
        return alt.Chart(df.dropna()).mark_line(clip=True).encode(
            x=alt.X('datum_date:T', title='Fecha del evento'),
            y=alt.Y('value', title="Tiempo de duplicación (días)",
                    scale=alt.Scale(type='log', domain=(1, 100))),
            color=alt.Color('variable', legend=None)
        ).properties(
            width=256,
            height=256
        ).facet(
            column=alt.X('variable', title=None,
                         sort=['Confirmados y probables',
                               'Confirmados',
                               'Probables',
                               'Muertes']),
            row=alt.Y('window_size_days:O', title='Ancho de ventana (días)')
        ).properties(
            title="Los tiempos de duplicación de casos confirmados han bajado consistentemente"
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
        return pd.melt(fix_date_columns(df, "datum_date"),
                       ["datum_date", "window_size_days"])


class DailyDeltas(AbstractChart):
    def make_chart(self, df):
        filtered = df \
            .replace(0, np.nan) \
            .dropna()
        logging.info("df info: %s", describe_frame(filtered))

        base = alt.Chart(filtered).encode(
            x=alt.X('yearmonthdate(datum_date):O',
                    title="Fecha evento", sort="descending"),
            y=alt.Y('yearmonthdate(bulletin_date):O',
                    title="Fecha boletín", sort="descending"),
            tooltip=['bulletin_date:T', 'datum_date:T', 'value']
        )

        heatmap = base.mark_rect(cornerRadius=12).encode(
            color=alt.Color('value:Q', title=None,
                            scale=alt.Scale(scheme="redgrey", domainMid=0))
        )

        text = base.mark_text(color='white', size=15).encode(
            text=alt.Text('value:Q'),
            color=alt.condition(
                alt.FieldRangePredicate(field='value', range=[0, 15]),
                alt.value('black'),
                alt.value('white')
            )
        )

        return (heatmap + text).properties(
            width=900, height=225
        ).facet(
            title="Muchas veces los casos que se añaden (¡o quitan!) son viejitos",
            row=alt.Row('variable', title=None,
                        sort=['Confirmados',
                              'Probables',
                              'Muertes'])
        )

    def fetch_data(self, connection, bulletin_date):
        table = sqlalchemy.Table('daily_deltas', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.delta_confirmed_cases,
                        table.c.delta_probable_cases,
                        table.c.delta_deaths]
        ).where(
            and_(bulletin_date - datetime.timedelta(days=7) < table.c.bulletin_date,
                 table.c.bulletin_date <= bulletin_date)
        )
        df = pd.read_sql_query(query, connection)
        df = df.rename(columns={
            'delta_confirmed_cases': 'Confirmados',
            'delta_probable_cases': 'Probables',
            'delta_deaths': 'Muertes'
        })
        return fix_and_melt(df, "bulletin_date", "datum_date")
