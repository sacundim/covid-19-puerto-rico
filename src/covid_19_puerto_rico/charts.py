from abc import ABC, abstractmethod
import altair as alt
import datetime
import numpy as np
from sqlalchemy.sql import select, and_
from .util import *

class AbstractChart(ABC):
    def __init__(self, engine, args):
        self.engine = engine
        self.metadata = sqlalchemy.MetaData(engine)
        self.args = args
        self.name = type(self).__name__

    def execute(self):
        with self.engine.connect() as connection:
            df = self.fetch_data(connection)
        logging.info("%s dataframe: %s", self.name, describe_frame(df))
        basename = f"{self.args.output_dir}/{self.name}_{self.args.bulletin_date}"
        save_chart(self.make_chart(df), basename, self.args.output_formats)

    @abstractmethod
    def make_chart(self, df):
        pass

    @abstractmethod
    def fetch_data(self, connection):
        pass


class Cumulative(AbstractChart):
    def make_chart(self, df):
        return alt.Chart(df).mark_line(point=True).encode(
            x=alt.X('datum_date:T', title=None),
            y=alt.Y('value', title=None, scale=alt.Scale(type='log')),
            color=alt.Color('variable', title=None,
                            legend=alt.Legend(orient="top", labelLimit=250)),
            tooltip=['datum_date', 'variable', 'value']
        ).properties(
            title="Los conteos acumulados que se anuncian cada día vs. revisiones posteriores",
            width=1200,
            height=800
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('cumulative_data', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.datum_date,
                        table.c.confirmed_cases,
                        table.c.probable_cases,
                        table.c.positive_results,
                        table.c.announced_cases,
                        table.c.deaths,
                        table.c.announced_deaths])\
            .where(table.c.bulletin_date == self.args.bulletin_date)
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


class Lateness(AbstractChart):
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
            baseline='middle',
            dy=-10
        ).encode(
            text=alt.Text('value:Q', format='.1f')
        )

        return (bars + text).properties(
            width=150,
            height=600
        ).facet(
            column=alt.X("bulletin_date", sort="descending", title="Fecha del boletín")
        ).properties(
            title="Es común que tarde una semana entre que se tome la muestra y se anuncie nuevo caso"
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('lateness', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.confirmed_and_probable_cases,
                        table.c.confirmed_cases,
                        table.c.probable_cases,
                        table.c.deaths]
        ).where(
            and_(self.args.bulletin_date - datetime.timedelta(days=7) < table.c.bulletin_date,
                 table.c.bulletin_date <= self.args.bulletin_date)
        )
        df = pd.read_sql_query(query, connection)
        df = df.rename(columns={
            'confirmed_and_probable_cases': 'Confirmados y probables',
            'confirmed_cases': 'Confirmados',
            'probable_cases': 'Probables',
            'deaths': 'Muertes'
        })
        return fix_and_melt(df, "bulletin_date")


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

    def fetch_data(self, connection):
        table = sqlalchemy.Table('doubling_times', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.datum_date,
                        table.c.window_size_days,
                        table.c.cumulative_confirmed_and_probable_cases,
                        table.c.cumulative_confirmed_cases,
                        table.c.cumulative_probable_cases,
                        table.c.cumulative_deaths]
        ).where(
            table.c.bulletin_date == self.args.bulletin_date
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
        def bug_workaround(df):
            """If both of these conditions hold:

             1. One of the subcharts in this faceted chart has
                no data points;
             2. I custom sort the faceting grid column;

             ...then I get an empty subchart (no gridlines even)
             and the sorting of the columns for that row breaks."""
            filtered = df\
                .replace(0, np.nan)\
                .dropna()
            return (min(filtered['datum_date']), max(filtered['datum_date']))

        return alt.Chart(df).mark_bar(clip=True).encode(
            x=alt.X('value', title="Casos +/-"),
            y=alt.Y('datum_date:T', title="Fecha del evento",
                    scale=alt.Scale(domain=bug_workaround(df))),
            color=alt.Color('variable', legend=None),
            tooltip = ['variable', 'datum_date:T', 'value']
        ).properties(
            width=140,
            height=250
        ).facet(
            column=alt.X('bulletin_date:T', sort="descending",
                         title="Fecha del boletín"),
            row=alt.Y('variable', title=None,
                      sort=['Confirmados y probables',
                            'Confirmados',
                            'Probables',
                            'Muertes'])
        ).properties(
            title="Muchas veces los casos que se añaden (¡o quitan!) son viejitos"
        )

    def fetch_data(self, connection):
        table = sqlalchemy.Table('daily_deltas', self.metadata,
                                 schema='products', autoload=True)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.delta_confirmed_cases,
                        table.c.delta_probable_cases,
                        table.c.delta_deaths]
        ).where(
            and_(self.args.bulletin_date - datetime.timedelta(days=7) < table.c.bulletin_date,
                 table.c.bulletin_date <= self.args.bulletin_date)
        )
        df = pd.read_sql_query(query, connection)
        df = df.rename(columns={
            'delta_confirmed_cases': 'Confirmados',
            'delta_probable_cases': 'Probables',
            'delta_deaths': 'Muertes'
        })
        return fix_and_melt(df, "bulletin_date", "datum_date")
