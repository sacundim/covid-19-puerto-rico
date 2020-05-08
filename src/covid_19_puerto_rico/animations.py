from abc import ABC, abstractmethod
import altair as alt
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import select
from wand.image import Image
from .util import *

class AbstractAnimation(ABC):
    def __init__(self, engine, args, delay=180):
        self.engine = engine
        self.metadata = sqlalchemy.MetaData(engine)
        self.args = args
        self.name = type(self).__name__
        self.delay = delay

    def execute(self):
        with self.engine.connect() as connection:
            df = self.fetch_data(connection)

        Path(f"{self.args.output_dir}/{self.name}").mkdir(parents=True, exist_ok=True)
        with Image() as gif:
            for current_date in pd.date_range(self.args.earliest_bulletin_date, self.args.bulletin_date):
                basename = f"{self.args.output_dir}/{self.name}/{self.name}_frame_{current_date.date()}"
                save_chart(self.make_frame(df, current_date), basename, ['png'])
                with Image(filename=f'{basename}.png') as frame:
                    gif.sequence.append(frame)
            for frame in gif.sequence:
                frame.delay = self.delay
            gif.type = 'optimize'
            gif.save(filename=f"{self.args.output_dir}/{self.name}_{self.args.bulletin_date}.gif")

    @abstractmethod
    def make_frame(self, df, current_date):
        pass

    @abstractmethod
    def fetch_data(self, connection):
        pass


class CaseLag(AbstractAnimation):
    def make_frame(self, df, current_date):
        return alt.vconcat(
            *list(map(lambda variable: self.one_variable(df, current_date, variable),
                      ['Total', 'Confirmados', 'Probables', 'Muertes'])),
        ).properties(
            title="Los números que anuncian en una fecha no son los finales para esa fecha"
        )

    def one_variable(self, df, current_date, variable):
        x_domain = (pd.to_datetime(self.args.earliest_bulletin_date),
                    pd.to_datetime(self.args.bulletin_date))
        y_domain = self.compute_domain(df, variable)
        base = alt.Chart(df).encode(
            x=alt.X('datum_date', title=None, scale=alt.Scale(domain=x_domain)),
            y=alt.Y('value', title=variable, axis=alt.Axis(labels=False, titlePadding=25),
                    scale=alt.Scale(zero=False, domain=y_domain))
        ).transform_filter(
            {
                "and": [
                    alt.FieldEqualPredicate(field='bulletin_date', equal=current_date),
                    alt.FieldEqualPredicate(field='variable', equal=variable)
                ]
            }
        )

        lines = base.mark_line(point=True, clip=True).encode(
            color=alt.Color('temporality', title=None,
                            legend=alt.Legend(orient='top', labelLimit=250)),
        )

        revised = base.encode(
            text='value:Q'
        ).mark_text(
            align='right',
            baseline='line-bottom',
            dy=-3
        ).transform_filter(
            alt.FieldOneOfPredicate(
                field='temporality',
                oneOf=['Revisados']
            )
        )

        announced = base.encode(
            text='value:Q'
        ).mark_text(
            align='right',
            baseline='line-top',
            dy=3
        ).transform_filter(
            alt.FieldOneOfPredicate(
                field='temporality',
                oneOf=['Anunciados']
            )
        )
        return alt.layer(lines, revised, announced).properties(
            width=900, height=150
        )

    @staticmethod
    def compute_domain(df, variable):
        filtered = df.loc[df['variable'] == variable]
        min = filtered['value'].min()
        max = filtered['value'].max()
        return (min * 0.95, max * 1.05)

    def fetch_data(self, connection):
        meta = sqlalchemy.MetaData()
        table = sqlalchemy.Table('animations', meta, schema='products',
                                 autoload_with=connection)
        query = select([table.c.bulletin_date,
                        table.c.datum_date,
                        table.c.confirmed_cases,
                        table.c.probable_cases,
                        table.c.cases,
                        table.c.deaths,
                        table.c.announced_confirmed_cases,
                        table.c.announced_probable_cases,
                        table.c.announced_cases,
                        table.c.announced_deaths])\
                .where(table.c.bulletin_date <= self.args.bulletin_date)
        df = pd.read_sql_query(query, connection)
        df = df.rename(columns={
            'confirmed_cases': 'Confirmados Revisados',
            'probable_cases': 'Probables Revisados',
            'cases': 'Total Revisados',
            'deaths': 'Muertes Revisados',
            'announced_confirmed_cases': 'Confirmados Anunciados',
            'announced_probable_cases': 'Probables Anunciados',
            'announced_cases': 'Total Anunciados',
            'announced_deaths': 'Muertes Anunciados',
        })
        melted = fix_and_melt(df, "bulletin_date", "datum_date")
        melted['temporality'] = melted['variable'].map(lambda var: var.split()[1])
        melted['variable'] = melted['variable'].map(lambda var: var.split()[0])
        return melted