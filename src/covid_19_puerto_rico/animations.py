from abc import ABC, abstractmethod
import altair as alt
import pandas as pd
from pathlib import Path
import sqlalchemy
from sqlalchemy import select
from wand.image import Image
from . import util

class AbstractAnimation(ABC):
    def __init__(self, engine, args, delay=180):
        self.engine = engine
        self.metadata = sqlalchemy.MetaData(engine)
        self.name = type(self).__name__
        self.output_dir = args.output_dir
        self.delay = delay

    def render(self, bulletin_date):
        with self.engine.connect() as connection:
            df = self.fetch_data(connection, bulletin_date)
            all_bulletin_dates = self.get_bulletin_dates(connection, bulletin_date)

        bulletin_dir = Path(f"{self.output_dir}/{bulletin_date}")
        bulletin_dir.mkdir(exist_ok=True)

        frames_dir = Path(f"{bulletin_dir}/{bulletin_date}_{self.name}_frames")
        frames_dir.mkdir(exist_ok=True)

        with Image() as gif:
            for current_date in all_bulletin_dates['bulletin_date']:
                basename = f"{frames_dir}/{bulletin_date}_{self.name}_frame_{current_date.date()}"
                util.save_chart(self.make_frame(df, current_date), basename, ['png'])
                with Image(filename=f'{basename}.png') as frame:
                    gif.sequence.append(frame)
            for frame in gif.sequence:
                frame.delay = self.delay
            gif.type = 'optimize'
            gif.save(filename=f"{bulletin_dir}/{bulletin_date}_{self.name}.gif")

    def get_bulletin_dates(self, connection, bulletin_date):
        table = sqlalchemy.Table('bitemporal', self.metadata, autoload=True)
        query = select([table.c.bulletin_date.label('bulletin_date')])\
            .where(table.c.bulletin_date <= bulletin_date)\
            .distinct()\
            .order_by(table.c.bulletin_date)
        df = pd.read_sql_query(query, connection)
        df['bulletin_date'] = pd.to_datetime(df['bulletin_date'])
        return df


    @abstractmethod
    def make_frame(self, df, current_date):
        pass

    @abstractmethod
    def fetch_data(self, connection, bulletin_date):
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
        def compute_date_domain(df):
            return (pd.to_datetime(df['bulletin_date'].min()),
                    pd.to_datetime(df['bulletin_date'].max()))

        def compute_value_domain(df, variable):
            filtered = df.loc[df['variable'] == variable]
            min = filtered['value'].min()
            max = filtered['value'].max()
            return (min * 0.90, max * 1.10)

        base = alt.Chart(df).encode(
            x=alt.X('datum_date', title=None, scale=alt.Scale(domain=compute_date_domain(df))),
            y=alt.Y('value', title=variable, axis=alt.Axis(labels=False, titlePadding=25),
                    scale=alt.Scale(zero=False, domain=compute_value_domain(df, variable)))
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
            dx=-3, dy=-3
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
            dx=-3, dy=3
        ).transform_filter(
            alt.FieldOneOfPredicate(
                field='temporality',
                oneOf=['Anunciados']
            )
        )
        return alt.layer(lines, revised, announced).properties(
            width=1000, height=100
        )

    def fetch_data(self, connection, bulletin_date):
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
                .where(table.c.bulletin_date <= bulletin_date)
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
        melted = util.fix_and_melt(df, "bulletin_date", "datum_date")
        melted['temporality'] = melted['variable'].map(lambda var: var.split()[1])
        melted['variable'] = melted['variable'].map(lambda var: var.split()[0])
        return melted