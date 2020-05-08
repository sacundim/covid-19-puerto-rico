import altair as alt
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import select
from sqlalchemy.sql import text
from wand.image import Image
from .util import *


def death_lag(connection, args):
    Path(f"{args.output_dir}/death_lag_animation").mkdir(parents=True, exist_ok=True)
    with Image() as gif:
        for current_date in pd.date_range(args.earliest_bulletin_date, args.bulletin_date):
            df = death_lag_data(connection,
                                args.earliest_bulletin_date,
                                current_date)
            logging.info("death_lag_animation frame: %s", describe_frame(df))

            basename = f"{args.output_dir}/death_lag_animation/frame_{current_date.date()}"
            save_chart(death_lag_chart(df, args), basename, ['png'])
            with Image(filename=f'{basename}.png') as frame:
                gif.sequence.append(frame)
        for frame in gif.sequence:
            frame.delay = 120
        gif.type = 'optimize'
        gif.save(filename=f"{args.output_dir}/death_lag_animation_{args.bulletin_date}.gif")


def death_lag_chart(df, args):
    lines =  alt.Chart(df).mark_line(point=True).encode(
        x=alt.X('datum_date', title='Fecha',
                scale=alt.Scale(domain=(pd.to_datetime(args.earliest_bulletin_date),
                                        pd.to_datetime(args.bulletin_date)))),
        y=alt.Y('value', title=None, scale=alt.Scale(domain=(80, 110))),
        color=alt.Color('variable', title=None,
                        legend=alt.Legend(orient='top', labelLimit=250)),
    )

    text = lines.mark_text(
        align='center',
        baseline='line-bottom',
        dy=-3
    ).encode(
        text='value:Q'
    )

    return (lines + text).properties(
        width=600, height=200
    ).configure(
        padding=15
    )

def death_lag_data(connection, earliest_bulletin_date, bulletin_date):
    query = text('''
        SELECT 
            datum_date,
            COALESCE(deaths, 
                    MAX(deaths) OVER (PARTITION BY bulletin_date ROWS UNBOUNDED PRECEDING)) deaths,
            announced_deaths
        FROM products.cumulative_data
        WHERE :earliest_bulletin_date <= datum_date 
        AND datum_date <= bulletin_date
        AND bulletin_date = :bulletin_date
        ORDER BY bulletin_date, datum_date''')\
        .bindparams(earliest_bulletin_date=earliest_bulletin_date,
                    bulletin_date=bulletin_date)
    df = pd.read_sql_query(query, connection)
    df = df.rename(columns={
        'announced_deaths': 'Muertes anunciadas',
        'deaths': 'Muertes revisadas'
    })
    return fix_and_melt(df, "datum_date")


def case_lag(connection, args):
    df = case_lag_data(connection, args.bulletin_date)

    Path(f"{args.output_dir}/case_lag_animation").mkdir(parents=True, exist_ok=True)
    with Image() as gif:
        for current_date in pd.date_range(args.earliest_bulletin_date, args.bulletin_date):
            basename = f"{args.output_dir}/case_lag_animation/frame_{current_date.date()}"
            save_chart(case_lag_chart(df, args, current_date), basename, ['png'])
            with Image(filename=f'{basename}.png') as frame:
                gif.sequence.append(frame)
        for frame in gif.sequence:
            frame.delay = 180
        gif.type = 'optimize'
        gif.save(filename=f"{args.output_dir}/case_lag_animation_{args.bulletin_date}.gif")


def case_lag_chart(df, args, current_date):
    base = alt.Chart(df).encode(
        x=alt.X('datum_date', title='Fecha',
                scale=alt.Scale(domain=(pd.to_datetime(args.earliest_bulletin_date),
                                        pd.to_datetime(args.bulletin_date)))),
        y=alt.Y('value', title=None, scale=alt.Scale(zero=False, domain=[400, 2200]))
    ).transform_filter(
        alt.FieldEqualPredicate(field='bulletin_date',
                                equal=current_date)
    )

    lines = base.mark_line(point=True).encode(
        color=alt.Color('temporality', title=None,
                        legend=alt.Legend(orient='top', labelLimit=250)),
    )

    revised = base.encode(
        text='value:Q'
    ).mark_text(
        align='center',
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
        align='center',
        baseline='line-top',
        dy=3
    ).transform_filter(
        alt.FieldOneOfPredicate(
            field='temporality',
            oneOf=['Anunciados']
        )
    )
    return (lines + revised + announced).properties(
        width=900, height=150
    ).facet(
        row=alt.Row('variable', title=None)
    ).configure(
        padding=15
    ) # .resolve_scale(y='independent')

def case_lag_data(connection, bulletin_date):
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('animations', meta, schema='products',
                             autoload_with=connection)
    query = select([table.c.bulletin_date,
                    table.c.datum_date,
                    table.c.confirmed_cases,
                    table.c.probable_cases,
                    table.c.cases,
                    table.c.announced_confirmed_cases,
                    table.c.announced_probable_cases,
                    table.c.announced_cases])\
            .where(table.c.bulletin_date <= bulletin_date)
    df = pd.read_sql_query(query, connection)
    df = df.rename(columns={
        'confirmed_cases': 'Confirmados Revisados',
        'probable_cases': 'Probables Revisados',
        'cases': 'Total Revisados',
        'announced_confirmed_cases': 'Confirmados Anunciados',
        'announced_probable_cases': 'Probables Anunciados',
        'announced_cases': 'Total Anunciados',
    })
    melted = fix_and_melt(df, "bulletin_date", "datum_date")
    melted['temporality'] = melted['variable'].map(lambda var: var.split()[1])
    melted['variable'] = melted['variable'].map(lambda var: var.split()[0])
    return melted