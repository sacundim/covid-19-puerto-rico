import altair as alt
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import select
from sqlalchemy.sql import text
from wand.image import Image
from .util import *


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
    return alt.vconcat(
        *list(map(lambda variable: case_lag_chart_one_variable(df, args, current_date, variable),
                  ['Total', 'Confirmados', 'Probables', 'Muertes'])),
    )

def case_lag_chart_one_variable(df, args, current_date, variable):
    x_domain = (pd.to_datetime(args.earliest_bulletin_date),
                pd.to_datetime(args.bulletin_date))
    y_domain = compute_domain(df, variable)
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

def compute_domain(df, variable):
    filtered = df.loc[df['variable'] == variable]
    min = filtered['value'].min()
    max = filtered['value'].max()
    return (min * 0.95, max * 1.05)

def case_lag_data(connection, bulletin_date):
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
    melted = fix_and_melt(df, "bulletin_date", "datum_date")
    melted['temporality'] = melted['variable'].map(lambda var: var.split()[1])
    melted['variable'] = melted['variable'].map(lambda var: var.split()[0])
    return melted