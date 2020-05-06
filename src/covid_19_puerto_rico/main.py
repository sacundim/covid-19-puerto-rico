#/usr/bin/env/python3

import altair as alt
import argparse
import datetime
import io
import logging
import numpy as np
import pandas as pd
import sqlalchemy
import toml
from sqlalchemy.sql import select, and_

def process_arguments():
    parser = argparse.ArgumentParser(description='Generate Puerto Rico COVID-19 charts')
    parser.add_argument('--output-dir', type=str, required=True,
                        help='Directory into which to place output')
    parser.add_argument('--output-formats', action='append', default=['json'])
    parser.add_argument('--bulletin-date', type=datetime.date.fromisoformat, required=True,
                        help='Bulletin date to generate charts for')
    parser.add_argument('--config-file', type=str, required=True,
                        help='TOML config file (for DB credentials and such')
    return parser.parse_args()

def main():
    global_configuration()
    args = process_arguments()
    args.output_formats = set(args.output_formats)
    logging.info("bulletin-date is %s; output-dir is %s; output-formats is %s",
                 args.bulletin_date, args.output_dir, args.output_formats)

    engine = create_db(args)
    with engine.connect() as connection:
        cumulative(connection, args)
        lateness(connection, args)
        doubling(connection, args)
        daily_deltas(connection, args)

def global_configuration():
    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO)

    alt.themes.register("custom_theme", custom_theme)
    alt.themes.enable("custom_theme")
    alt.renderers.enable('altair_saver', fmts=['png'])

    alt.renderers.set_embed_options(
        timeFormatLocale={
            "dateTime": "%x, %X",
            "date": "%d/%m/%Y",
            "time": "%-I:%M:%S %p",
            "periods": ["AM", "PM"],
            "days": ["domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"],
            "shortDays": ["dom", "lun", "mar", "mié", "jue", "vie", "sáb"],
            "months": ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"],
            "shortMonths": ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
        }
    )

def custom_theme():
    return {
        "config": {
            "title": {
                "fontSize": 20,
            },
            "axis": {
                "labelFontSize": 14,
                "titleFontSize": 14
            },
            "legend": {
                "labelFontSize": 14,
                "titleFontSize": 14
            },
            "header": {
                "labelFontSize": 14,
                "titleFontSize": 14
            }
        }
    }


def cumulative(connection, args):
    df = cumulative_data(connection, args)
    logging.info("cumulative frame: %s", describe_frame(df))
    basename = f"{args.output_dir}/cumulative_{args.bulletin_date}"
    save_chart(cumulative_chart(df), basename, args.output_formats)

def cumulative_chart(df):
    return alt.Chart(df).mark_line(point=True).encode(
        x=alt.X('datum_date:T', title="Fecha de la muestra o muerte"),
        y=alt.Y('value', title="Casos únicos o muertes (cumulativo)",
                scale=alt.Scale(type='log')),
        color=alt.Color('variable', title=None,
                        legend=alt.Legend(orient="top", labelLimit=250)),
        tooltip=['datum_date', 'variable', 'value']
    ).properties(
        title="Los conteos cumulativos que se anuncian cada día vs. revisiones posteriores",
        width=1200,
        height=800
    )

def cumulative_data(connection, args):
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('cumulative_data', meta, schema='products',
                             autoload_with=connection)
    query = select([table.c.datum_date,
                    table.c.confirmed_cases,
                    table.c.probable_cases,
                    table.c.positive_results,
                    table.c.announced_cases,
                    table.c.deaths,
                    table.c.announced_deaths]).where(table.c.bulletin_date == args.bulletin_date)
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


def lateness(connection, args):
    df = lateness_data(connection, args)
    logging.info("lateness frame: %s", describe_frame(df))
    basename = f"{args.output_dir}/lateness_{args.bulletin_date}"
    save_chart(lateness_chart(df), basename, args.output_formats)

def lateness_chart(df):
    return alt.Chart(df).mark_bar().encode(
        y=alt.Y('value', title="Rezado estimado (días)"),
        x=alt.X('variable', title=None,
                sort=['Confirmados y probables',
                      'Confirmados',
                      'Probables',
                      'Muertes']),
        color=alt.Color('variable', legend=None),
        tooltip=['variable', 'bulletin_date',
                 alt.Tooltip(field='value',
                             type='quantitative',
                             format=".1f")]
    ).properties(
        width=150,
        height=600
    ).facet(
        column=alt.X("bulletin_date", sort="descending",
                     title="Fecha del boletín")
    ).properties(
        title="Es común que tome una semana entre toma de muestra y aviso en boletín"
    )

def lateness_data(connection, args):
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('lateness', meta, schema='products',
                             autoload_with=connection)
    query = select([table.c.bulletin_date,
                    table.c.confirmed_and_probable_cases,
                    table.c.confirmed_cases,
                    table.c.probable_cases,
                    table.c.deaths]
    ).where(
        and_(args.bulletin_date - datetime.timedelta(days=7) < table.c.bulletin_date,
             table.c.bulletin_date <= args.bulletin_date)
    )
    df = pd.read_sql_query(query, connection)
    df = df.rename(columns={
        'confirmed_and_probable_cases': 'Confirmados y probables',
        'confirmed_cases': 'Confirmados',
        'probable_cases': 'Probables',
        'deaths': 'Muertes'
    })
    return fix_and_melt(df, "bulletin_date")


def doubling(connection, args):
    df = doubling_data(connection, args)
    logging.info("doubling frame: %s", describe_frame(df))
    basename = f"{args.output_dir}/doubling_{args.bulletin_date}"
    save_chart(doubling_chart(df), basename, args.output_formats)

def doubling_chart(df):
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
        row=alt.Y('window_size_days:O', title='Window size (days)')
    ).properties(
        title="El tiempo de duplicación de positivos y muertes ha bajado consistentemente"
    )

def doubling_data(connection, args):
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('doubling_times', meta, schema='products',
                             autoload_with=connection)
    query = select([table.c.datum_date,
                    table.c.window_size_days,
                    table.c.cumulative_confirmed_and_probable_cases,
                    table.c.cumulative_confirmed_cases,
                    table.c.cumulative_probable_cases,
                    table.c.cumulative_deaths]
    ).where(
        table.c.bulletin_date == args.bulletin_date
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


def daily_deltas(connection, args):
    df = daily_deltas_data(connection, args)
    logging.info("deltas frame: %s", describe_frame(df))

    basename = f"{args.output_dir}/daily_deltas_{args.bulletin_date}"
#    save_chart(daily_deltas_chart(df), basename)
    save_chart(workaround_daily_deltas_chart(df), basename, args.output_formats)

def workaround_daily_deltas_chart(df):
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
                     title="Bulletin date"),
        row=alt.Y('variable', title=None,
                  sort=['Confirmados y probables',
                        'Confirmados',
                        'Probables',
                        'Muertes'])
    ).properties(
        title="Muchas veces los casos que se añaden (¡o quitan!) son viejitos"
    )

def daily_deltas_data(connection, args):
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('daily_deltas', meta, schema='products',
                             autoload_with=connection)
    query = select([table.c.bulletin_date,
                    table.c.datum_date,
                    table.c.delta_confirmed_cases,
                    table.c.delta_probable_cases,
                    table.c.delta_deaths]
    ).where(
        and_(args.bulletin_date - datetime.timedelta(days=7) < table.c.bulletin_date,
             table.c.bulletin_date <= args.bulletin_date)
    )
    df = pd.read_sql_query(query, connection)
    df = df.rename(columns={
        'delta_confirmed_cases': 'Confirmados',
        'delta_probable_cases': 'Probables',
        'delta_deaths': 'Muertes'
    })
    return fix_and_melt(df, "bulletin_date", "datum_date")


def create_db(args):
    config = {
        'drivername': 'postgres',
        'port': 5432
    }
    toml_dict = toml.load(args.config_file)
    config.update(toml_dict['database'])
    url = sqlalchemy.engine.url.URL(**config)
    return sqlalchemy.create_engine(url)

def save_chart(chart, basename, formats):
    for format in formats:
        filename = f"{basename}.{format}"
        logging.info("Writing chart to %s", filename)
        chart.save(filename)

def fix_date_columns(df, *date_columns):
    """Pandas is making us frames with object type for date columns,
    which some libraries hate."""
    for col in date_columns:
        df[col] = pd.to_datetime(df[col])
    return df

def fix_and_melt(df, *date_columns):
    return pd.melt(fix_date_columns(df, *date_columns), date_columns)

def describe_frame(df):
    """Because df.info() prints instead of returning a string."""
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()


if __name__ == '__main__':
    main()