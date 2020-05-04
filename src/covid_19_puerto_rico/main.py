#/usr/bin/env/python3

import altair as alt
import argparse
import datetime
import io
import logging
import pandas as pd
import sqlalchemy
from sqlalchemy.sql import select, and_

def process_arguments():
    parser = argparse.ArgumentParser(description='Generate Puerto Rico COVID-19 graphs')
    parser.add_argument('--output-dir', type=str,
                        help='Directory into which to place output')
    parser.add_argument('--bulletin-date', type=datetime.date.fromisoformat,
                        help='Bulletin date to generate graphs for')
    return parser.parse_args()

def main():
    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO)
    args = process_arguments();
    logging.info("bulletin-date is %s; output-dir is %s",
                 args.bulletin_date, args.output_dir)

    engine = create_db()
    with engine.connect() as connection:
        main_graph(connection, args)
        lateness_graph(connection, args)
        doubling_graph(connection, args)
        daily_deltas_graph(connection, args)

def main_graph(connection, args):
    df = main_graph_data(connection, args)
    logging.info("main_graph frame: %s", describe_frame(df))
    lines = alt.Chart(df).mark_line(point=True).encode(
        x='datum_date:T',
        y=alt.X('value', scale=alt.Scale(type='log')),
        color='variable',
        tooltip=['datum_date', 'variable', 'value']
    ).properties(
        width=1024,
        height=768
    )
    filename = f"{args.output_dir}/main_{args.bulletin_date}.html"
    logging.info("Writing graph to %s", filename)
    lines.save(filename)

def main_graph_data(connection, args):
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('main_graph', meta, schema='products',
                             autoload_with=connection)
    query = select([table.c.datum_date,
                    table.c.confirmed_cases,
                    table.c.probable_cases,
                    table.c.positive_results,
                    table.c.announced_cases,
                    table.c.deaths,
                    table.c.announced_deaths]).where(table.c.bulletin_date == args.bulletin_date)
    df = pd.read_sql_query(query, connection)
    return adjust_frame(df, "datum_date")

def lateness_graph(connection, args):
    df = lateness_data(connection, args)
    logging.info("lateness frame: %s", describe_frame(df))
    bars = alt.Chart(df).mark_bar().encode(
        x='value',
        y='variable',
        color='variable'
    ).properties(
        width=640,
    ).facet(
        row="bulletin_date"
    )
    filename = f"{args.output_dir}/lateness_{args.bulletin_date}.html"
    logging.info("Writing graph to %s", filename)
    bars.save(filename)

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
        table.c.bulletin_date <= args.bulletin_date
    )
    df = pd.read_sql_query(query, connection)
    return adjust_frame(df, "bulletin_date")

def adjust_frame(df, date_column):
    df[date_column] = pd.to_datetime(df[date_column])
    return pd.melt(df, date_column)

def describe_frame(df):
    """Because df.info() prints instead of returning a string."""
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()

def doubling_graph(connection, args):
    df = doubling_data(connection, args)
    logging.info("doubling frame: %s", describe_frame(df))
    lines = alt.Chart(df).mark_line().encode(
        x='datum_date',
        y=alt.X('value', scale=alt.Scale(type='log')),
    ).properties(
        width=256,
        height=256
    ).facet(
        column='variable',
        row='window_size_days:O'
    )
    filename = f"{args.output_dir}/doubling_{args.bulletin_date}.html"
    logging.info("Writing graph to %s", filename)
    lines.save(filename)

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
    df["datum_date"] = pd.to_datetime(df["datum_date"])
    return pd.melt(df, ["datum_date", "window_size_days"])


def daily_deltas_graph(connection, args):
    df = daily_deltas_data(connection, args)
    logging.info("deltas frame: %s", describe_frame(df))
    bars = alt.Chart(df).mark_bar().encode(
        x='value',
        y='datum_date',
        tooltip = ['variable', 'datum_date', 'value']
    ).properties(
        width=250,
        height=250
    ).facet(
        row='bulletin_date:O',
        column='variable'
    )
    filename = f"{args.output_dir}/daily_deltas_{args.bulletin_date}.html"
    logging.info("Writing graph to %s", filename)
    bars.save(filename)

def daily_deltas_data(connection, args):
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('daily_deltas', meta, schema='products',
                             autoload_with=connection)
    query = select([table.c.bulletin_date,
                    table.c.datum_date,
                    table.c.delta_confirmed_and_probable_cases,
                    table.c.delta_confirmed_cases,
                    table.c.delta_probable_cases,
                    table.c.delta_deaths]
    ).where(
        and_(args.bulletin_date - datetime.timedelta(days=3) < table.c.bulletin_date,
             table.c.bulletin_date <= args.bulletin_date)
    )
    df = pd.read_sql_query(query, connection)
    df["bulletin_date"] = pd.to_datetime(df["bulletin_date"])
    df["datum_date"] = pd.to_datetime(df["datum_date"])
    return pd.melt(df, ["bulletin_date", "datum_date"])


def create_db():
    url = sqlalchemy.engine.url.URL(
        drivername = 'postgres',
        username = 'postgres',
        password = 'password',
        host = 'localhost')
    return sqlalchemy.create_engine(url)

if __name__ == '__main__':
    main()