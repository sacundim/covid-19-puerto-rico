#/usr/bin/env/python3

import altair as alt
import argparse
import datetime
import logging
import pandas as pd
import sqlalchemy
from sqlalchemy.sql import select

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

def main_graph(connection, args):
    df = main_graph_data(connection, args)
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

def create_db():
    url = sqlalchemy.engine.url.URL(
        drivername = 'postgres',
        username = 'postgres',
        password = 'password',
        host = 'localhost')
    return sqlalchemy.create_engine(url)

if __name__ == '__main__':
    main()