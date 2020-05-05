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
    parser = argparse.ArgumentParser(description='Generate Puerto Rico COVID-19 graphs')
    parser.add_argument('--output-dir', type=str, required=True,
                        help='Directory into which to place output')
    parser.add_argument('--bulletin-date', type=datetime.date.fromisoformat, required=True,
                        help='Bulletin date to generate graphs for')
    parser.add_argument('--config-file', type=str, required=True,
                        help='TOML config file (for DB credentials and such')
    return parser.parse_args()

def main():
    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO)
    args = process_arguments()
    logging.info("bulletin-date is %s; output-dir is %s",
                 args.bulletin_date, args.output_dir)

    engine = create_db(args)
    with engine.connect() as connection:
        cumulative(connection, args)
        lateness(connection, args)
        doubling(connection, args)
        daily_deltas(connection, args)


def cumulative(connection, args):
    df = cumulative_data(connection, args)
    logging.info("cumulative frame: %s", describe_frame(df))
    basename = f"{args.output_dir}/cumulative_{args.bulletin_date}"
    save_graph(cumulative_graph(df), basename)

def cumulative_graph(df):
    return alt.Chart(df).mark_line(point=True).encode(
        x=alt.X('datum_date:T', title="Date of test sample or death"),
        y=alt.Y('value', title="Cumulative cases or deaths",
                scale=alt.Scale(type='log')),
        color=alt.Color('variable', title=None),
        tooltip=['datum_date', 'variable', 'value']
    ).properties(
        width=1024,
        height=768
    )

def cumulative_data(connection, args):
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
    df = df.rename(columns={
        'confirmed_cases': 'Confirmed cases  (by test date)',
        'probable_cases': 'Probable cases (by test date)',
        'positive_results': 'Positive results (by bulletin date)',
        'announced_cases': 'Cases (by bulletin date)',
        'deaths': 'Deaths (by actual date)',
        'announced_deaths': 'Deaths (by bulletin date)'
    })
    return fix_and_melt(df, "datum_date")


def lateness(connection, args):
    df = lateness_data(connection, args)
    logging.info("lateness frame: %s", describe_frame(df))
    basename = f"{args.output_dir}/lateness_{args.bulletin_date}"
    save_graph(lateness_graph(df), basename)

def lateness_graph(df):
    return alt.Chart(df).mark_bar().encode(
        x=alt.X('value', title="Estimated lag (days)"),
        y=alt.Y('variable', title=None,
                sort=['Confirmed and probable',
                      'Confirmed',
                      'Probable',
                      'Deaths']),
        color=alt.Color('variable', legend=None),
        tooltip=['variable', 'bulletin_date',
                 alt.Tooltip(field='value',
                             type='quantitative',
                             format=".1f")]
    ).facet(
        row=alt.Y("bulletin_date", title="Bulletin date",
                  sort="descending")
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
        table.c.bulletin_date <= args.bulletin_date
    )
    df = pd.read_sql_query(query, connection)
    df = df.rename(columns={
        'confirmed_and_probable_cases': 'Confirmed and probable',
        'confirmed_cases': 'Confirmed',
        'probable_cases': 'Probable',
        'deaths': 'Deaths'
    })
    return fix_and_melt(df, "bulletin_date")


def doubling(connection, args):
    df = doubling_data(connection, args)
    logging.info("doubling frame: %s", describe_frame(df))
    basename = f"{args.output_dir}/doubling_{args.bulletin_date}"
    save_graph(doubling_graph(df), basename)

def doubling_graph(df):
    return alt.Chart(df.dropna()).mark_line(clip=True).encode(
        x=alt.X('datum_date:T', title='Event date'),
        y=alt.Y('value', title="Doubling time (days)",
                scale=alt.Scale(type='log', domain=(1, 100))),
        color=alt.Color('variable', legend=None)
    ).properties(
        width=256,
        height=256
    ).facet(
        column=alt.X('variable', title=None,
                     sort=['Confirmed and probable',
                           'Confirmed',
                           'Probable',
                           'Deaths']),
        row=alt.Y('window_size_days:O', title='Window size (days)')
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
        'cumulative_confirmed_and_probable_cases': 'Confirmed and probable',
        'cumulative_confirmed_cases': 'Confirmed',
        'cumulative_probable_cases': 'Probable',
        'cumulative_deaths': 'Deaths'
    })
    return pd.melt(fix_date_columns(df, "datum_date"),
                   ["datum_date", "window_size_days"])


def daily_deltas(connection, args):
    df = daily_deltas_data(connection, args)
    logging.info("deltas frame: %s", describe_frame(df))

    basename = f"{args.output_dir}/daily_deltas_{args.bulletin_date}"
#    save_graph(daily_deltas_graph(df), basename)
    save_graph(workaround_daily_deltas_graph(df), basename)

def daily_deltas_graph(df):
    return alt.Chart(df).transform_filter(
        alt.datum.value != 0
    ).mark_bar().encode(
        x=alt.X('value', title="Cases added/subtracted"),
        y=alt.Y('datum_date:T', title="Event date"),
        color=alt.Color('variable', legend=None),
        tooltip = ['variable', 'datum_date:T', 'value']
    ).properties(
        width=250,
        height=250
    ).facet(
        row=alt.Y('bulletin_date:T', sort="descending",
                  title="Bulletin date"),
        column=alt.X('variable', title=None,
                     sort=['Confirmed and probable',
                           'Confirmed',
                           'Probable',
                           'Deaths'])
    )

def workaround_daily_deltas_graph(df):
    def bug_workaround(df):
        """If both of these conditions hold:

         1. One of the subgraphs in this faceted graph has
            no data points;
         2. I custom sort the faceting grid column;

         ...then I get an empty subgraph (no gridlines even)
         and the sorting of the columns for that row breaks."""
        filtered = df\
            .replace(0, np.nan)\
            .dropna()
        return (min(filtered['datum_date']), max(filtered['datum_date']))

    return alt.Chart(df).mark_bar(clip=True).encode(
        x=alt.X('value', title="Cases added/subtracted"),
        y=alt.Y('datum_date:T', title="Event date",
                scale=alt.Scale(domain=bug_workaround(df))),
        color=alt.Color('variable', legend=None),
        tooltip = ['variable', 'datum_date:T', 'value']
    ).properties(
        width=250,
        height=250
    ).facet(
        row=alt.Y('bulletin_date:T', sort="descending",
                  title="Bulletin date"),
        column=alt.X('variable', title=None,
                     sort=['Confirmed and probable',
                           'Confirmed',
                           'Probable',
                           'Deaths'])
    )

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
    df = df.rename(columns={
        'delta_confirmed_and_probable_cases': 'Confirmed and probable',
        'delta_confirmed_cases': 'Confirmed',
        'delta_probable_cases': 'Probable',
        'delta_deaths': 'Deaths'
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

def save_graph(graph, basename):
    filename = f"{basename}.html"
    logging.info("Writing graph to %s", filename)
    graph.save(filename)

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