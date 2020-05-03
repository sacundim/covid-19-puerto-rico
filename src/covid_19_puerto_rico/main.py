#/usr/bin/env/python3

import argparse
import datetime
import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import sqlalchemy
from sqlalchemy.sql import select

def main():
    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO)
    logging.info('Hello World!')
    sns.set(style="darkgrid")

    engine = create_db()
    with engine.connect() as connection:
        main_graph(connection, datetime.date(2020, 5, 2))

def main_graph(connection, bulletin_date):
    meta = sqlalchemy.MetaData()
    table = sqlalchemy.Table('main_graph', meta, autoload=True, autoload_with=connection)
    query = select([table.c.datum_date,
                    table.c.confirmed_cases,
                    table.c.probable_cases,
                    table.c.positive_results,
                    table.c.announced_cases,
                    table.c.deaths,
                    table.c.announced_deaths]).where(table.c.bulletin_date == bulletin_date)
    df = pd.read_sql_query(query, connection)
    logging.info("%s", df['positive_results'])

    graph = sns.relplot(x='datum_date', y='value', kind='line', hue='variable',
                        data=pd.melt(df, ['datum_date']))
    graph.fig.autofmt_xdate()
    graph.set(yscale='log')

    logging.info("Writing graph to main.png")
    graph.savefig("output/main.png", dpi=150)


def create_db():
    url = sqlalchemy.engine.url.URL(
        drivername = 'postgres',
        username = 'postgres',
        password = 'password',
        host = 'localhost')
    return sqlalchemy.create_engine(url)

if __name__ == '__main__':
    main()