import datetime
import io
import logging
import pandas as pd
import sqlalchemy
import toml


def make_date_range(start, end):
    """Inclusive date range"""
    return [start + datetime.timedelta(n)
            for n in range(int((end - start).days) + 1)]

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

def describe_frame(df):
    """Because df.info() prints instead of returning a string."""
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()