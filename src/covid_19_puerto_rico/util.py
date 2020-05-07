import io
import logging
import pandas as pd
import sqlalchemy
import toml


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