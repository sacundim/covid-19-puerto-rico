import altair as alt
import datetime
from envyaml import EnvYAML
import geojson
import importlib.resources
import io
import json
import logging
from math import log10, floor
import pandas as pd
import platform
import polars as pl
import pyathena
from pyathena.arrow.cursor import ArrowCursor
from pyathena.pandas.cursor import PandasCursor
from urllib.parse import quote_plus

from . import resources

def make_date_range(start, end):
    """Inclusive date range"""
    return [start + datetime.timedelta(n)
            for n in range(int((end - start).days) + 1)]


def save_chart(chart, basename, formats):
    for format in formats:
        filename = f"{basename}.{format}"
        logging.debug("Writing chart to %s", filename)
        chart.save(filename)

def heatmap_text_color(df, field, extreme_color='white', mid_color='black'):
    """Compute the color of the text so that it'll contrast with
    the diverging color scale of the heatmap."""
    lo, hi = midrange(lo=df[field].min(), hi=df[field].max())
    return alt.condition(
        # This is tricky because I'm picky: I want the mid range to
        # have **exclusive** endpoints, but FieldRangePredicate is
        # inclusive.  Hence this dance:
        alt.LogicalOrPredicate(
            **{'or': [alt.FieldLTPredicate(field=field, lt=lo),
                      alt.FieldGTPredicate(field=field, gt=hi)]}),
        alt.value(extreme_color),
        alt.value(mid_color)
    )

def midrange(lo, hi, mid=0, scale=1.0):
    """Compute the range of values in the middle of a diverging scale."""
    return [min(mid, (mid + lo) / (1.0 + scale)),
            max(mid, (mid + hi) / (1.0 + scale))]

def describe_frame(df):
    """Because df.info() prints instead of returning a string."""
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()

def altair_date_expr(date):
    return alt.expr.toDate(f'{date.isoformat()}T00:00:00')

def get_json_resource(filename):
    text = importlib.resources.read_text(resources, filename)
    return json.loads(text)

def get_geojson_resource(filename):
    text = importlib.resources.read_text(resources, filename)
    return geojson.loads(text)


def round_up_sig(x, sig=2):
    """Round `x` upwards to `sig` significant digits."""
    magnitude = int(floor(log10(abs(x))))
    rounded = round(x, sig - magnitude - 1)
    if rounded < x:
        return rounded + 10 ** (magnitude - (sig - 1))
    else:
        return rounded

def round_down_sig(x, sig=2):
    """Round `x` downwards to `sig` significant digits."""
    magnitude = int(floor(log10(abs(x))))
    rounded = round(x, sig - magnitude - 1)
    if rounded > x:
        return rounded - 10 ** (magnitude - (sig - 1))
    else:
        return rounded


def log_platform(level=logging.INFO):
    """Log `platform.uname()` results, to confirm what CPU arch we're using."""
    uname = platform.uname()
    logging.log(level, "Platform: system=%s, machine=%s, version=%s",
                uname.system, uname.machine, uname.version)


##########################################################
##########################################################
##
## Athena/Arrow/Polars
##

def create_pyathena_connection(args):
    yaml_dict = EnvYAML(args.config_file)
    config = (yaml_dict['athena'])
    return pyathena.connect(
        region_name=config['region_name'],
        schema_name=config['schema_name'],
        work_group=config['work_group'],
        cursor_class=PandasCursor
    )

def execute_pandas(athena, sql, params={}):
    with athena.cursor(PandasCursor) as cursor:
        return cursor.execute(sql, params).as_pandas()

def execute_polars(athena, sql, params={}):
    with athena.cursor(ArrowCursor) as cursor:
        return pl.from_arrow(cursor.execute(sql, params).as_arrow())

def describe_frame(df):
    if isinstance(df, pd.DataFrame):
        return describe_pandas_frame(df)
    elif isinstance(df, pl.DataFrame):
        return str(df)

def describe_pandas_frame(df):
    """Because df.info() prints instead of returning a string."""
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()
