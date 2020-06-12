import altair as alt
import datetime
import geojson
import importlib.resources
import io
import json
import logging
import sqlalchemy
import toml

from . import resources

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

def heatmap_text_color(df, field, extreme_color='white', mid_color='black'):
    """Compute the color of the text so that it'll contrast with
    the diverging color scale of the heatmap."""
    lo, hi = midrange(lo=df[field].min(), hi=df[field].max())
    return alt.condition(
        # This is tricky because I'm picky: I want the mid range to
        # have **exclusive** endpoints, but FieldRangePredicate is
        # inclusive.  Hence this dance:
        alt.LogicalOrPredicate(
            **{'or': [alt.FieldLTEPredicate(field=field, lte=lo),
                      alt.FieldGTEPredicate(field=field, gte=hi)]}),
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


def get_json_resource(filename):
    text = importlib.resources.read_text(resources, filename)
    return json.loads(text)

def get_geojson_resource(filename):
    text = importlib.resources.read_text(resources, filename)
    return geojson.loads(text)
