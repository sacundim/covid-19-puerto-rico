import altair as alt
import argparse
import datetime
import importlib.resources
import json

from . import animations
from . import charts
from . import resources
from .util import *

def process_arguments():
    parser = argparse.ArgumentParser(description='Generate Puerto Rico COVID-19 charts')
    parser.add_argument('--output-dir', type=str, required=True,
                        help='Directory into which to place output')
    parser.add_argument('--output-formats', action='append', default=['json'])
    parser.add_argument('--bulletin-date', type=datetime.date.fromisoformat, required=True,
                        help='Bulletin date to generate charts for')
    parser.add_argument('--config-file', type=str, required=True,
                        help='TOML config file (for DB credentials and such')
    parser.add_argument('--animations', action='store_true',
                        help="Switch to run the animations")
    parser.add_argument('--earliest-bulletin-date',
                        type=datetime.date.fromisoformat,
                        default=datetime.date(2020, 4, 25),
                        help="Earliest bitemporal bulletin date (you probably don't need to touch this)")
    return parser.parse_args()

def main():
    global_configuration()
    args = process_arguments()
    args.output_formats = set(args.output_formats)
    logging.info("bulletin-date is %s; output-dir is %s; output-formats is %s",
                 args.bulletin_date, args.output_dir, args.output_formats)

    engine = create_db(args)
    charts.Cumulative(engine, args).execute()
    charts.Lateness(engine, args).execute()
    charts.Doubling(engine, args).execute()
    charts.DailyDeltas(engine, args).execute()
    if args.animations:
        animations.CaseLag(engine, args).execute()

def global_configuration():
    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO)

    alt.themes.register("custom_theme", lambda: get_json_resource('theme.json'))
    alt.themes.enable("custom_theme")
    alt.renderers.enable('altair_saver', fmts=['png'])
    alt.renderers.set_embed_options(
        timeFormatLocale=get_json_resource('es-PR.json')
    )

def get_json_resource(filename):
    text = importlib.resources.read_text(resources, filename)
    return json.loads(text)


