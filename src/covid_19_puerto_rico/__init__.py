import altair as alt
import argparse
import datetime
import importlib.resources
import json
import logging
import sqlalchemy
from sqlalchemy.sql import select
from sqlalchemy.sql.functions import max

from . import animations
from . import charts
from . import resources
from . import util
from . import website

def process_arguments():
    parser = argparse.ArgumentParser(description='Generate Puerto Rico COVID-19 charts')
    parser.add_argument('--config-file', type=str, required=True,
                        help='TOML config file (for DB credentials and such')
    parser.add_argument('--assets-dir', type=str, required=True,
                        help='Static website assets directory')
    parser.add_argument('--output-dir', type=str, required=True,
                        help='Directory into which to place output')
    parser.add_argument('--bulletin-date', type=datetime.date.fromisoformat,
                        help='Bulletin date to generate charts for. Default: most recent in DB.')
    parser.add_argument('--earliest-date', type=datetime.date.fromisoformat,
                        default=datetime.date(2020, 4, 25),
                        help='Earliest date to generate website for. Has a sensible built-in default.')
    parser.add_argument('--png', action='store_true',
                        help="Switch generate all the PNG files (which is a bit slow)")
    parser.add_argument('--website', action='store_true',
                        help="Switch to run the website generation (which is a bit slow)")
    parser.add_argument('--animations', action='store_true',
                        help="Switch to run the animations (which are a bit slow to generate)")
    return parser.parse_args()

def main():
    global_configuration()
    args = process_arguments()
    logging.info("output-dir is %s", args.output_dir)

    engine = util.create_db(args)
    bulletin_date = compute_bulletin_date(args, engine)
    logging.info('Using bulletin date of %s', bulletin_date)

    if args.png:
        output_formats = frozenset(['json', 'png'])
    else:
        output_formats = frozenset(['json'])

    targets = [
        charts.LatenessDaily(engine, args.output_dir, output_formats),

        # We always generate PNG for this because it's our Twitter card
        charts.Lateness7Day(engine, args.output_dir,
                            frozenset(['json', 'png']))
    ]

    if args.animations:
        targets.append(animations.CaseLag(engine, args))

    date_range = list(
        util.make_date_range(args.earliest_date, bulletin_date)
    )

    if args.website:
        site = website.Website(args, date_range)
        targets.append(site)

    charts.Cumulative(engine, args.output_dir, output_formats).render(date_range)
    charts.NewCases(engine, args.output_dir, output_formats).render(date_range)
    charts.Doubling(engine, args.output_dir, output_formats).render(date_range)
    charts.DailyDeltas(engine, args.output_dir, output_formats).render(date_range)

    for date in date_range:
        for target in targets:
            target.render(date)

    if args.website:
        site.copy_assets()
        site.render_top(bulletin_date)



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


def compute_bulletin_date(args, engine):
    if args.bulletin_date != None:
        return args.bulletin_date
    else:
        return query_for_bulletin_date(engine)

def query_for_bulletin_date(engine):
    metadata = sqlalchemy.MetaData(engine)
    with engine.connect() as connection:
        table = sqlalchemy.Table('bitemporal', metadata, autoload=True)
        query = select([max(table.c.bulletin_date)])
        result = connection.execute(query)
        return result.fetchone()[0]