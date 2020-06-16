import altair as alt
import argparse
import datetime
import logging
import sqlalchemy
from sqlalchemy.sql import select
from sqlalchemy.sql.functions import max

from . import charts
from . import molecular
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
                        default=datetime.date(2020, 5, 20),
                        help='Earliest date to generate website for. Has a sensible built-in default.')
    parser.add_argument('--no-svg', action='store_false', dest='svg',
                        help="Switch turn off the svg files (which is a bit slow)")
    parser.add_argument('--no-website', action='store_false', dest='website',
                        help="Switch to turn off website generation (which is a bit slow)")
    return parser.parse_args()

def main():
    global_configuration()
    args = process_arguments()
    logging.info("output-dir is %s", args.output_dir)

    engine = util.create_db(args)
    bulletin_date = compute_bulletin_date(args, engine)
    logging.info('Using bulletin date of %s', bulletin_date)

    if args.svg:
        output_formats = frozenset(['json', 'svg'])
    else:
        output_formats = frozenset(['json'])

    targets = [
        molecular.TestsPerCase(engine, args.output_dir, output_formats),
        charts.MunicipalMap(engine, args.output_dir, output_formats),
        charts.Municipal(engine, args.output_dir, output_formats),
        molecular.CumulativeMissingTests(engine, args.output_dir, output_formats),
        molecular.DailyMissingTests(engine, args.output_dir, output_formats),
        charts.CurrentDeltas(engine, args.output_dir, output_formats),
        charts.WeekdayBias(engine, args.output_dir, output_formats),
        charts.NewCases(engine, args.output_dir, output_formats),
        charts.DailyDeltas(engine, args.output_dir, output_formats),
        charts.LatenessDaily(engine, args.output_dir, output_formats),

        # We always generate png for this because they're our Twitter cards
        charts.Lateness7Day(engine, args.output_dir, frozenset(['json', 'svg', 'png']))
    ]
    if args.website:
        site = website.Website(args)
        targets.append(site)

    date_range = list(
        util.make_date_range(args.earliest_date, bulletin_date)
    )

    for target in targets:
        target.render(date_range)

    if args.website:
        site.render_top(bulletin_date)



def global_configuration():
    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO)

    alt.themes.register("custom_theme", lambda: util.get_json_resource('theme.json'))
    alt.themes.enable("custom_theme")
    alt.renderers.enable('altair_saver', fmts=['svg', 'png'])
    alt.renderers.set_embed_options(
        timeFormatLocale=util.get_json_resource('es-PR.json')
    )



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