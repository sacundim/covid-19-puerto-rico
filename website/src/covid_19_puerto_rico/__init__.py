import altair as alt
import argparse
import concurrent.futures as futures
import datetime
import logging
import sqlalchemy
import sqlalchemy.sql as sql
import sqlalchemy.sql.functions as sqlfn
import vegafusion as vf

from . import charts
from . import molecular
from . import util
from . import website

def process_arguments():
    parser = argparse.ArgumentParser(description='Generate Puerto Rico COVID-19 charts')
    parser.add_argument('--config-file', type=str, required=True,
                        help='TOML config file (for DB credentials and such')
    parser.add_argument('--assets-dir', type=str, required=False,
                        help='Static website assets directory')
    parser.add_argument('--output-dir', type=str, required=True,
                        help='Directory into which to place output')
    parser.add_argument('--bulletin-date', type=datetime.date.fromisoformat,
                        help='Bulletin date to generate charts for. Default: most recent in DB.')
    parser.add_argument('--earliest-date', type=datetime.date.fromisoformat,
                        default=datetime.date(2021, 7, 24),
                        help='Earliest date to generate website for. Has a sensible built-in default.')
    parser.add_argument('--days-back', type=int, default=15,
                        help='How many days back from the most recent to build for. Default: 15.')
    parser.add_argument('--no-svg', action='store_false', dest='svg',
                        help="Switch turn off the svg files (which is a bit slow)")
    parser.add_argument('--no-website', action='store_false', dest='build_website',
                        help="Switch to turn off website generation (which is a bit slow)")
    parser.add_argument('--no-assets', action='store_false', dest='build_assets',
                        help="Switch to turn off website static assets generation (which is slow)")
    return parser.parse_args()

def main():
    global_configuration()
    args = process_arguments()
    logging.info("output-dir is %s", args.output_dir)

    athena = util.create_athena_engine(args)
    bulletin_date = compute_bulletin_date(args, athena)
    logging.info('Using bulletin date of %s', bulletin_date)

    if args.svg:
        output_formats = frozenset(['json', 'svg'])
    else:
        output_formats = frozenset(['json'])

    targets = [
        # Occasional:
        #        molecular.MunicipalSPLOM(athena, args.output_dir, output_formats),

        molecular.MunicipalTestingScatter(athena, args.output_dir, output_formats),
        molecular.NewCases(athena, args.output_dir, output_formats),

        # We always generate png for this because they're our Twitter cards
        molecular.RecentCases(athena, args.output_dir, frozenset(['json', 'svg', 'png'])),

        # Disabled because it's broken, the Athena staging times out, and others
        # do it better than I do:
        #molecular.VaccinationMap(athena, args.output_dir, output_formats),

        charts.MunicipalMap(athena, args.output_dir, output_formats),
        molecular.RecentAgeGroups(athena, args.output_dir, output_formats),
        molecular.AgeGroups(athena, args.output_dir, output_formats),
        charts.Municipal(athena, args.output_dir, output_formats),
        molecular.RecentHospitalizations(athena, args.output_dir, output_formats),
        charts.WeekdayBias(athena, args.output_dir, output_formats),
        charts.LatenessTiers(athena, args.output_dir, output_formats),
        charts.CurrentDeltas(athena, args.output_dir, output_formats),
        charts.DailyDeltas(athena, args.output_dir, output_formats),
        molecular.EncounterLag(athena, args.output_dir, output_formats),
        molecular.NaivePositiveRate(athena, args.output_dir, output_formats),
        molecular.CaseFatalityRate(athena, args.output_dir, output_formats),
        molecular.NewTestSpecimens(athena, args.output_dir, output_formats),
        molecular.ConfirmationsVsRejections(athena, args.output_dir, output_formats),
        molecular.MolecularCurrentDeltas(athena, args.output_dir, output_formats),
        molecular.MolecularDailyDeltas(athena, args.output_dir, output_formats),
    ]

    start_date = max([args.earliest_date,
                      bulletin_date - datetime.timedelta(days=args.days_back)])
    date_range = list(
        util.make_date_range(start_date, bulletin_date)
    )

    with futures.ThreadPoolExecutor(thread_name_prefix='worker_thread') as executor:
        if args.build_website:
            site = website.Website(args)
            targets = [site] + targets
        for future in futures.as_completed([executor.submit(target, date_range) for target in targets]):
            logging.info("Completed %s", future.result())

    if args.build_website:
        site.render_top(bulletin_date)



def global_configuration():
    logging.basicConfig(format='%(asctime)s %(threadName)s %(message)s',
                        level=logging.INFO)

    alt.themes.register("custom_theme", lambda: util.get_json_resource('theme.json'))
    alt.themes.enable("custom_theme")
    alt.renderers.set_embed_options(
        timeFormatLocale=util.get_json_resource('es-PR.json')
    )

    vf.enable()



def compute_bulletin_date(args, engine):
    if args.bulletin_date != None:
        return args.bulletin_date
    else:
        return query_for_bulletin_date(engine)

def query_for_bulletin_date(engine):
    metadata = sqlalchemy.MetaData(engine)
    with engine.connect() as connection:
        table = sqlalchemy.Table('bulletin_cases', metadata, autoload=True)
        query = sql.select([sqlfn.max(table.c.bulletin_date)])
        result = connection.execute(query)
        return result.fetchone()[0]