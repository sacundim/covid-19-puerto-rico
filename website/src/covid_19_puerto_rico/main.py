import altair as alt
import argparse
import concurrent.futures as futures
import datetime
import logging
import pathlib
import subprocess
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
    parser.add_argument('--rclone-destination', type=str, default=None,
                        help='If given, the `--output-dir` will be copied over to that destination with `rclone`.')

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

    parser.add_argument('--rclone-command', type=str, default='rclone',
                        help='Override the path to the rclone command. Default: `rclone`.')

    return parser.parse_args()

def main():
    global_configuration()
    args = process_arguments()
    logging.info("output-dir is %s", args.output_dir)
    pathlib.Path(args.output_dir).mkdir(exist_ok=True)

    with util.create_pyathena_connection(args) as athena:
        run(args, athena)

def run(args, athena):
    bulletin_date = compute_bulletin_date(args, athena)
    logging.info('Using bulletin date of %s', bulletin_date)

    if args.svg:
        output_formats = frozenset(['json', 'svg'])
    else:
        output_formats = frozenset(['json'])

    targets = [
        # We always generate png for this because they're our Twitter cards
        molecular.RecentCases(athena, args.output_dir, frozenset(['json', 'svg', 'png'])),

        molecular.MunicipalTestingScatter(athena, args.output_dir, output_formats),
        molecular.NewCases(athena, args.output_dir, output_formats),
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

    if args.output_dir and args.rclone_destination:
        rclone(
            args.output_dir,
            args.rclone_destination,
            args.rclone_command)


def global_configuration():
    logging.basicConfig(format='%(asctime)s %(threadName)s %(message)s',
                        level=logging.INFO)
    util.log_platform()
#    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    alt.themes.register("custom_theme", lambda: util.get_json_resource('theme.json'))
    alt.themes.enable("custom_theme")
    alt.renderers.set_embed_options(
        timeFormatLocale=util.get_json_resource('es-PR.json')
    )

    vf.enable()



def compute_bulletin_date(args, athena):
    if args.bulletin_date != None:
        return args.bulletin_date
    else:
        return query_for_bulletin_date(athena)

def query_for_bulletin_date(connection):
    query = """
    SELECT max(bulletin_date)
    FROM bulletin_cases"""
    with connection.cursor() as cursor:
        result = cursor.execute(query)
        return result.fetchone()[0]


def rclone(output_dir, rclone_destination, rclone_command):
    logging.info("rclone from: %s to: %s...", output_dir, rclone_destination)
    subprocess.run([
        rclone_command, 'copy',
        '--fast-list',
        '--verbose',
        '--checksum',
        '--exclude', '*.DS_Store',
        output_dir,
        rclone_destination
    ])
    logging.info("rclone from: %s to: %s... DONE", output_dir, rclone_destination)
