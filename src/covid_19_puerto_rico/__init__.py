import altair as alt
import argparse
import datetime
import logging
import sqlalchemy
import sqlalchemy.sql as sql
import sqlalchemy.sql.functions as sqlfn

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
                        default=datetime.date(2020, 10, 1),
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

    postgres = util.create_postgres_engine(args)
    athena = util.create_athena_engine(args)
    bulletin_date = compute_bulletin_date(args, postgres)
    logging.info('Using bulletin date of %s', bulletin_date)

    if args.svg:
        output_formats = frozenset(['json', 'svg'])
    else:
        output_formats = frozenset(['json'])

    targets = [
#        molecular.MunicipalSPLOM(athena, args.output_dir, output_formats),
        molecular.VaccinationMap(athena, args.output_dir, output_formats),
        charts.MunicipalMap(postgres, args.output_dir, output_formats),
        molecular.AgeGroups(athena, args.output_dir, output_formats),
        molecular.NaivePositiveRate(athena, args.output_dir, output_formats),

        # We always generate png for this because they're our Twitter cards
        molecular.RecentCases(athena, args.output_dir, frozenset(['json', 'svg', 'png'])),

        molecular.NewCases(athena, args.output_dir, output_formats),
        molecular.Hospitalizations(athena, args.output_dir, output_formats),
        molecular.CaseFatalityRate(athena, args.output_dir, output_formats),
        charts.ICUsByRegion(postgres, args.output_dir, output_formats),
        charts.ICUsByHospital(postgres, args.output_dir, output_formats),
        molecular.NewTestSpecimens(athena, args.output_dir, output_formats),
        molecular.ConfirmationsVsRejections(athena, args.output_dir, output_formats),
        molecular.MolecularLatenessTiers(athena, args.output_dir, output_formats),
        molecular.MolecularCurrentDeltas(athena, args.output_dir, output_formats),
        molecular.MolecularDailyDeltas(athena, args.output_dir, output_formats),
        charts.LatenessTiers(postgres, args.output_dir, output_formats),
        charts.BulletinChartMismatch(postgres, args.output_dir, output_formats),
        charts.ConsecutiveBulletinMismatch(postgres, args.output_dir, output_formats),
        charts.Municipal(postgres, args.output_dir, output_formats),
        charts.CurrentDeltas(postgres, args.output_dir, output_formats),
        charts.WeekdayBias(postgres, args.output_dir, output_formats),
        charts.DailyDeltas(postgres, args.output_dir, output_formats),
        charts.LatenessDaily(postgres, args.output_dir, output_formats),
        charts.Lateness7Day(postgres, args.output_dir, output_formats)
    ]
    if args.website:
        site = website.Website(args)
        targets.append(site)

    start_date = max([args.earliest_date,
                      bulletin_date - datetime.timedelta(days=31)])
    date_range = list(
        util.make_date_range(start_date, bulletin_date)
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
        query = sql.select([sqlfn.max(table.c.bulletin_date)])
        result = connection.execute(query)
        return result.fetchone()[0]