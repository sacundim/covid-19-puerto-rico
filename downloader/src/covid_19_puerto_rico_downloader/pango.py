import argparse
import datetime
import logging
import pathlib
import shutil

from . import task
from . import util

def process_arguments():
    parser = argparse.ArgumentParser(description='Download PANGO lineage dataset')

    parser.add_argument('--s3-sync-dir', type=str, required=False,
                        help='Override for directory to which to deposit the output files for sync')
    parser.add_argument('--rclone-destination', type=str, default=None,
                        help='If given, the `--s3-sync-dir` will be copied over to that destination with `rclone`.')

    parser.add_argument('--endpoint-url', type=str, default=ENDPOINT,
                        help='Override for the URL of the Covid19Datos V2 API endpoint root.')
    parser.add_argument('--duckdb-file', type=str, default='Covid19Datos-V2.duckdb',
                        help='Override name of the DuckDB database file. Default: `Covid19Datos-V2.duckdb`.')
    parser.add_argument('--rclone-command', type=str, default='rclone',
                        help='Override the path to the rclone command. Default: `rclone`.')

    return parser.parse_args()

ENDPOINT = 'https://raw.githubusercontent.com/cov-lineages/pango-designation/master'

def pango_lineages():
    """Entry point for PANGO lineages download code."""
    logging.basicConfig(
        format='%(asctime)s %(threadName)s %(message)s',
        level=logging.INFO)
    util.log_platform()
    args = process_arguments()

    now = pick_and_log_now()
    parquetfile = download_and_convert(args, now)

    if args.s3_sync_dir:
        move_to_sync_dir(args, parquetfile, now)

        if args.rclone_destination:
            task.rclone(
                args.s3_sync_dir,
                args.rclone_destination,
                args.rclone_command)

def download_and_convert(args, now):
    duck = util.make_duckdb_connection(
        args.duckdb_file,
        init=[
            'INSTALL httpfs',
            'LOAD httpfs'
        ]
    )
    jinja = util.make_jinja('pango')
    ts_format = '%Y-%m-%dT%H:%M:%SZ'
    parquetfile = f'lineages_{now.strftime(ts_format)}.parquet'

    template = jinja.get_template('lineages.sql.j2')
    sql = template.render(
        endpoint=args.endpoint_url,
        output_parquet=parquetfile,
        downloaded_at=now.isoformat()
    )
    with duck.cursor() as c:
        c.execute(sql)

    return parquetfile

def move_to_sync_dir(args, parquetfile, now):
    logging.info("Moving files to sync dir %s...", args.s3_sync_dir)
    s3_sync_dir = pathlib.Path(args.s3_sync_dir)
    s3_sync_dir.mkdir(exist_ok=True)
    endpoint_dir = s3_sync_dir / 'pango'
    endpoint_dir.mkdir(exist_ok=True)
    dataset_dir = endpoint_dir / 'lineages'
    dataset_dir.mkdir(exist_ok=True)

    parquet_dir = dataset_dir / 'parquet_v1'
    parquet_dir.mkdir(parents=True, exist_ok=True)
    partition_dir = parquet_dir / f'downloaded_date={now.strftime("%Y-%m-%d")}'
    partition_dir.mkdir(exist_ok=True)
    shutil.move(parquetfile, partition_dir)
    logging.info("Moved %s to %s...", parquetfile, partition_dir)


def pick_and_log_now():
    now = datetime.datetime.utcnow()
    logging.info('Now = %s', now.isoformat())
    return now