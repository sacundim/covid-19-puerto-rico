import argparse
import datetime
import logging
from threading import Lock

from . import task
from . import util

ENDPOINT = 'https://labvegas.com/data/covid19/walgreens/'

def process_arguments():
    parser = argparse.ArgumentParser(description='Download Andy Bloch Walgreens dataset')
    parser.add_argument('--s3-sync-dir', type=str, required=True,
                        help='Override for directory to which to deposit the output files for sync')
    parser.add_argument('--endpoint-url', type=str, default=ENDPOINT,
                        help='Override for the URL of the API endpoint root.')
    parser.add_argument('--bzip2-command', type=str, default='lbzip2',
                        help='Override the command used to do bzip2 compression. Default: `lbzip2`.')
    parser.add_argument('--duckdb-file', type=str, default='Walgreens.duckdb',
                        help='Override name of the DuckDB database file. Default: `Walgreens.duckdb`.')
    return parser.parse_args()


def walgreens():
    logging.basicConfig(
        format='%(asctime)s %(threadName)s %(message)s',
        level=logging.INFO)
    args = process_arguments()

    config = task.TaskConfig(
        now=pick_and_log_now(),
        extension='csv',
        http=util.make_requests_session('application/csv'),
        duck=util.make_duckdb_connection(args.duckdb_file),
        jinja=util.make_jinja('walgreens'),
        # We don't want to cause a big spike on the Covid19Datos servers
        # by going off and downloading all their datasets in parallel.  So
        # we use this mutex to make sure only one download goes at a time.
        mutex=Lock(),
        endpoint_url=args.endpoint_url,
        s3_sync_dir=args.s3_sync_dir,
        endpoint_dir_name='Walgreens',
        input_dir_name='csv_v1',
        parquet_dir_name='parquet_v2',
        ts_format='%Y-%m-%dT%H:%M:%SZ'
    )

    dataset = 'Tracker_Aggregation'
    the_task = task.Task(dataset, f'dashboard-Tracker_Aggregation.csv', config)
    the_task()
    logging.info("Completed")


def pick_and_log_now():
    now = datetime.datetime.utcnow()
    logging.info('Now = %s', now.isoformat())
    return now