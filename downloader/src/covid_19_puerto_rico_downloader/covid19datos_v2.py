import argparse
import concurrent.futures as futures
import datetime
import logging
from threading import Lock

from . import task
from . import util


def process_arguments():
    parser = argparse.ArgumentParser(description='Download PRDoH Covid19Datos V2 data sets')
    parser.add_argument('--s3-sync-dir', type=str, required=True,
                        help='Override for directory to which to deposit the output files for sync')
    parser.add_argument('--endpoint-url', type=str, default=ENDPOINT,
                        help='Override for the URL of the Covid19Datos V2 API endpoint root.')
    parser.add_argument('--bzip2-command', type=str, default='lbzip2',
                        help='Override the command used to do bzip2 compression. Default: `lbzip2`.')
    parser.add_argument('--duckdb-file', type=str, default='Covid19Datos-V2.duckdb',
                        help='Override name of the DuckDB database file. Default: `Covid19Datos-V2.duckdb`.')
    return parser.parse_args()

ENDPOINT='https://covid19datos.salud.pr.gov/estadisticas_v2/download/data'

DATASETS = [
    # Put larger ones first
    'pruebas',
    'casos',
    'vacunacion',
    'defunciones',
    'sistemas_salud',
    'vigilancia',
]


def covid19datos_v2():
    """Entry point for PRDoH Covid19Datos V2 download code."""
    logging.basicConfig(
        format='%(asctime)s %(threadName)s %(message)s',
        level=logging.INFO)
    util.log_platform()
    args = process_arguments()

    config = task.TaskConfig(
        now=pick_and_log_now(),
        extension='csv',
        http=util.make_requests_session('application/csv'),
        duck=util.make_duckdb_connection(args.duckdb_file),
        jinja=util.make_jinja('covid19datos-v2'),
        # We don't want to cause a big spike on the Covid19Datos servers
        # by going off and downloading all their datasets in parallel.  So
        # we use this mutex to make sure only one download goes at a time.
        mutex=Lock(),
        endpoint_url=args.endpoint_url,
        s3_sync_dir=args.s3_sync_dir,
        endpoint_dir_name='covid19datos-v2',
        input_dir_name='csv_v3',
        parquet_dir_name='parquet_v4',
        ts_format='%Y-%m-%dT%H:%M:%SZ',
        bzip2_command=args.bzip2_command,
    )

    tasks = [
        task.Task(dataset, f'{dataset}/completo', config)
        for dataset in DATASETS
    ]
    task.run_tasks(tasks)

def pick_and_log_now():
    now = datetime.datetime.utcnow()
    logging.info('Now = %s', now.isoformat())
    return now