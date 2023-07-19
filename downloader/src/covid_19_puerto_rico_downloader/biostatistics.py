import argparse
import concurrent.futures as futures
import datetime
import logging
from threading import Lock

from . import task
from . import util


def process_arguments():
    parser = argparse.ArgumentParser(description='Download PRDoH Biostatistics COVID-19 data sets')
    parser.add_argument('--s3-sync-dir', type=str, required=True,
                        help='Override for directory to which to deposit the output files for sync')
    parser.add_argument('--endpoint-url', type=str, default='https://biostatistics.salud.pr.gov',
                        help='Override for the URL of the Biostatistics API endpoint root.')
    parser.add_argument('--bzip2-command', type=str, default='lbzip2',
                        help='Override the command used to do bzip2 compression. Default: `lbzip2`.')
    parser.add_argument('--duckdb-file', type=str, default='Biostatistics.duckdb',
                        help='Override name of the DuckDB database file. Default: `Biostatistics.duckdb`.')
    return parser.parse_args()

DATASETS = {
    # Put larger ones first
    "tests": "orders/tests/covid-19/minimal",
    "cases": "cases/covid-19/minimal",
    "persons-with-vaccination-status": "vaccines/covid-19/persons-with-vaccination-status",
    "tests-grouped": "orders/tests/covid-19/grouped-by-sample-collected-date-and-entity",
    "deaths": "deaths/covid-19/minimal",
    "data-sources": "data-sources",
}

def biostatistics():
    """Entry point for PRDoH Biostatistics download code."""
    logging.basicConfig(
        format='%(asctime)s %(threadName)s %(message)s',
        level=logging.INFO)
    util.log_platform()
    args = process_arguments()

    config = task.TaskConfig(
        now=pick_and_log_now(),
        extension='json',
        http=util.make_requests_session('application/json'),
        duck=util.make_duckdb_connection(args.duckdb_file),
        jinja=util.make_jinja('biostatistics'),
        # We don't want to cause a big spike on the Biostatistics servers
        # by going off and downloading all their datasets in parallel.  So
        # we use this mutex to make sure only one download goes at a time.
        mutex=Lock(),
        endpoint_url=args.endpoint_url,
        s3_sync_dir=args.s3_sync_dir,
        endpoint_dir_name='biostatistics.salud.pr.gov',
        input_dir_name='json_v1',
        parquet_dir_name='parquet_v2',
        ts_format='%Y-%m-%dT%H:%M:%SZ',
        bzip2_command=args.bzip2_command,
    )

    tasks = [
        task.Task(dataset, path, config)
        for (dataset, path) in DATASETS.items()
    ]
    task.run_tasks(tasks)

def pick_and_log_now():
    now = datetime.datetime.utcnow()
    logging.info('Now = %s', now.isoformat())
    return now