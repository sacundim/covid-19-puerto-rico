import argparse
import datetime
import logging
import os
import os.path
import shutil
from sodapy import Socrata
from threading import Lock

from . import task
from . import util


def process_arguments():
    parser = argparse.ArgumentParser(description='Download HHS COVID-19 data sets')
    parser.add_argument('--socrata-app-token', type=str,
                        help='Socrata API App Token. '
                             'Not required but we get throttled without it. '
                             'This parameter takes precedence over --socrata-app-token-env-var.')
    parser.add_argument('--socrata-app-token-env-var', type=str,
                        help='Environment variable from which to get Socrata API App Token. '
                             'Not required but we get throttled without it. '
                             'The --socrata-app-token parameter takes precedence over this one.')

    parser.add_argument('--s3-sync-dir', type=str, required=False,
                        help='Override for directory to which to deposit the output files for sync')
    parser.add_argument('--rclone-destination', type=str, required=False,
                        help='If given, the `--s3-sync-dir` will be copied over to that destination with `rclone`.')

    parser.add_argument('--duckdb-file', type=str, default='Walgreens.duckdb',
                        help='Override name of the DuckDB database file. Default: `Walgreens.duckdb`.')
    parser.add_argument('--bzip2-command', type=str, default='lbzip2',
                        help='Override the command used to do bzip2 compression. Default: `lbzip2`.')
    parser.add_argument('--rclone-command', type=str, default='rclone',
                        help='Override the path to the rclone command. Default: `rclone`.')

    return parser.parse_args()


def hhs_download():
    """Entry point for HHS download code."""
    logging.basicConfig(format='%(asctime)s %(threadName)s %(message)s', level=logging.INFO)
    util.log_platform()
    args = process_arguments()

    config = task.TaskConfig(
        now=pick_and_log_now(),
        extension='csv',
        http=util.make_requests_session('application/csv'),
        duck=util.make_duckdb_connection(args.duckdb_file),
        jinja=util.make_jinja('hhs'),
        # We don't want to cause a big spike on the Covid19Datos servers
        # by going off and downloading all their datasets in parallel.  So
        # we use this mutex to make sure only one download goes at a time.
        mutex=Lock(),
        endpoint_url=None,
        s3_sync_dir=args.s3_sync_dir,
        endpoint_dir_name='HHS',
        input_dir_name='v4/csv',
        parquet_dir_name='v4/parquet',
        ts_format='%Y%m%d_%H%M',
        bzip2_command=args.bzip2_command,
    )

    socrata_token = get_socrata_app_token(args)
    with Socrata('healthdata.gov', socrata_token, timeout=60) as hhs:
        task.run_tasks(healthdata_tasks(hhs, config))

    if args.s3_sync_dir and args.rclone_destination:
        task.rclone(
            args.s3_sync_dir,
            args.rclone_destination,
            args.rclone_command)



def healthdata_tasks(client, config):
    """Datasets hosted at healthdata.gov API endpoints"""
    datasets = {
        # Put larger ones earlier
        'covid-19_diagnostic_lab_testing': 'j8mb-icvb',
    }
    return [
        HHSTask(dataset, id, client, config)
        for (dataset, id) in datasets.items()
    ]


class HHSTask(task.Task):
    def __init__(self, dataset, id, client, config):
        super().__init__(dataset, None, config)
        self.id = id
        self.client = client

    def __call__(self):
        metadata = self.client.get_metadata(self.id)
        file_timestamp = datetime.datetime.utcfromtimestamp(metadata['rowsUpdatedAt'])

        inputfile = self.download(file_timestamp)
        parquetfile = self.convert(inputfile, file_timestamp)
        bzip2file = self.compress(inputfile)
        self.move_to_sync_dir(bzip2file, parquetfile)
        return self.dataset

    def download(self, file_timestamp):
        with self.mutex:
            url = f'https://{self.client.domain}/api/views/{self.id}/rows.csv?accessType=DOWNLOAD'

            # CODE SMELL: Is the `session` attribute in the client morally private?
            r = self.client.session.get(url, stream=True)

            outpath = f'{self.dataset}_{file_timestamp.strftime("%Y%m%d_%H%M")}.csv'
            with open(outpath, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fd.write(chunk)
            return outpath

    def convert(self, inputfile, file_timestamp):
        logging.info("Converting %s to Parquet...", self.dataset)
        parquetfile = f'{self.dataset}_{file_timestamp.strftime(self.ts_format)}.parquet'

        template = self.jinja.get_template(f'{self.dataset}.sql.j2')
        sql = template.render(
            input_file=inputfile,
            output_parquet=parquetfile,
            file_timestamp=file_timestamp.isoformat(),
            downloaded_at=self.now.isoformat()
        )

        with self.duck.cursor() as c:
            c.execute(sql)

        logging.info("Converted %s to Parquet.", self.dataset)
        return parquetfile

    def move_to_parquet_dir(self, parquetfile, dataset_dir):
        parquet_dir = dataset_dir / self.parquet_dir_name
        parquet_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(parquetfile, parquet_dir)
        logging.info("Moved %s to %s...", parquetfile, parquet_dir)




def pick_and_log_now():
    now = datetime.datetime.utcnow()
    logging.info('Now = %s', now.isoformat())
    return now

def get_socrata_app_token(args):
    if args.socrata_app_token:
        logging.info("Using Socrata App Token from command line")
        return args.socrata_app_token
    elif args.socrata_app_token_env_var:
        env_var = args.socrata_app_token_env_var
        logging.info("Using Socrata App Token from environment variable %s", env_var)
        try:
            return os.environ[env_var]
        except e:
            logging.error('Environment variable %s not set', env_var)
            raise e
    else:
        logging.warning("No Socrata App Token. The API may throttle us.")
        return None
