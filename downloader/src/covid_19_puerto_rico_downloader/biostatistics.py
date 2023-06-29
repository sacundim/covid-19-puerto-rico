import argparse
import concurrent.futures as futures
import datetime
import duckdb
from jinja2 import Environment, PackageLoader
import logging
import pathlib
import requests
import shutil
import subprocess
from threading import Lock



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
    "tests": "orders/tests/covid-19/minimal",
    "data-sources": "data-sources",
    "cases": "cases/covid-19/minimal",
    "deaths": "deaths/covid-19/minimal",
    "tests-grouped": "orders/tests/covid-19/grouped-by-sample-collected-date-and-entity",
    "persons-with-vaccination-status": "vaccines/covid-19/persons-with-vaccination-status",
}

def biostatistics():
    """Entry point for PRDoH Biostatistics download code."""
    logging.basicConfig(
        format='%(asctime)s %(threadName)s %(message)s',
        level=logging.INFO)
    args = process_arguments()

    now = datetime.datetime.utcnow()
    logging.info('Now = %s', now.isoformat())

    # We don't want to cause a big spike on the Biostatistics servers
    # by going off and downloading all their datasets in parallel.  So
    # we use this mutex to make sure only one
    mutex = Lock()

    http = make_requests_session()
    duck = make_duckdb_connection(args.duckdb_file)
    jinja = make_jinja()
    tasks = [
        Task(dataset, path, now, http, duck, jinja, mutex, args)
        for (dataset, path) in DATASETS.items()
    ]
    with futures.ThreadPoolExecutor(
            max_workers=len(tasks),
            thread_name_prefix='download_task') as executor:
        for future in futures.as_completed([executor.submit(task) for task in tasks]):
            logging.info("Completed %s", future.result())


def make_duckdb_connection(filename):
    return duckdb.connect(filename, config={})

def make_requests_session():
    session = requests.Session()
    session.headers.update({
        'accept': 'application/json',
        'Accept-Encoding': 'gzip'
    })
    return session

def make_jinja():
    return Environment(
        loader=PackageLoader('covid_19_puerto_rico_downloader', 'templates'),
#        autoescape=select_autoescape(['html', 'xml'])
    )

class Task():
    TS_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

    def __init__(self, dataset, path, now, http, duck, jinja, mutex, args):
        self.dataset = dataset
        self.path = path
        self.now = now
        self.http = http
        self.duck = duck
        self.jinja = jinja
        self.mutex = mutex
        self.endpoint_url = args.endpoint_url
        self.s3_sync_dir = pathlib.Path(args.s3_sync_dir)


    def __call__(self):
        jsonfile = self.download()
        parquetfile = self.convert(jsonfile)
        bzip2file = self.compress(jsonfile)
        self.move_to_sync_dir(bzip2file, parquetfile)
        return self.dataset

    def download(self):
        url = f'{self.endpoint_url}/{self.path}'
        with self.mutex:
            logging.info("Downloading %s from %s...", self.dataset, url)
            jsonfile = f'{self.dataset}_{self.now.strftime(Task.TS_FORMAT)}.json'

            request = self.http.get(url, stream=True)
            with open(jsonfile, 'wb') as fd:
                for chunk in request.iter_content(chunk_size=1024 * 1024):
                    fd.write(chunk)

            logging.info("Downloaded %s", jsonfile)
            return jsonfile

    def convert(self, jsonfile):
        logging.info("Converting %s to Parquet...", self.dataset)
        parquetfile = f'{self.dataset}_{self.now.strftime(Task.TS_FORMAT)}.parquet'

        template = self.jinja.get_template(f'{self.dataset}.sql.j2')
        sql = template.render(
            input_json=jsonfile,
            output_parquet=parquetfile,
            downloaded_at=self.now.isoformat()
        )

        with self.duck.cursor() as c:
            c.execute(sql)

        logging.info("Converted %s to Parquet.", self.dataset)
        return parquetfile

    def compress(self, jsonfile):
        logging.info("Compressing %s to bzip2 format...", jsonfile)
        subprocess.run(['lbzip2', '-f', '-9', jsonfile])
        logging.info("Compressed %s to bzip2 format.", jsonfile)
        return f'{jsonfile}.bz2'

    def move_to_sync_dir(self, jsonfile, parquetfile):
        logging.info("Moving files to sync dir %s...", self.s3_sync_dir)
        self.s3_sync_dir.mkdir(exist_ok=True)
        biostatistics_dir = self.s3_sync_dir / 'biostatistics.salud.pr.gov'
        biostatistics_dir.mkdir(exist_ok=True)
        dataset_dir = biostatistics_dir / self.dataset
        dataset_dir.mkdir(exist_ok=True)

        json_dir = dataset_dir / 'json_v1'
        json_dir.mkdir(exist_ok=True)
        shutil.move(jsonfile, json_dir)
        logging.info("Moved %s to %s...", jsonfile, json_dir)

        parquet_dir = dataset_dir / 'parquet_v2'
        parquet_dir.mkdir(exist_ok=True)
        partition_dir = parquet_dir / f'downloaded_date={self.now.strftime("%Y-%m-%d")}'
        partition_dir.mkdir(exist_ok=True)
        shutil.move(parquetfile, partition_dir)
        logging.info("Moved %s to %s...", parquetfile, partition_dir)
