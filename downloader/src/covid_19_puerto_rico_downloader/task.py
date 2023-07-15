import collections
import concurrent.futures as futures
import logging
import pathlib
import shutil
import subprocess


TaskConfig = collections.namedtuple('TaskConfig', [
    'now', 'extension', 'http', 'duck', 'jinja', 'mutex',
    'endpoint_url', 's3_sync_dir', 'endpoint_dir_name', 'input_dir_name', 'parquet_dir_name',
    'ts_format'
])


class Task():
    def __init__(self, dataset, path, config):
        self.dataset = dataset
        self.path = path
        self.now = config.now
        self.extension = config.extension
        self.http = config.http
        self.duck = config.duck
        self.jinja = config.jinja
        self.mutex = config.mutex
        self.endpoint_url = config.endpoint_url
        self.s3_sync_dir = pathlib.Path(config.s3_sync_dir)
        self.input_dir_name = config.input_dir_name
        self.endpoint_dir_name = config.endpoint_dir_name
        self.parquet_dir_name = config.parquet_dir_name
        self.ts_format = config.ts_format


    def __call__(self):
        inputfile = self.download()
        parquetfile = self.convert(inputfile)
        bzip2file = self.compress(inputfile)
        self.move_to_sync_dir(bzip2file, parquetfile)
        return self.dataset

    def download(self):
        url = f'{self.endpoint_url}/{self.path}'
        with self.mutex:
            logging.info("Downloading %s from %s...", self.dataset, url)
            inputfile = f'{self.dataset}_{self.now.strftime(self.ts_format)}.{self.extension}'

            request = self.http.get(url, stream=True)
            with open(inputfile, 'wb') as fd:
                for chunk in request.iter_content(chunk_size=1024 * 1024):
                    fd.write(chunk)

            logging.info("Downloaded %s", inputfile)
            return inputfile

    def convert(self, inputfile):
        logging.info("Converting %s to Parquet...", self.dataset)
        parquetfile = f'{self.dataset}_{self.now.strftime(self.ts_format)}.parquet'

        template = self.jinja.get_template(f'{self.dataset}.sql.j2')
        sql = template.render(
            input_file=inputfile,
            output_parquet=parquetfile,
            downloaded_at=self.now.isoformat()
        )

        with self.duck.cursor() as c:
            c.execute(sql)

        logging.info("Converted %s to Parquet.", self.dataset)
        return parquetfile

    def compress(self, inputfile):
        logging.info("Compressing %s to bzip2 format...", inputfile)
        subprocess.run(['lbzip2', '-f', '-9', inputfile])
        logging.info("Compressed %s to bzip2 format.", inputfile)
        return f'{inputfile}.bz2'

    def move_to_sync_dir(self, inputfile, parquetfile):
        logging.info("Moving files to sync dir %s...", self.s3_sync_dir)
        self.s3_sync_dir.mkdir(exist_ok=True)
        endpoint_dir = self.s3_sync_dir / self.endpoint_dir_name
        endpoint_dir.mkdir(exist_ok=True)
        dataset_dir = endpoint_dir / self.dataset
        dataset_dir.mkdir(exist_ok=True)

        self.move_to_input_dir(inputfile, dataset_dir)
        self.move_to_parquet_dir(parquetfile, dataset_dir)


    def move_to_input_dir(self, inputfile, dataset_dir):
        input_dir = dataset_dir / self.input_dir_name
        input_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(inputfile, input_dir)
        logging.info("Moved %s to %s...", inputfile, input_dir)
        return input_dir

    def move_to_parquet_dir(self, parquetfile, dataset_dir):
        parquet_dir = dataset_dir / self.parquet_dir_name
        parquet_dir.mkdir(parents=True, exist_ok=True)
        partition_dir = parquet_dir / f'downloaded_date={self.now.strftime("%Y-%m-%d")}'
        partition_dir.mkdir(exist_ok=True)
        shutil.move(parquetfile, partition_dir)
        logging.info("Moved %s to %s...", parquetfile, partition_dir)


def run_tasks(tasks, thread_name_prefix='download_task'):
    """Run a collection of Tasks in parallel.  Carefully crafted to not swallow exceptions"""
    with futures.ThreadPoolExecutor(
            max_workers=len(tasks),
            thread_name_prefix=thread_name_prefix) as executor:
        for future in futures.as_completed([executor.submit(task) for task in tasks]):
            logging.info("Completed %s", future.result())
