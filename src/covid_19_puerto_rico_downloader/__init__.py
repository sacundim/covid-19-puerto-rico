import argparse
from csv2parquet import csv2parquet
import datetime
import json
import logging
import os.path
import pathlib
import requests
import shutil
from sodapy import Socrata
import subprocess


def hhs_downloader():
    """Entry point for HHS download code."""
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser(description='Download HHS COVID-19 data sets')
    parser.add_argument('--s3-sync-dir', type=str, required=True,
                        help='Directory to which to deposit the output files for sync')
    args = parser.parse_args()

    datasets = [
        Asset('reported_hospital_utilization', '6xf2-c3ie'),
        Asset('reported_hospital_utilization_timeseries', 'g62h-syeh'),
        Asset('reported_hospital_capacity_admissions_facility_level_weekly_average_timeseries', 'anag-cw7u'),
        Asset('estimated_icu', '7ctx-gtb7'),
        Asset('estimated_inpatient_all', 'jjp9-htie'),
        Asset('estimated_inpatient_covid', 'py8k-j5rq'),
        Asset('covid-19_diagnostic_lab_testing', 'j8mb-icvb'),
    ]

    with Socrata('beta.healthdata.gov', None, timeout=60) as client:
        for dataset in datasets:
            logging.info('Fetching %s...', dataset.name)
            csv_file = dataset.get_csv(client)

            logging.info('Dowloaded %s. Converting to Parquet...', csv_file)
            basename, extension = os.path.splitext(csv_file)
            parquet_file = basename + '.parquet'
            csv2parquet.main_with_args(csv2parquet.convert, [
                '--codec', 'gzip',
                '--row-group-size', '10000000',
                '--output', parquet_file,
                csv_file
            ])

            logging.info('Generated Parquet. Compressing %s...', csv_file)
            subprocess.run(['bzip2', '-f', '-9', csv_file])
            s3_sync_dir = pathlib.Path(args.s3_sync_dir)
            s3_sync_dir.mkdir(exist_ok=True)
            hhs_sync_dir = pathlib.Path(f'{s3_sync_dir}/HHS')
            hhs_sync_dir.mkdir(exist_ok=True)

            logging.info('Copying files to target...')
            dataset_dir = pathlib.Path(f'{hhs_sync_dir}/{dataset.name}/v2')
            dataset_dir.mkdir(exist_ok=True, parents=True)
            csv_dir = pathlib.Path(f'{dataset_dir}/csv')
            csv_dir.mkdir(exist_ok=True)
            parquet_dir = pathlib.Path(f'{dataset_dir}/parquet')
            parquet_dir.mkdir(exist_ok=True)
            shutil.move(f'{csv_file}.bz2', f'{csv_dir}/{csv_file}.bz2')
            shutil.move(parquet_file, f'{parquet_dir}/{parquet_file}')

        logging.info('All done!')

class Asset():
    """A dataset in a Socrata server, and methods to work with it"""
    def __init__(self, name, id):
        self.name = name
        self.id = id

    def get_metadata(self, client):
        return client.get_metadata(self.id)

    def get_csv(self, client):
        metadata = self.get_metadata(client)
        updated_at = datetime.datetime.utcfromtimestamp(metadata['rowsUpdatedAt'])
        url = f'https://{client.domain}/api/views/{self.id}/rows.csv?accessType=DOWNLOAD'
        r = requests.get(url)
        outpath = f'{self.name}_{updated_at.strftime("%Y%m%d_%H%M")}.csv'
        with open(outpath, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        return outpath


