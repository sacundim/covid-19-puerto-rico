"""Scrape data from Puerto Rico Department of Health Dashboard.

This is reverse engineered from the `https://covid19datos.salud.gov.pr/`
website."""

import argparse
from datetime import datetime
import json
import logging
import pathlib
from pytz import timezone
import requests
import shutil


def process_arguments():
    parser = argparse.ArgumentParser(description='Download HHS COVID-19 data sets')
    parser.add_argument('--s3-sync-dir', type=str, required=True,
                        help='Directory to which to deposit the output files for sync')
    return parser.parse_args()

URLS = {
    'casos': 'https://covid19datos.salud.gov.pr/estadisticas/casos',
    'defunc': 'https://covid19datos.salud.gov.pr/estadisticas/defunc',
    'vacunaciones': 'https://covid19datos.salud.gov.pr/estadisticas/vacunaciones',
    'hospitales': 'https://covid19datos.salud.gov.pr/estadisticas/hospitales',
    'vacunaciones-municipio': 'https://covid19datos.salud.gov.pr/estadisticas/vacunaciones/municipio'
}

def covid19datos():
    """Entry point for PRDoH dashboard download code."""
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    args = process_arguments()

    now = datetime.now(tz=timezone('America/Puerto_Rico'))
    logging.info('Now = %s', now.isoformat())

    s3_sync_dir = pathlib.Path(args.s3_sync_dir)
    s3_sync_dir.mkdir(exist_ok=True)
    dash_sync_dir = pathlib.Path(f'{s3_sync_dir}/covid19datos.salud.gov.pr')
    dash_sync_dir.mkdir(exist_ok=True)

    for key, url in URLS.items():
        jsonfile = download(key, url, now)
        destination = pathlib.Path(f'{dash_sync_dir}/{key}')
        destination.mkdir(exist_ok=True)

        logging.info('Moving %s to %s/', jsonfile, destination)
        shutil.move(jsonfile, f'{destination}/{jsonfile}')

    logging.info('Downloads all done!')


def download(key, url, now):
    logging.info('Downloading %s from %s', key, url)
    r = requests.post(url)
    outpath = f'{key}_{now.isoformat()}.json'
    with open(outpath, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)
    return outpath
