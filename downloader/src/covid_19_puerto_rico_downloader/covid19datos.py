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

def covid19datos():
    """Entry point for PRDoH dashboard download code."""
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    args = process_arguments()
    Covid19Datos(args).run()


class Covid19Datos():
    """Downloader for Puerto Rico Department of Health dashboard"""
    def __init__(self, args):
        self.args = args
        self.s3_sync_dir = pathlib.Path(args.s3_sync_dir)
        self.dash_sync_dir = pathlib.Path(f'{self.s3_sync_dir}/covid19datos.salud.gov.pr')

    URLS = {
        'casos': 'https://covid19datos.salud.gov.pr/estadisticas/casos',
        'defunc': 'https://covid19datos.salud.gov.pr/estadisticas/defunc',
        'vacunaciones': 'https://covid19datos.salud.gov.pr/estadisticas/vacunaciones',
        'hospitales': 'https://covid19datos.salud.gov.pr/estadisticas/hospitales'
    }

    MUNICIPIOS_URL = 'https://covid19datos.salud.gov.pr/estadisticas/vacunaciones/municipio'


    def run(self):
        self.make_directory_structure()
        now = datetime.now(tz=timezone('America/Puerto_Rico'))
        logging.info('Now = %s', now.isoformat())
        self.download_urls(now)
        self.download_municipios(now)
        logging.info('Downloads all done!')

    def make_directory_structure(self):
        """Ensure all of the required directories exist."""
        self.s3_sync_dir.mkdir(exist_ok=True)
        self.dash_sync_dir.mkdir(exist_ok=True)

    def make_destination_dir(self, key):
        destination = pathlib.Path(f'{self.dash_sync_dir}/{key}')
        destination.mkdir(exist_ok=True)
        return destination


    def download_urls(self, now):
        for key, url in self.URLS.items():
            jsonfile = self.download_url(key, url, now)
            destination = self.make_destination_dir(key)
            logging.info('Moving %s to %s/', jsonfile, destination)
            shutil.move(jsonfile, f'{destination}/{jsonfile}')

    def download_url(self, key, url, now):
        logging.info('Downloading %s from %s', key, url)
        r = requests.post(url)
        outpath = f'{key}_{now.isoformat()}.json'
        with open(outpath, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        return outpath


    def download_municipios(self, now):
        logging.info('Downloading municipios vaccination files...')
        jsonfiles = []
        for municipio in MUNICIPIOS:
            jsonfiles.append(self.download_municipio(municipio, now))

        destination = self.make_destination_dir('vacunaciones-municipios')
        logging.info('Moving files to %s/', destination)
        for jsonfile in jsonfiles:
            shutil.move(jsonfile, f'{destination}/{jsonfile}')

    def download_municipio(self, municipio, now):
        url = f'{self.MUNICIPIOS_URL}/{municipio}'
        logging.info('Downloading %s from %s', municipio, url)
        r = requests.post(url)
        outpath = f'vacunaciones-{municipio}_{now.isoformat()}.json'
        with open(outpath, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        return outpath


MUNICIPIOS = [
    'Adjuntas',
    'Aguada',
    'Aguadilla',
    'Aguas Buenas',
    'Aibonito',
    'Añasco',
    'Arecibo',
    'Arroyo',
    'Barceloneta',
    'Barranquitas',
    'Bayamón',
    'Cabo Rojo',
    'Caguas',
    'Camuy',
    'Canóvanas',
    'Carolina',
    'Cataño',
    'Cayey',
    'Ceiba',
    'Ciales',
    'Cidra',
    'Coamo',
    'Comerío',
    'Corozal',
    'Culebra',
    'Dorado',
    'Fajardo',
    'Florida',
    'Guánica',
    'Guayama',
    'Guayanilla',
    'Guaynabo',
    'Gurabo',
    'Hatillo',
    'Hormigueros',
    'Humacao',
    'Isabela',
    'Jayuya',
    'Juana Díaz',
    'Juncos',
    'Lajas',
    'Lares',
    'Las Marías',
    'Las Piedras',
    'Loíza',
    'Luquillo',
    'Manatí',
    'Maricao',
    'Maunabo',
    'Mayagüez',
    'Moca',
    'Morovis',
    'Naguabo',
    'Naranjito',
    'Orocovis',
    'Patillas',
    'Peñuelas',
    'Ponce',
    'Quebradillas',
    'Rincón',
    # This one doesn't have the accent on the "í" in the website,
    # but luckily the API endpoint is accent-insensitive
    'Río Grande',
    'Sabana Grande',
    'Salinas',
    'San Germán',
    'San Juan',
    'San Lorenzo',
    'San Sebastián',
    'Santa Isabel',
    'Toa Alta',
    'Toa Baja',
    'Trujillo Alto',
    'Utuado',
    'Vega Alta',
    'Vega Baja',
    'Vieques',
    'Villalba',
    'Yabucoa',
    'Yauco'
]