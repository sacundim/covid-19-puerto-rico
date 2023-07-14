import datetime
import logging
from jinja2 import Environment, PackageLoader, select_autoescape
import os
import pathlib
from PIL import Image
import shutil


class Website:
    def __init__(self, args):
        self.build_assets = args.build_assets
        self.assets_dir = args.assets_dir
        self.output_dir = args.output_dir
        self.jinja = Environment(
            loader=PackageLoader('covid_19_puerto_rico', 'templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )

    def render(self, date_range):
        for bulletin_date in date_range:
            self.render_bulletin_date(bulletin_date, date_range)
        if self.build_assets and self.assets_dir:
            self.copy_assets()
        else:
            logging.info("Skipping assets copy")

    def copy_assets(self):
        for directory, subdirs, filenames in os.walk(self.assets_dir):
            relative = pathlib.Path(directory).relative_to(self.assets_dir)
            output_directory = pathlib.Path(f'{self.output_dir}/{relative}')
            output_directory.mkdir(exist_ok=True)
            logging.info("Copying files from %s/ to %s/", directory, output_directory)
            for filename in filenames:
                logging.debug("Copying %s from %s/ to %s/", filename, directory, output_directory)
                basename, extension = os.path.splitext(filename)
                if extension == '.png':
                    logging.warning("Converting %s to jpg", filename)
                    copy_to_jpg(f'{directory}/{filename}',
                                f'{output_directory}/{basename}.jpg')
                elif extension == '.jpeg':
                    shutil.copyfile(f'{directory}/{filename}',
                                    f'{output_directory}/{basename}.jpg')
                else:
                    shutil.copyfile(f'{directory}/{filename}',
                                    f'{output_directory}/{filename}')

    def render_bulletin_date(self, bulletin_date, date_range):
        output_index_html = f'{self.output_dir}/{bulletin_date}/index.html'
        logging.info("Rendering %s", output_index_html)
        previous_date = bulletin_date - datetime.timedelta(days=1)
        next_date = bulletin_date + datetime.timedelta(days=1)
        template = self.jinja.get_template('bulletin_date_index.html')
        template.stream(
            bulletin_dates=sorted(date_range, reverse=True),
            bulletin_date=bulletin_date,
            previous_date=previous_date,
            bulletin_month=bulletin_date.strftime('%Y-%m'),
            previous_date_month=previous_date.strftime('%Y-%m'),
            next_date=next_date)\
            .dump(output_index_html)

    def render_molecular_tests_page(self, date_range):
        molecular_dir = pathlib.Path(f'{self.output_dir}/molecular_tests')
        molecular_dir.mkdir(exist_ok=True)
        molecular_index_html = f'{molecular_dir}/index.html'
        logging.info("Rendering %s", molecular_index_html)
        template = self.jinja.get_template('molecular_tests_index.html')
        template.stream(
            bulletin_dates=sorted(date_range, reverse=True),
            bulletin_date=max(date_range))\
            .dump(molecular_index_html)

    def render_top(self, bulletin_date):
        output_index_html = f'{self.output_dir}/index.html'
        logging.info("Rendering %s", output_index_html)
        template = self.jinja.get_template('top_index.html')
        template.stream(bulletin_date=bulletin_date)\
            .dump(output_index_html)


def copy_to_jpg(origin, destination):
    with Image.open(origin) as image:
        if image.mode in ('RGBA', 'P'):
            image = image.convert('RGB')
        image.save(destination)
