import datetime
import logging
from jinja2 import Environment, PackageLoader, select_autoescape
import os
import pathlib
import shutil
from wand.image import Image
from . import util


class Website:
    def __init__(self, args, date_range):
        self.assets_dir = args.assets_dir
        self.output_dir = args.output_dir
        self.jinja = Environment(
            loader=PackageLoader('covid_19_puerto_rico', 'templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        self.date_range = date_range

    def generate(self, date_range):
        self.copy_assets()
        for bulletin_date in date_range:
            self.render_bulletin_date(bulletin_date)

    def copy_assets(self):
        for directory, subdirs, filenames in os.walk(self.assets_dir):
            relative = pathlib.Path(directory).relative_to(self.assets_dir)
            output_directory = pathlib.Path(f'{self.output_dir}/{relative}')
            output_directory.mkdir(exist_ok=True)
            for filename in filenames:
                logging.info("Copying %s from %s/ to %s/", filename, directory, output_directory)
                basename, extension = os.path.splitext(filename)
                if (extension == '.jpg' or extension == '.jpeg'):
                    logging.info("Converting %s to png", filename)
                    copy_to_png(f'{directory}/{filename}',
                                f'{output_directory}/{basename}.png')
                else:
                    shutil.copyfile(f'{directory}/{filename}',
                                    f'{output_directory}/{filename}')

    def render(self, bulletin_date):
        output_index_html = f'{self.output_dir}/{bulletin_date}/index.html'
        logging.info("Rendering %s", output_index_html)
        previous_date = bulletin_date - datetime.timedelta(days=1)
        template = self.jinja.get_template('bulletin_date_index.html')
        template.stream(
            bulletin_dates=reversed(self.date_range),
            bulletin_date=bulletin_date,
            previous_date=previous_date)\
            .dump(output_index_html)


def copy_to_png(origin, destination):
    with Image(filename=origin) as original:
        with original.convert('png') as converted:
            converted.save(filename=destination)
