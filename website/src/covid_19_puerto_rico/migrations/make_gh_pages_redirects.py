#!/usr/bin/env python3
#
# Generate redirect pages from our old Github gh-pages website to
# our new covid-19-puerto-rico.org domain.
#
import argparse
import datetime
from jinja2 import Environment, PackageLoader, select_autoescape
import logging
import pathlib
from covid_19_puerto_rico.util import make_date_range

def process_arguments():
    parser = argparse.ArgumentParser(description='Extract historical contents of `gh-pages` branch')
    parser.add_argument('--output-dir', type=str, required=True,
                        help='Directory into which to place output')
    parser.add_argument('--start-date', type=datetime.date.fromisoformat,
                        default=datetime.date(2020, 4, 25),
                        help='Earliest date to generate website for. Has a sensible built-in default.')
    parser.add_argument('--end-date', type=datetime.date.fromisoformat,
                        default=datetime.date(2022, 8, 5),
                        help='Latest date to generate website for. Has a sensible built-in default.')
    return parser.parse_args()


def migrate_off_gh_pages():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    jinja = Environment(
        loader=PackageLoader('covid_19_puerto_rico', 'migrations'),
        autoescape=select_autoescape(['html', 'xml'])
    )
    args = process_arguments()
    root_directory = pathlib.Path(args.output_dir)
    root_directory.mkdir(exist_ok=True)
    for bulletin_date in make_date_range(args.start_date, args.end_date):
        render_subdirectory(bulletin_date, root_directory, jinja)
    render_index_html('index.html', f'{root_directory}/index.html', jinja)


def render_subdirectory(bulletin_date, root_directory, jinja):
    output_directory = root_directory.joinpath(f'{bulletin_date}')
    output_directory.mkdir(exist_ok=True)
    output_index_html = f'{output_directory}/index.html'
    render_index_html(f'{bulletin_date}/index.html', output_index_html, jinja)


def render_index_html(path, destination, jinja):
    logging.info("Rendering %s to %s", path, destination)
    template = jinja.get_template('redirect_index.html')
    template.stream(path=path)\
        .dump(destination)
