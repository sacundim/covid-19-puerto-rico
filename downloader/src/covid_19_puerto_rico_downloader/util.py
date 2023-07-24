import duckdb
from jinja2 import Environment, PackageLoader
import logging
import platform
import requests

def make_duckdb_connection(filename):
    return duckdb.connect(filename, config={})

def make_requests_session(accept):
    session = requests.Session()
    session.headers.update({
        'accept': accept,
        'Accept-Encoding': 'gzip'
    })
    return session

def make_jinja(package):
    return Environment(
        loader=PackageLoader('covid_19_puerto_rico_downloader.templates', package),
#        autoescape=select_autoescape(['html', 'xml'])
    )

def log_platform(level=logging.INFO):
    """Log `platform.uname()` results, to confirm what CPU arch we're using."""
    uname = platform.uname()
    logging.log(level, "Platform: system=%s, machine=%s, version=%s",
                uname.system, uname.machine, uname.version)
