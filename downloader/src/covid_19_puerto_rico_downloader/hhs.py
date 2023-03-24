import argparse
import datetime
import logging
import os
import os.path
import pathlib
import shutil
from sodapy import Socrata
import subprocess


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
    return parser.parse_args()


def hhs_download():
    """Entry point for HHS download code."""
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    args = process_arguments()
    healthdata_download(args)
    cdc_download(args)

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


def healthdata_download(args):
    '''Download datasets hosted at healthdata.gov API endpoints'''
    datasets = [
        Asset('covid-19_community_profile_report_county', 'di4u-7yu6'),
        Asset('covid-19_diagnostic_lab_testing', 'j8mb-icvb'),
        Asset('estimated_icu', '7ctx-gtb7'),
        Asset('estimated_inpatient_all', 'jjp9-htie'),
        Asset('estimated_inpatient_covid', 'py8k-j5rq'),
        Asset('reported_hospital_utilization', '6xf2-c3ie'),
        Asset('reported_hospital_utilization_timeseries', 'g62h-syeh'),
        Asset('reported_hospital_capacity_admissions_facility_level_weekly_average_timeseries', 'anag-cw7u'),
        Asset('reported_hospital_capacity_admissions_facility_level_weekly_average_timeseries_raw', 'uqq2-txqb'),
    ]
    download_datasets(args, 'healthdata.gov', datasets)

def cdc_download(args):
    '''Download datasets hosted at data.cdc.gov endpoints'''
    datasets = [
        Asset('weekly_united_states_covid_19_cases_and_deaths_by_state', 'pwn4-m3yp'),
        Asset('excess_deaths_associated_with_covid_19', 'xkkf-xrst'),
        Asset('covid_vaccinations_state', 'unsk-b7fc'),
        Asset('covid_vaccinations_county', '8xkx-amqh'),
        Asset('covid_vaccine_allocations_state_pfizer', 'saz5-9hgg'),
        Asset('covid_vaccine_allocations_state_moderna', 'b7pe-5nws'),
        Asset('covid_vaccine_allocations_state_janssen', 'w9zu-fywh'),
        Asset('nationwide_commercial_laborator_seroprevalence_survey', 'd2tw-32xv'),
        Asset('nationwide_blood_donor_seroprevalence', 'wi5c-cscz'),
        Asset('rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status', '3rge-nu2a'),
        Asset('rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status_and_booster_dose', 'd6p8-wqjm'),
        Asset('rates_of_covid_19_cases_or_deaths_by_age_group_and_vaccination_status_and_second_booster_dose', 'ukww-au2k'),
        Asset('united_states_covid_19_community_levels_by_county', '3nnm-4jni'),

        # Policy surveillance datasets. These have all been discontinued, so no need to download them again...
#        Asset('US-State-Territorial-and-County-Stay-At-Home-Orders-By-County-March-15-to-May-5', 'qz3x-mf9n'),
#        Asset('US-State-and-Territorial-Public-Mask-Mandates-By-State-2020-04-08-to-2021-09-15', 'tzyy-aayg'),
#        Asset('US-State-and-Territorial-Orders-Closing-and-Reopening-Bars-By-County-2020-03-11-to-2021-09-15', '9kjw-3miq'),
#        Asset('US-State-and-Territorial-Orders-Closing-and-Reopening-Restaurants-By-County-2020-03-11-to-2021-09-15', 'azmd-939x'),
#        Asset('State-Level-Restrictions-on-Vaccine-Mandates-All', '3m2r-fh4s'),
#        Asset('State-Level-Vaccine-Mandates-All', 'kw6u-z8u2'),
#        Asset('US-State-and-Territorial-Orders-Closing-and-Reopening-Restaurants-By-County-2020-03-11-to-2021-05-31', '647a-wjd2'),
#        Asset('US-State-and-Territorial-Orders-Closing-and-Reopening-Bars-By-County-2020-03-11-to-2021-05-31', 'kp49-9dp8'),
#        Asset('US-State-and-Territorial-Public-Mask-Mandates-By-County-2020-04-10-to-2021-09-15', '62d6-pm5i'),
#        Asset('US-State-and-Territorial-Stay-At-Home-Orders-By-County-2020-03-15-to-2021-09-15', 'y2iy-8irm'),
#        Asset('US-State-and-Territorial-Public-Mask-Mandates-By-County-2020-04-10-to-2021-07-20', '42jj-z7fa'),
#        Asset('US-State-and-Territorial-Gathering-Bans-By-County-2020-03-11-to-2021-09-15', '7xvh-y5vh'),
#        Asset('US-State-and-Territorial-Stay-At-Home-Orders-By-County-2020-03-15-to-2021-05-31', 'hm3s-vk7u'),
#        Asset('US-State-and-Territorial-Gathering-Bans-By-County-2020-03-15-to-2021-05-31', '3qs9-qnbs'),
#        Asset('Efforts-to-sustain-education-and-subsidized-meal-programs', 'jkmz-c8jz'),
    ]
    download_datasets(args, 'data.cdc.gov', datasets)


def download_datasets(args, server, datasets):
    with Socrata(server, get_socrata_app_token(args), timeout=60) as client:
        for dataset in datasets:
            logging.info('Fetching %s...', dataset.name)
            csv_file = dataset.get_csv(client)


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

        # CODE SMELL: Is the `session` attribute in the client morally private?
        r = client.session.get(url)

        outpath = f'{self.name}_{updated_at.strftime("%Y%m%d_%H%M")}.csv'
        with open(outpath, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        return outpath


