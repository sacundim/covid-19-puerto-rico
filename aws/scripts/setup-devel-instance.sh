#!/usr/bin/env bash
#
# Install devel stuff on a fresh EC2 instance
#

sudo yum install -y python3 git
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
source $HOME/.poetry/env
git clone https://github.com/sacundim/covid-19-puerto-rico
cd covid-19-puerto-rico
mkdir -p output s3-bucket-sync/covid-19-puerto-rico-data
poetry install
