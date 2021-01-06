#!/usr/bin/env bash
#
# Install devel stuff on a fresh EC2 instance
#

sudo apt update
sudo apt install -y python3-pip python3-venv awscli

curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
source $HOME/.poetry/env

git clone https://github.com/sacundim/covid-19-puerto-rico
cd covid-19-puerto-rico
mkdir -p tmp output s3-bucket-sync/covid-19-puerto-rico-data
poetry install
