#!/usr/bin/env bash
#
# Install devel stuff on a fresh EC2 instance
#

sudo yum install -y gcc python3 git jq
# AWS Linux installs a hella old version of pip
sudo python3 -m pip install --upgrade pip

python3 -m pip install --user pipx
python3 -m pipx ensurepath
pipx install parquet-metadata
pipx inject parquet-metadata pyarrow
pipx install csv2parquet

curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
source $HOME/.poetry/env

git clone https://github.com/sacundim/covid-19-puerto-rico
cd covid-19-puerto-rico
mkdir -p tmp output s3-bucket-sync/covid-19-puerto-rico-data
poetry install
