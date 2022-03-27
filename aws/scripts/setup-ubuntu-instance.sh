#!/usr/bin/env bash
#
# Install devel stuff on a fresh EC2 instance
#
# To log in: ssh ubuntu@[public-ec2-hostname]
#

# This repository has a big menu of Python versions
sudo add-apt-repository ppa:deadsnakes/ppa

sudo apt update
sudo apt install -y python3.9 awscli

curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
source $HOME/.poetry/env

git clone https://github.com/sacundim/covid-19-puerto-rico
cd covid-19-puerto-rico
mkdir -p tmp output s3-bucket-sync/covid-19-puerto-rico-data
poetry install
