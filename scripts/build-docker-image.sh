#!/usr/bin/env bash

set -e
cd "$(dirname $0)"/..
docker build . -t covid-19-puerto-rico