#
# An environment to run the Bioportal download and sync scripts
# on the cloud.  The JSON to CSV conversion with jq is memory
# intensive, you want to use at least 8 GiB as of 2020-12-30,
# possibly more as the files get bigger, otherwise the OOM
# killer is going to be your worst friend.
#
FROM python:3.7-slim

RUN apt-get update
RUN apt-get install -y wget bzip2 time
RUN python3 -m pip install pipx
RUN pipx install awscli
RUN pipx install csv2parquet
RUN pipx inject csv2parquet pyarrow


# Debian Buster comes with jq 1.5, we want 1.6:
ARG JQ16_URL="https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64"
ARG JQ16_SHA256="af986793a515d500ab2d35f8d2aecd656e764504b789b66d7e1a0b727a124c44"
RUN wget -O /usr/local/bin/jq "${JQ16_URL}"
RUN [ "${JQ16_SHA256}  /usr/local/bin/jq" = "$(sha256sum /usr/local/bin/jq)" ]
RUN chmod +x /usr/local/bin/jq


WORKDIR /covid-19-puerto-rico
RUN mkdir -p \
    s3-bucket-sync/covid-19-puerto-rico-data \
    scripts \
    tmp
COPY \
    bioportal-basic-to-csv.sh \
    bioportal-tests-to-csv.sh \
    bioportal-download-and-sync.sh \
    bioportal-download.sh \
    bioportal-s3-sync.sh \
    hhs-download-and-sync.sh \
    hhs-download.sh \
    hhs-s3-sync.sh \
    ./scripts/
RUN chmod +x ./scripts/*.sh

ENV PATH=/root/.local/bin:/covid-19-puerto-rico/scripts:$PATH
