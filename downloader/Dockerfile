FROM python:3.9-slim AS base

FROM base AS poetry
RUN --mount=type=cache,target=/root/.cache \
    pip install poetry==1.5.0


#####################################################################
#####################################################################
##
## The Python requirements image.  This depends exclusively on our
## project's pyproject.toml` and `poetry.lock`, so as long as those
## two files don't change, Docker's cache of these steps remains valid.
##
## Note that the `poetry export` command doesn't install the dependencies,
## it just generates a `requirements.txt` file that `pip install` knows
## how to consume without Poetry.
##
FROM poetry AS requirements
WORKDIR /covid-19-puerto-rico
COPY pyproject.toml poetry.lock ./
RUN poetry export \
    --without-hashes \
    -f requirements.txt >requirements.txt



#####################################################################
#####################################################################
##
## This image packages our project's actual code as a Python wheel.
## Note that the `poetry build` command doesn't install dependencies;
## all it does is package our own code.
##
FROM requirements AS build
WORKDIR /covid-19-puerto-rico
COPY src src
RUN poetry build


#####################################################################
#####################################################################
##
## The actual app image.
##
FROM base as app
# Install various prerequistes from the OS distribution
RUN apt-get update
RUN apt-get install -y ca-certificates lbzip2


# The rclone packaged with the OS distribution is always hopelessly out
# of date, better to get it from the official rclone Docker images.
# See: https://github.com/rclone/rclone/issues/6060
COPY --from=rclone/rclone:1.61.1 \
    /usr/local/bin/rclone \
    /usr/local/bin/rclone


# Install our project's Python dependencies all by their lonesome, so
# that Docker will cache this step and not redo it unless they change.
# Note that since we generated a `requirements.txt` above, we don't need
# Poetry here, `pip install -r` is enough.
WORKDIR /covid-19-puerto-rico
COPY --from=requirements \
    /covid-19-puerto-rico/requirements.txt \
    ./
RUN --mount=type=cache,target=/root/.cache \
    pip install -r requirements.txt \
 && rm requirements.txt


# `covid19datos.salud.gov.pr`'s SSL certificate is missing its
# intermediate cert, so we check in a copy of that into our repo
# and install it by hand.
COPY certs/*.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates
ENV REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"


# Install our actual stuff.  Since all the dependencies have
# already been installed above, these steps can be blazing fast.
# And again since we packaged our code above as a Python wheel
# (`.whl` file), we don't need Poetry here, `pip install` will do.
RUN mkdir -p scripts s3-bucket-sync/covid-19-puerto-rico-data
COPY scripts/run-and-sync.sh ./scripts/
RUN chmod +x ./scripts/*.sh
ENV PATH=/covid-19-puerto-rico/scripts:$PATH
COPY --from=build \
    /covid-19-puerto-rico/dist/covid_19_puerto_rico_downloader-*.whl \
    .
RUN --mount=type=cache,target=/root/.cache \
    pip install covid_19_puerto_rico_downloader-*.whl \
 && rm covid_19_puerto_rico_downloader-*.whl


ENTRYPOINT ["run-and-sync.sh"]