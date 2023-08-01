ARG PYTHON_VERSION="3.11"

#####################################################################
#####################################################################
##
## The Python build environment image.  Note this is based on the
## default "fat" Python Docker images, not the "slim" ones.  Building
## with the "fat" image is better because you have more tools like
## git and gcc which are sometimes required or useful.  But later we
## install our artifacts into a "slim" base image.
##
FROM python:${PYTHON_VERSION} AS build
ARG POETRY_VERSION="1.5.1"
ENV POETRY_VIRTUALENVS_CREATE=false
RUN --mount=type=cache,target=/root/.cache \
    pip install poetry=="${POETRY_VERSION}"

WORKDIR /covid-19-puerto-rico

##
## Download or build binary wheels for the project's dependencies into
## the `dist/` directory.  This doesn't install the dependencies, just
## collects or produces artifacts that we will `pip install` in our
## app image.
##
## We take care to make this step depend exclusively on our project's
## `pyproject.toml` and `poetry.lock`, so as long as those two files
## don't change, Docker's cache of these steps remains valid.
##
COPY pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=/root/.cache \
    poetry export \
      --without-hashes \
      -f requirements.txt >requirements.txt \
 && pip wheel --wheel-dir=dist -r requirements.txt


##
## Only now, after we've produced the project's build dependencies,
## do we loop in our actual code.
##
## Note that we do `poetry build` to generate a wheel file (into `dist/`)
## that we `pip install` in our app image.
##
COPY src src
RUN --mount=type=cache,target=/root/.cache \
    poetry build --format wheel



#####################################################################
#####################################################################
##
## The actual app image.  Note we use a "slim" Python image.
##
FROM python:${PYTHON_VERSION}-slim AS app

# Install various prerequistes from the OS distribution.
# Magic APT caching incantations from Docker docs:
#
# * https://docs.docker.com/engine/reference/builder/#run---mounttypecache
#
RUN rm -f /etc/apt/apt.conf.d/docker-clean \
    && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' \
          > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt update && apt-get --no-install-recommends install -y ca-certificates lbzip2


# The rclone packaged with the OS distribution is always hopelessly out
# of date, better to get it from the official rclone Docker images.
# See: https://github.com/rclone/rclone/issues/6060
COPY --link --from=rclone/rclone:1.61.1 \
    /usr/local/bin/rclone \
    /usr/local/bin/rclone


# `covid19datos.salud.gov.pr`'s SSL certificate is missing its
# intermediate cert, so we check in a copy of that into our repo
# and install it by hand.
COPY --link certs/*.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates
ENV REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt"

##
## We pip install from the wheels we prepared in our build stage,
## without going to PyPi at all. Then we remove pip & friends for
## further space savings.
##
WORKDIR /covid-19-puerto-rico
RUN --mount=type=cache,from=build,source=/covid-19-puerto-rico/dist,target=/covid-19-puerto-rico/dist \
    pip install --no-index --find-links=dist covid_19_puerto_rico_downloader \
 && pip uninstall --yes wheel setuptools pip pipx

