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
## Create a virtualenv and set the environment variables to activate it
## by hand (`source venv/bin/activate` doesn't work in a Docker build)
##
ENV VIRTUAL_ENV="/covid-19-puerto-rico/venv"
RUN python3 -m venv "${VIRTUAL_ENV}" \
 && rm "${VIRTUAL_ENV}"/bin/[Aa]ctivate*
ENV PATH="${VIRTUAL_ENV}/bin":$PATH


##
## Install the project requirements to the virtualenv.  We take care
## to make this depend exclusively on our project's pyproject.toml`
## and `poetry.lock`, so as long as those two files don't change,
## Docker's cache of these steps remains valid.
##
COPY --link pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=/root/.cache \
    poetry export \
      --without-hashes \
      -f requirements.txt >requirements.txt \
 && pip install -r requirements.txt


##
## Only now, after we've installed the project's build dependencies,
## do we loop in our actual code and install to our venv.
##
## Note that we do `poetry build` to generate a wheel file that we
## `pip install`; if we do a `poetry install` it doesn't install our
## code to the venv, but rather puts a link there to the source tree
## which then means we can't copy it over to the app image.
##
## After we've installed our app into the venv, we don't need pip and
## friends anymore, we can really just delete them; saves about 25 MB
##
COPY --link src src
RUN --mount=type=cache,target=/root/.cache \
    poetry build \
 && pip install dist/covid_19_puerto_rico_downloader-*.whl \
 && pip uninstall --yes wheel setuptools pip



#####################################################################
#####################################################################
##
## The actual app image.  Note we use a "slim" Python image.
##
FROM python:${PYTHON_VERSION}-slim AS app


##
## Since we're just copying a venv over to here, we don't need pip
## and friends; just nuke them to save space
##
RUN pip uninstall --yes wheel setuptools pip

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


# Install our project's Python virtualenv
WORKDIR /covid-19-puerto-rico
ENV VIRTUAL_ENV="/covid-19-puerto-rico/venv"
COPY --link --from=build "${VIRTUAL_ENV}" "${VIRTUAL_ENV}"
ENV PATH="${VIRTUAL_ENV}/bin":$PATH