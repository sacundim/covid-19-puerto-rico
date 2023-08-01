ARG PYTHON_VERSION="3.11"

#####################################################################
#####################################################################
##
## Fat build image
##
FROM python:${PYTHON_VERSION} AS build
ARG POETRY_VERSION="1.5.1"
ENV POETRY_VIRTUALENVS_CREATE=false
RUN --mount=type=cache,target=/root/.cache \
    pip install poetry=="${POETRY_VERSION}"

WORKDIR /covid-19-puerto-rico

COPY pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=/root/.cache \
    poetry export \
      --without-hashes \
      -f requirements.txt >requirements.txt \
 && pip wheel --wheel-dir=dist -r requirements.txt


COPY src src
RUN --mount=type=cache,target=/root/.cache \
    poetry build --format wheel



#####################################################################
#####################################################################
##
## Slim app image
##
FROM python:${PYTHON_VERSION}-slim AS app

# The rclone packaged with the OS distribution is always hopelessly out
# of date, better to get it from the official rclone Docker images.
# See: https://github.com/rclone/rclone/issues/6060
COPY --link --from=rclone/rclone:1.61.1 \
    /usr/local/bin/rclone \
    /usr/local/bin/rclone

WORKDIR /covid-19-puerto-rico
RUN --mount=type=cache,from=build,source=/covid-19-puerto-rico/dist,target=/covid-19-puerto-rico/dist \
    pip install --no-index --find-links=dist covid_19_puerto_rico \
 && pip uninstall --yes wheel setuptools pip

COPY --link config/environment.yaml environment.yaml
CMD ["covid19pr"]