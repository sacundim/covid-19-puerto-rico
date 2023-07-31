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

ENV VIRTUAL_ENV="/covid-19-puerto-rico/venv"
RUN python3 -m venv "${VIRTUAL_ENV}"
ENV PATH="${VIRTUAL_ENV}/bin":$PATH


COPY pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=/root/.cache \
    poetry export \
      --without-hashes \
      -f requirements.txt >requirements.txt \
 && pip install -r requirements.txt


COPY src src
RUN --mount=type=cache,target=/root/.cache \
    poetry build \
 && pip install dist/covid_19_puerto_rico-*.whl



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
ENV VIRTUAL_ENV="/covid-19-puerto-rico/venv"
COPY --link --from=build "${VIRTUAL_ENV}" "${VIRTUAL_ENV}"
ENV PATH="${VIRTUAL_ENV}/bin":$PATH

COPY --link config/environment.yaml environment.yaml
CMD ["covid19pr"]