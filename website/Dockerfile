FROM python:3.11-slim AS base

FROM base AS poetry
ARG POETRY_VERSION="1.5.1"
ENV POETRY_VIRTUALENVS_CREATE=false
RUN --mount=type=cache,target=/root/.cache \
    pip install poetry=="${POETRY_VERSION}"


FROM poetry AS requirements
ENV POETRY_VIRTUALENVS_CREATE=false
WORKDIR /covid-19-puerto-rico
COPY pyproject.toml poetry.lock ./
RUN poetry export \
    --without-hashes \
    -f requirements.txt >requirements.txt


FROM requirements AS build
WORKDIR /covid-19-puerto-rico
COPY src src
RUN poetry build


FROM base AS app
# The rclone packaged with the OS distribution is always hopelessly out
# of date, better to get it from the official rclone Docker images.
# See: https://github.com/rclone/rclone/issues/6060
COPY --link --from=rclone/rclone:1.61.1 \
    /usr/local/bin/rclone \
    /usr/local/bin/rclone
WORKDIR /covid-19-puerto-rico
COPY --link --from=requirements /covid-19-puerto-rico/requirements.txt ./
RUN apt update && apt install -y git
RUN --mount=type=cache,target=/root/.cache \
    pip install -r requirements.txt && rm requirements.txt
COPY --link --from=build /covid-19-puerto-rico/dist/covid_19_puerto_rico-*.whl .
RUN --mount=type=cache,target=/root/.cache \
    pip install covid_19_puerto_rico-*.whl && rm covid_19_puerto_rico-*.whl

COPY --link config/environment.yaml environment.yaml
ENV PATH=/covid-19-puerto-rico:$PATH
CMD ["covid19pr"]