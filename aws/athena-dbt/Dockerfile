FROM python:3.10-slim AS dbt-athena
RUN --mount=type=cache,target=/root/.cache \
    python3 -m pip install pipx
ARG DBT_CORE_VERSION="1.5.2"
RUN --mount=type=cache,target=/root/.cache \
    pipx install dbt-core=="$DBT_CORE_VERSION"
ARG DBT_ATHENA_VERSION="1.5.1"
RUN --mount=type=cache,target=/root/.cache \
    pipx inject dbt-core dbt-athena-community=="$DBT_ATHENA_VERSION"
ENV PATH=/root/.local/bin:$PATH


FROM dbt-athena
# Install DBT packages first so as to exploit Docker
# build cache
WORKDIR /covid-19-puerto-rico/aws/athena-dbt/
# Ideally we'd just copy the packages.yml
COPY --link packages.yml dbt_project.yml ./
RUN dbt deps

# Now actually install our project
COPY --link . .
# Put our special Docker `profiles.yml` into the
# root of the project so that it's used in this
# image but not when we use the project outside of
# Docker
RUN mv docker/profiles.yml . && rmdir docker/

CMD ["./run-models.sh"]