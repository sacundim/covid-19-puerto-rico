FROM python:3.11-slim AS dbt-athena
COPY --link requirements.txt .
RUN --mount=type=cache,target=/root/.cache \
    python3 -m pip install -r requirements.txt \
 && rm requirements.txt


FROM dbt-athena
# Install DBT packages first so as to exploit Docker
# build cache
WORKDIR /covid-19-puerto-rico-dbt/
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