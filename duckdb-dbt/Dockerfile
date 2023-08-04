FROM python:3.11-slim AS dbt-duckdb
COPY --link requirements.txt .
RUN --mount=type=cache,target=/root/.cache \
    python3 -m pip install -r requirements.txt \
 && rm requirements.txt


FROM dbt-duckdb
# Install DBT packages first so as to exploit Docker
# build cache
WORKDIR /covid-19-puerto-rico-duckdb/
# Ideally we'd just copy the packages.yml
COPY --link packages.yml dbt_project.yml ./
RUN dbt deps

# Now actually install our project
COPY --link . .

CMD ["dbt", "build", "--target", "aws"]