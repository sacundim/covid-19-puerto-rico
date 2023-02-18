FROM python:3.9-slim

RUN apt-get update
RUN apt-get install -y git
RUN python3 -m pip install pipx
RUN pipx install dbt-core
RUN pipx inject dbt-core dbt-athena-community
ENV PATH=/root/.local/bin:$PATH

# Install DBT packages first so as to exploit Docker
# build cache
WORKDIR /covid-19-puerto-rico/aws/athena-dbt/
# Ideally we'd just copy the packages.yml
COPY packages.yml dbt_project.yml profiles.yml ./
RUN dbt deps

# Now actually install our project
COPY . .

ENTRYPOINT ["./run-models.sh"]