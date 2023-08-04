FROM python:3.11-slim
ARG BOTO3_VERSION
ARG DBT_DUCKDB_VERSION="1.6.0"
COPY requirements.txt /
RUN pip install -r /requirements.txt && rm /requirements.txt
COPY issue-226.py /
CMD ["/issue-226.py"]