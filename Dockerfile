ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.2.0-python3.11
FROM ${AIRFLOW_BASE_IMAGE}

USER root

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER airflow
