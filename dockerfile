
ARG Airflow_VERSION=2.9.2
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${Airflow_VERSION}-python${PYTHON_VERSION}

ARG Airflow_VERSION=2.9.2

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt /
RUN pip install --no-cache-dir  "apache-airflow==${Airflow_VERSION}" -r /requirements.txt
