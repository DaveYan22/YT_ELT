ARG AIRFLOW_VERSION=3.1.3
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt ./

# Fix: Remove the leading slash (/) to correctly reference the file in the current directory.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt