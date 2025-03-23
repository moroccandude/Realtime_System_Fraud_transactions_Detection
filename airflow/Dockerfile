FROM apache/airflow:2.7.1

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libsasl2-dev \
    libsasl2-modules \
    libkrb5-dev \
    libssl-dev \
    default-libmysqlclient-dev \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create directories with proper permissions
RUN mkdir -p /opt/airflow/logs /opt/airflow/data /opt/airflow/files && \
    chown -R airflow:root /opt/airflow && \
    chmod -R g+rwx /opt/airflow

USER airflow

# Install Airflow with constraints
ARG AIRFLOW_VERSION=2.7.1
ARG PYTHON_VERSION=3.8
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" && \
    pip install --no-cache-dir \
    'apache-airflow-providers-apache-hdfs>=3.2.0' \
    'apache-airflow-providers-apache-hive>=5.1.0' \
    'pyhive[hive]>=0.6.5' \
    'thrift>=0.16.0' \
    'thrift-sasl>=0.4.3' \
    'pyhdfs>=0.3.1' \
    'hdfs>=2.7.0' \
    'requests>=2.28.0' \
    'python-dotenv>=1.0.0' \
    'apache-airflow-providers-openlineage>=1.8.0'

WORKDIR /opt/airflow