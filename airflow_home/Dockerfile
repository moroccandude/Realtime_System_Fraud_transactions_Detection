# Use the official Airflow image as base
FROM apache/airflow:2.7.1

USER root

# Install system dependencies
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#     openjdk-11-jdk \
#     build-essential \
#     software-properties-common \
#     ssh \
#     git \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Switch back to airflow user
USER airflow

# Install Python dependencies
# RUN pip install --no-cache-dir \
#     pyhive[hive] \
#     thrift \
#     thrift_sasl \
#     sasl \
#     requests \
#     python-dotenv \
#     hdfs \
#     pytest \
#     pytest-mock \
#     mock

# Create directories
WORKDIR /opt/airflow

# Create data directory structure
RUN mkdir -p dags/data \
    && mkdir -p dags/hadoop/hadoop_namenode \
    && mkdir -p logs \
    && mkdir -p config \
    && mkdir -p plugins

# Copy the DAG file
COPY --chown=airflow:root ./dags/dags.py dags/

# Create a .env file template
RUN echo "HIVE_HOST=hive-server\nHIVE_PORT=10000\nHIVE_USERNAME=hive\nHIVE_DATABASE=default\nHIVE_PASSWORD=\nHIVE_AUTH=CUSTOM" > .env

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
ENV AIRFLOW__CORE__FERNET_KEY=''
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

# Initialize Airflow variables with defaults
ENV AIRFLOW_VAR_api_base_url=http://localhost:8000/api/v1
ENV AIRFLOW_VAR_local_data_path=/opt/airflow/dags/data
ENV AIRFLOW_VAR_hdfs_warehouse_dir=/opt/hive/warehouse
ENV AIRFLOW_VAR_hive_host=hive-server
ENV AIRFLOW_VAR_hive_port=10000
ENV AIRFLOW_VAR_hive_username=hive
ENV AIRFLOW_VAR_hive_database=default
ENV AIRFLOW_VAR_hive_auth=CUSTOM

# Expose ports
EXPOSE 8080

# The entrypoint is provided by the base image