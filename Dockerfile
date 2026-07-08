FROM apache/airflow:2.9.2-python3.11

# Switch to root to install system level dependencies if required
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user to install python libraries
USER airflow

# Install python dependencies
RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    fastparquet \
    dbt-core \
    dbt-sqlite
