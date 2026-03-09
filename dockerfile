# Start with the official Airflow image
FROM apache/airflow:2.8.1-python3.10

# Switch to root to install system dependencies
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         gcc \
         g++ \
         python3-dev \
         librdkafka-dev \
         libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set Java Home so PySpark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to the airflow user
USER airflow

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# THE ANCHOR: We explicitly include apache-airflow==2.8.1 so pip cannot secretly uninstall it!
RUN pip install --no-cache-dir "apache-airflow==2.8.1" pyspark==3.5.0 delta-spark==3.1.0 confluent-kafka==2.3.0 psycopg2-binary==2.9.9 apache-airflow-providers-apache-spark dbt-core==1.7.9 "dbt-spark[pyspark]==1.7.1" streamlit==1.31.0