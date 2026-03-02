# Start with the official Airflow image
FROM apache/airflow:2.8.1-python3.10

# Switch to root to install system dependencies
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         gcc \
         g++ \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set Java Home so PySpark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to the airflow user to install Python packages safely
USER airflow

# Install ALL pipeline dependencies here so the image is 100% portable
# Install ALL pipeline dependencies here so the image is 100% portable
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.1.0 \
    confluent-kafka==2.3.0 \
    dbt-core==1.7.9 \
    dbt-spark[pyspark]==1.7.1 \
    apache-airflow-providers-apache-spark \
    streamlit==1.31.0 \
    psycopg2-binary==2.9.9