# Use a standard Python image
FROM python:3.10-bullseye

# Install Java (Absolutely required for PySpark)
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set the Java Home environment variable so PySpark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Set the working directory
WORKDIR /workspace