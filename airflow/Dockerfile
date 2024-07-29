FROM python:3.10-slim-buster

# Create necessary directories and install dependencies
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y openjdk-11-jdk librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN echo $JAVA_HOME

# Install additional packages including PostgreSQL development libraries
RUN apt-get update -y && \
    apt-get install -y libzbar-dev bash gcc git libc-dev curl wget vim nano \
    iputils-ping telnet openssh-client net-tools man unzip vim-tiny bc \
    openssh-server thrift-compiler netcat sudo build-essential \
    libpq-dev && \
    apt-get autoremove -y && \
    apt-get clean

# Upgrade pip, setuptools, and wheel
RUN pip install --upgrade pip setuptools wheel

# Install Spark
RUN curl -o spark-3.3.4-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz && \
    tar -xzvf spark-3.3.4-bin-hadoop3.tgz && \
    mv spark-3.3.4-bin-hadoop3 /opt/spark && \
    rm -rf spark-3.3.4-bin-hadoop3.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# Copy Airflow DAGs and configuration
COPY ./dags /opt/airflow/dags
RUN chmod -R a+rwx /opt/airflow/dags

COPY airflow.cfg /opt/airflow/airflow.cfg
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create airflow user and group
RUN groupadd -r airflow && useradd -r -g airflow airflow

# Set permissions for airflow home
RUN mkdir -p /opt/airflow && chown -R airflow:airflow /opt/airflow

# Set working directory
WORKDIR /opt/airflow

# Expose Airflow web server port
EXPOSE 8000

# Use airflow user for running Airflow
USER airflow

# Command to run the entrypoint script
CMD ["/entrypoint.sh"]
