# Use the spark-base image as the base
FROM spark-base:latest

# Argument for the Airflow version
ARG AIRFLOW_VERSION=2.8.0
ARG PYTHON_VERSION=3.9 

# Export the environment variable AIRFLOW_HOME where Airflow will be installed
ENV AIRFLOW_HOME=/opt/airflow

# Set environment variables for Airflow
ENV PATH=$AIRFLOW_HOME/bin:$PATH

# Install dependencies and tools
RUN apt-get update -y && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
    python3-dev \
    build-essential \
    gcc \
    g++ \
    libpq-dev \
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils \
    freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libffi-dev \
    freetds-bin \
    default-libmysqlclient-dev \
    pkg-config \
    apt-utils \
    rsync \
    zip \
    unzip \
    vim \
    netcat \
    && apt-get autoremove -yqq --purge && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create airflow user and group
RUN groupadd -r airflow && useradd -r -g airflow airflow

# Install Apache Airflow and its dependencies
RUN pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh,docker,hdfs,redis,slack,webhdfs,spark]==${AIRFLOW_VERSION} \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    --root-user-action=ignore

# Create necessary directories and set owner to airflow user
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins && \
    chown -R airflow:airflow ${AIRFLOW_HOME}

# Copy the start-airflow.sh script from the host to the container
COPY ./start-airflow.sh ${AIRFLOW_HOME}/start-airflow.sh

# Set the start-airflow.sh file to be executable
RUN chmod +x ${AIRFLOW_HOME}/start-airflow.sh

# Set the username to use the airflow user
USER airflow

# Expose ports (Airflow webserver and scheduler)
EXPOSE 8080 8793

# Set the working directory to Airflow home
WORKDIR ${AIRFLOW_HOME}

# Start Airflow using the entrypoint script
CMD ["./start-airflow.sh"]
