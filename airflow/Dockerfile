FROM python:3.9.16-slim

# Install OpenJDK 11 and required libraries
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y openjdk-11-jdk libpq-dev libpcap-dev && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
RUN echo $JAVA_HOME

# Install additional packages and clean up
RUN apt-get update -y && \
    apt-get install -y libzbar-dev bash gcc git libc-dev curl wget vim nano \
    iputils-ping telnet openssh-client net-tools man unzip vim-tiny bc \
    openssh-server thrift-compiler netcat sudo build-essential && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark
RUN curl -o spark-3.1.1-bin-hadoop3.2.tgz https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz && \
    tar -xzvf spark-3.1.1-bin-hadoop3.2.tgz && \
    mv spark-3.1.1-bin-hadoop3.2 /opt/spark && \
    rm -rf spark-3.1.1-bin-hadoop3.2.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python3:$SPARK_HOME/python3/lib/py4j-0.10.7-src.zip:$PYTHONPATH

# Copy and install Python dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs and set permissions
COPY ./dags /opt/airflow/dags
RUN chmod -R a+rwx /opt/airflow

# Copy Airflow configuration
COPY airflow.cfg /opt/airflow/airflow.cfg

# Copy and set permissions for run.sh
COPY run.sh /run.sh
RUN chmod +x /run.sh

# Set working directory
WORKDIR /opt/airflow

# Set entry point and expose port
CMD ["/run.sh"]
EXPOSE 8000
