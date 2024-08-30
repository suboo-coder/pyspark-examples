FROM python:3.11-bullseye

RUN apt-get update && apt-get install -y \
    scala \
    openjdk-11-jdk \
    bash

# Install spark 3.0.1
RUN mkdir /opt/spark && \
    curl -L -o spark-3.5.2-bin-hadoop3.tgz https://dlcdn.apache.org/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz && \
    tar xvzf spark-3.5.2-bin-hadoop3.tgz --directory /opt/spark --strip-components 1

# Set up environment variables
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/spark
# Add /opt/spark to path
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install python dependencies
RUN pip install pandas
RUN pip install psycopg2

# Move the helloworld-example directory to the container
COPY . /pyspark-examples

# COPY postgresql-42.7.4.jar driver under /opt/spark/jars
COPY postgresql-42.7.4.jar /opt/spark/jars

CMD ["/bin/bash"]
