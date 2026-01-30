FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Fix SPARK_HOME for pip-installed packages that expect /opt/spark
# Base image uses /usr/local/spark, but delta-spark expects /opt/spark
RUN mkdir -p /opt && ln -s /usr/local/spark /opt/spark
ENV SPARK_HOME=/usr/local/spark

# Download S3A and Delta Lake JARs (same versions as Spark cluster)
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DELTA_VERSION=3.2.0
ENV SCALA_VERSION=2.12

RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    -o ${SPARK_HOME}/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    -o ${SPARK_HOME}/jars/delta-storage-${DELTA_VERSION}.jar

# Copy Spark configuration
COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

USER jovyan

# Copy requirements and install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create notebooks directory
RUN mkdir -p /home/jovyan/work

WORKDIR /home/jovyan/work

EXPOSE 8888 4040 4041 4042
