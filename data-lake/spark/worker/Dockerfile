FROM bitnamilegacy/spark:3.5.0

USER root

# Versions compatible with Spark 3.5.x / Hadoop 3.3.4
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DELTA_VERSION=3.2.0
ENV SCALA_VERSION=2.12

# Install utilities
RUN apt-get update && apt-get install -y curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Download S3A JARs (for MinIO)
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o /opt/bitnami/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Download Delta Lake JARs
RUN curl -sL https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    -o /opt/bitnami/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    -o /opt/bitnami/spark/jars/delta-storage-${DELTA_VERSION}.jar

# Copy configuration
COPY config/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY config/core-site.xml /opt/bitnami/spark/conf/core-site.xml
COPY config/hive-site.xml /opt/bitnami/spark/conf/hive-site.xml

USER 1001

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8081/ || exit 1

EXPOSE 8081
