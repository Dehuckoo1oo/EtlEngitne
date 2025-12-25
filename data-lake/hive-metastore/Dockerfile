FROM apache/hive:4.0.0

USER root

# Install wget, netcat, download PostgreSQL JDBC driver and AWS libraries for S3 support
RUN apt-get update && \
    apt-get install -y wget netcat-openbsd && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -O /opt/hive/lib/postgresql-jdbc.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar && \
    ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar /opt/hive/lib/hadoop-aws-3.3.4.jar && \
    ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar && \
    rm -rf /var/lib/apt/lists/*

# Copy Hadoop configuration for MinIO S3 support
COPY --chown=hive:hive config/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

USER hive

ENTRYPOINT ["/entrypoint.sh"]
