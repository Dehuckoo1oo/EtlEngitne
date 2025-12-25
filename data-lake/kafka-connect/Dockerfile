FROM confluentinc/cp-kafka-connect:7.5.0

# Install Confluent S3 Sink Connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.0

# Switch to root to create system directories
USER root
RUN mkdir -p /data && chown appuser:appuser /data

# Switch back to non-privileged user
USER appuser
