#!/usr/bin/env python3
"""
Kafka to MinIO Streaming with Spark Structured Streaming

Читает данные из Kafka топика order-events и пишет в MinIO в формате Parquet.
Решает проблему мелких файлов: объединяет данные из всех 72 Kafka партиций
в N файлов оптимального размера.

Usage:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
        kafka_to_minio.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
import requests
import json
import sys
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaToMinioStreaming:
    """Spark Structured Streaming: Kafka → MinIO"""

    def __init__(self, config):
        self.config = config
        self.spark = self._create_spark_session()
        self.avro_schema = None

    def _create_spark_session(self):
        """Создание Spark сессии с настройками для Kafka и S3"""
        logger.info("Creating Spark session...")

        spark = SparkSession.builder \
            .appName(self.config['app_name']) \
            .config("spark.hadoop.fs.s3a.endpoint", self.config['s3_endpoint']) \
            .config("spark.hadoop.fs.s3a.access.key", self.config['s3_access_key']) \
            .config("spark.hadoop.fs.s3a.secret.key", self.config['s3_secret_key']) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()

        spark.sparkContext.setLogLevel(self.config.get('log_level', 'WARN'))

        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Spark UI: http://localhost:4040")

        return spark

    def _get_avro_schema(self):
        """Получает Avro схему из Schema Registry"""
        subject = f"{self.config['kafka_topic']}-value"
        url = f"{self.config['schema_registry_url']}/subjects/{subject}/versions/latest"

        logger.info(f"Fetching schema from {url}")
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(f"Failed to get schema: {response.status_code} - {response.text}")

        schema_json = response.json()['schema']
        logger.info("Avro schema loaded successfully")

        return schema_json

    def _read_from_kafka(self):
        """Читает streaming данные из Kafka"""
        logger.info(f"Reading from Kafka: {self.config['kafka_bootstrap_servers']} / {self.config['kafka_topic']}")

        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['kafka_bootstrap_servers']) \
            .option("subscribe", self.config['kafka_topic']) \
            .option("startingOffsets", self.config.get('starting_offsets', 'latest')) \
            .option("maxOffsetsPerTrigger", self.config.get('max_offsets_per_trigger', 100000)) \
            .option("kafka.session.timeout.ms", "30000") \
            .option("kafka.request.timeout.ms", "40000") \
            .load()

        logger.info("Kafka stream created")
        return kafka_df

    def _deserialize_avro(self, kafka_df):
        """Десериализует Avro данные (с пропуском Schema Registry ID)"""
        logger.info("Deserializing Avro data...")

        # Получаем схему если еще не загружена
        if self.avro_schema is None:
            self.avro_schema = self._get_avro_schema()

        # Пропускаем первые 5 байт (magic byte + schema ID) и десериализуем
        df = kafka_df.select(
            from_avro(
                F.expr("substring(value, 6, length(value)-5)"),
                self.avro_schema
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset")
        )

        # Раскрываем структуру
        df = df.select("data.*", "kafka_timestamp", "kafka_partition", "kafka_offset")

        # Добавляем партиционные поля для MinIO
        partition_format = self.config.get('partition_format', 'hourly')

        if partition_format == 'hourly':
            df = df.withColumn("calc_id", F.date_format(F.col("kafka_timestamp"), "yyyyMMdd-HHmmss")) \
                   .withColumn("dt", F.date_format(F.col("kafka_timestamp"), "yyyy-MM-dd")) \
                   .withColumn("hour", F.date_format(F.col("kafka_timestamp"), "HH"))
        elif partition_format == 'daily':
            df = df.withColumn("dt", F.date_format(F.col("kafka_timestamp"), "yyyy-MM-dd"))

        logger.info("Avro deserialization configured")
        return df

    def _write_batch(self, batch_df, batch_id):
        """
        Функция для записи каждого батча в MinIO.
        Объединяет все Kafka партиции в N файлов.
        """
        logger.info(f"{'='*70}")
        logger.info(f"Processing batch {batch_id}")
        logger.info(f"{'='*70}")

        row_count = batch_df.count()
        logger.info(f"Rows in batch: {row_count:,}")

        if row_count == 0:
            logger.info("Empty batch, skipping")
            return

        # Объединяем 72 Kafka партиции в N файлов
        num_files = self.config.get('num_output_files', 4)
        batch_df_coalesced = batch_df.coalesce(num_files)

        # Настройки для записи
        output_path = f"s3a://{self.config['s3_bucket']}/{self.config['s3_path']}/"
        partition_cols = self.config.get('partition_columns', ['calc_id', 'dt', 'hour'])

        logger.info(f"Writing to {output_path} in {num_files} files")

        # Записываем в Parquet
        writer = batch_df_coalesced.write \
            .mode("append") \
            .format("parquet") \
            .option("compression", self.config.get('compression', 'snappy'))

        # Опциональные настройки Parquet
        if 'parquet_block_size' in self.config:
            writer = writer.option("parquet.block.size", self.config['parquet_block_size'])
        if 'parquet_page_size' in self.config:
            writer = writer.option("parquet.page.size", self.config['parquet_page_size'])

        # Партиционирование
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(output_path)

        logger.info(f"✓ Written {row_count:,} rows in {num_files} files")
        logger.info(f"{'='*70}")

    def start_streaming(self):
        """Запускает streaming job"""
        try:
            # Читаем из Kafka
            kafka_df = self._read_from_kafka()

            # Десериализуем
            df = self._deserialize_avro(kafka_df)

            # Запускаем streaming query
            trigger_interval = self.config.get('trigger_interval', '5 minutes')
            checkpoint_location = self.config['checkpoint_location']

            logger.info(f"Starting streaming query with trigger interval: {trigger_interval}")
            logger.info(f"Checkpoint location: {checkpoint_location}")

            query = df.writeStream \
                .foreachBatch(self._write_batch) \
                .trigger(processingTime=trigger_interval) \
                .option("checkpointLocation", checkpoint_location) \
                .start()

            logger.info(f"Streaming started!")
            logger.info(f"Query ID: {query.id}")
            logger.info(f"Waiting for termination...")

            # Ждем завершения
            query.awaitTermination()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping...")
            query.stop()
        except Exception as e:
            logger.error(f"Error in streaming: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point"""

    # Конфигурация (можно вынести в config.yaml или env vars)
    config = {
        'app_name': 'Kafka-to-MinIO-Streaming',

        # Kafka
        'kafka_bootstrap_servers': 'kafka:9092',
        'kafka_topic': 'order-events',
        'starting_offsets': 'latest',  # или 'earliest' для полной загрузки
        'max_offsets_per_trigger': 100000,

        # Schema Registry
        'schema_registry_url': 'http://schema-registry:8081',

        # MinIO/S3
        's3_endpoint': 'http://minio:9000',
        's3_access_key': 'minioadmin',
        's3_secret_key': 'minioadmin',
        's3_bucket': 'datalake',
        's3_path': 'topics-streaming/order-events',

        # Streaming
        'trigger_interval': '5 minutes',  # Как часто записывать
        'checkpoint_location': '/tmp/checkpoints/kafka-to-minio',

        # Output
        'num_output_files': 4,  # 72 Kafka партиции → 4 файла
        'partition_columns': ['calc_id', 'dt', 'hour'],
        'partition_format': 'hourly',  # или 'daily'

        # Parquet
        'compression': 'snappy',
        'parquet_block_size': 268435456,  # 256 MB
        'parquet_page_size': 1048576,     # 1 MB

        # Logging
        'log_level': 'INFO'
    }

    logger.info("Starting Kafka to MinIO streaming application")
    logger.info(f"Config: {json.dumps({k: v for k, v in config.items() if 'key' not in k.lower()}, indent=2)}")

    streaming_app = KafkaToMinioStreaming(config)
    streaming_app.start_streaming()


if __name__ == "__main__":
    main()
