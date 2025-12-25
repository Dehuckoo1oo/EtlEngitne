# Kafka Connect Production Deployment Guide

## Содержание
1. [Обзор](#обзор)
2. [Архитектура](#архитектура)
3. [Требования](#требования)
4. [Production развертывание](#production-развертывание)
5. [Конфигурация](#конфигурация)
6. [S3 Sink Коннектор](#s3-sink-коннектор)
7. [Высокая доступность](#высокая-доступность)
8. [Мониторинг](#мониторинг)
9. [Операции](#операции)
10. [Troubleshooting](#troubleshooting)

---

## Обзор

Kafka Connect - это распределенная платформа для подключения Apache Kafka к внешним системам. В Data Lake используется для реплицирования данных из Kafka topics в MinIO в формате Parquet с использованием S3 Sink Connector.

### Роль в архитектуре
- Интеграция между Kafka и объектным хранилищем (MinIO)
- Конвертация событий в Parquet формат
- Временные партиции данных по часам и дням
- Ленивая непрерывная загрузка данных в Data Lake

### Зависимости
- **Входящие подключения**: Kafka brokers (для чтения events), Schema Registry (Avro conversion), MinIO (S3 API)
- **Исходящие подключения**: Другие Kafka Connect узлы (в distributed cluster режиме)

### Поддерживаемые коннекторы
- **S3 Sink** (Confluent, версия 10.5.0)
  - Запись Parquet файлов в S3/MinIO
  - Поддержка Avro, JSON, Protobuf форматов
  - Time-based partitioning
  - Flush и rotation policies

---

## Архитектура

### Single Instance (Development)
```
┌─────────────────────────┐
│  Kafka Brokers (3)      │
│  Topics, Partitions     │
└────────────┬────────────┘
             │
             ↓
┌─────────────────────────┐
│  Kafka Connect Worker   │
│  Port: 8083 (REST API)  │
│  Connectors & Tasks     │
└────────────┬────────────┘
             │
        ┌────┴─────┐
        ↓          ↓
    ┌───────┐  ┌───────────┐
    │MinIO  │  │  Schema   │
    │S3 API │  │ Registry  │
    └───────┘  └───────────┘
```

### Distributed Cluster (Production) - 2-3 узла
```
        ┌──────────────────────────┐
        │  Kafka Brokers (3)       │
        │  Topics, Partitions      │
        └──────────┬───────────────┘
                   │
    ┌──────────────┼──────────────┐
    ↓              ↓              ↓
┌─────────────┐┌─────────────┐┌─────────────┐
│  Connect 1  ││  Connect 2  ││  Connect 3  │
│ :8083 REST  ││ :8083 REST  ││ :8083 REST  │
│  Topics:    ││  Topics:    ││  Topics:    │
│  config     ││  offsets    ││  status     │
│  offsets    ││  status     ││  (replica)  │
│  status     ││  config     ││             │
└──────┬──────┘└──────┬──────┘└──────┬──────┘
       │              │              │
       └──────────────┼──────────────┘
                      │
            ┌─────────┴────────┐
            ↓                  ↓
        ┌────────┐         ┌──────────┐
        │ MinIO  │         │ Schema   │
        │        │         │ Registry │
        └────────┘         └──────────┘
            ↑
        Parquet Files
        (time-partitioned)
```

**Distributed Mode (Stateless)**:
- Каждый worker узел идентичен
- Состояние хранится в Kafka topics (__connect-config, __connect-offsets, __connect-status)
- Автоматический балансинг задач между узлами
- Отказоустойчивость: при падении узла, его задачи переходят на другие
- Scale up/down просто добавляя/удаляя узлы в группе

---

## Требования

### Аппаратные требования (на узел)

#### Minimum (Testing)
- **CPU**: 4 cores
- **RAM**: 8GB
- **Storage**: 50GB SSD (для сохранения offset и connector metadata)
- **Network**: 1 Gbit/s

#### Recommended (Production)
- **CPU**: 8-16 cores
- **RAM**: 16-32GB (+ heap для коннектора)
- **Storage**: 100-200GB SSD (быстрый доступ к metadata/logs)
- **Network**: 10 Gbit/s

### Требования к памяти (KAFKA_HEAP_OPTS)

Heap выделяется для:
- Worker JVM overhead: ~1GB
- S3 Sink Connector: зависит от задач
- Буферизация сообщений: 5-10MB × tasks.max

Формула: `-Xms<shared_buffers> -Xmx<effective_cache_size>`

**Примеры**:
- 1-2 tasks: `-Xms2g -Xmx4g`
- 4-6 tasks: `-Xms4g -Xmx8g`
- 8-12 tasks: `-Xms8g -Xmx16g` (Recommended)

### Сетевые требования
- **Latency** между Kafka и Kafka Connect: < 10ms
- **Bandwidth** для consumer: ~5-10 Mbps × tasks (зависит от throughput)
- **DNS**: Корректное разрешение Kafka brokers, Schema Registry, MinIO
- **Firewall**: Открыты порты 8083 (REST API) и inter-node communication (random ports)

### Софтовые требования
- **Docker**: 20.10+
- **Java**: встроена в image (опционально нужно для native коннекторов)
- **JMX**: опционально для мониторинга

---

## Production развертывание

### 1. Подготовка инфраструктуры

На каждом узле:

```bash
# Обновление системы
apt-get update && apt-get upgrade -y

# Установка Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Создание директорий
mkdir -p /opt/kafka-connect/data
mkdir -p /opt/kafka-connect/config
mkdir -p /opt/kafka-connect/logs
mkdir -p /opt/kafka-connect/connectors

# Права доступа
chmod 755 /opt/kafka-connect
```

### 2. Создание конфигурационных файлов

#### Переменные окружения: `/opt/kafka-connect/.env`

```bash
# Kafka brokers
KAFKA_BOOTSTRAP_SERVERS=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092

# Kafka Connect config
CONNECT_GROUP_ID=kafka-connect-cluster
CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect-1   # Изменить на hostname каждого узла
CONNECT_REST_PORT=8083
CONNECT_LISTENERS=HTTP://0.0.0.0:8083

# Offsets/Config/Status topics
CONNECT_CONFIG_STORAGE_TOPIC=_connect-configs
CONNECT_OFFSET_STORAGE_TOPIC=_connect-offsets
CONNECT_STATUS_STORAGE_TOPIC=_connect-status
CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=3
CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=3
CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=3

# Converters (Avro для производства)
CONNECT_KEY_CONVERTER=io.confluent.connect.avro.AvroConverter
CONNECT_VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter
CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081
CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Internal converters (JSON для metadata)
CONNECT_INTERNAL_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter
CONNECT_INTERNAL_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter
CONNECT_INTERNAL_KEY_CONVERTER_SCHEMAS_ENABLE=false
CONNECT_INTERNAL_VALUE_CONVERTER_SCHEMAS_ENABLE=false

# Плагины
CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components

# Performance
CONNECT_PRODUCER_COMPRESSION_TYPE=snappy
CONNECT_PRODUCER_MAX_REQUEST_SIZE=10485760
CONNECT_CONSUMER_MAX_POLL_RECORDS=5000
CONNECT_CONSUMER_MAX_PARTITION_FETCH_BYTES=10485760

# Worker properties
CONNECT_OFFSET_STORAGE_PARTITIONS=25
CONNECT_STATUS_STORAGE_PARTITIONS=5
CONNECT_OFFSET_FLUSH_INTERVAL_MS=10000

# Logging
CONNECT_LOG4J_ROOT_LOGLEVEL=INFO
CONNECT_LOG4J_LOGGERS=org.apache.kafka.connect.runtime.rest=WARN,org.reflections=ERROR

# Java/JVM
KAFKA_HEAP_OPTS="-Xms4g -Xmx8g"
```

### 3. Dockerfile для Kafka Connect

Использовать существующий Dockerfile из `data-lake/kafka-connect/Dockerfile`:

```dockerfile
FROM confluentinc/cp-kafka-connect:7.5.0

# Install Confluent S3 Sink Connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.0

# Switch to root to create system directories
USER root
RUN mkdir -p /data && chown appuser:appuser /data

# Switch back to non-privileged user
USER appuser
```

**Сборка image**:
```bash
cd /opt/kafka-connect
docker build -t kafka-connect:7.5.0 ./
# или использовать registry:
docker tag kafka-connect:7.5.0 registry.company.com/kafka-connect:7.5.0
docker push registry.company.com/kafka-connect:7.5.0
```

### 4. Docker запуск на КАЖДОМ узле

Создать `/opt/kafka-connect/docker-run.sh`:

```bash
#!/bin/bash

# Load environment
source /opt/kafka-connect/.env

# Stop if running
docker stop kafka-connect 2>/dev/null || true
docker rm kafka-connect 2>/dev/null || true

# Run container
docker run -d \
  --name kafka-connect \
  --restart unless-stopped \
  --network host \
  -e KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS} \
  -e CONNECT_GROUP_ID=${CONNECT_GROUP_ID} \
  -e CONNECT_REST_ADVERTISED_HOST_NAME=${CONNECT_REST_ADVERTISED_HOST_NAME} \
  -e CONNECT_REST_PORT=${CONNECT_REST_PORT} \
  -e CONNECT_LISTENERS=${CONNECT_LISTENERS} \
  -e CONNECT_CONFIG_STORAGE_TOPIC=${CONNECT_CONFIG_STORAGE_TOPIC} \
  -e CONNECT_OFFSET_STORAGE_TOPIC=${CONNECT_OFFSET_STORAGE_TOPIC} \
  -e CONNECT_STATUS_STORAGE_TOPIC=${CONNECT_STATUS_STORAGE_TOPIC} \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=${CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR} \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=${CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR} \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=${CONNECT_STATUS_STORAGE_REPLICATION_FACTOR} \
  -e CONNECT_KEY_CONVERTER=${CONNECT_KEY_CONVERTER} \
  -e CONNECT_VALUE_CONVERTER=${CONNECT_VALUE_CONVERTER} \
  -e CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL=${CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL} \
  -e CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL=${CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL} \
  -e CONNECT_INTERNAL_KEY_CONVERTER=${CONNECT_INTERNAL_KEY_CONVERTER} \
  -e CONNECT_INTERNAL_VALUE_CONVERTER=${CONNECT_INTERNAL_VALUE_CONVERTER} \
  -e CONNECT_INTERNAL_KEY_CONVERTER_SCHEMAS_ENABLE=${CONNECT_INTERNAL_KEY_CONVERTER_SCHEMAS_ENABLE} \
  -e CONNECT_INTERNAL_VALUE_CONVERTER_SCHEMAS_ENABLE=${CONNECT_INTERNAL_VALUE_CONVERTER_SCHEMAS_ENABLE} \
  -e CONNECT_PLUGIN_PATH=${CONNECT_PLUGIN_PATH} \
  -e CONNECT_PRODUCER_COMPRESSION_TYPE=${CONNECT_PRODUCER_COMPRESSION_TYPE} \
  -e CONNECT_PRODUCER_MAX_REQUEST_SIZE=${CONNECT_PRODUCER_MAX_REQUEST_SIZE} \
  -e CONNECT_CONSUMER_MAX_POLL_RECORDS=${CONNECT_CONSUMER_MAX_POLL_RECORDS} \
  -e CONNECT_CONSUMER_MAX_PARTITION_FETCH_BYTES=${CONNECT_CONSUMER_MAX_PARTITION_FETCH_BYTES} \
  -e CONNECT_OFFSET_STORAGE_PARTITIONS=${CONNECT_OFFSET_STORAGE_PARTITIONS:-25} \
  -e CONNECT_STATUS_STORAGE_PARTITIONS=${CONNECT_STATUS_STORAGE_PARTITIONS:-5} \
  -e CONNECT_OFFSET_FLUSH_INTERVAL_MS=${CONNECT_OFFSET_FLUSH_INTERVAL_MS:-10000} \
  -e CONNECT_LOG4J_ROOT_LOGLEVEL=${CONNECT_LOG4J_ROOT_LOGLEVEL} \
  -e CONNECT_LOG4J_LOGGERS="${CONNECT_LOG4J_LOGGERS}" \
  -e KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS}" \
  -v /opt/kafka-connect/data:/data \
  -v /opt/kafka-connect/logs:/var/log/kafka \
  kafka-connect:7.5.0

# Wait for startup
sleep 5
docker logs kafka-connect
```

**Запуск**:
```bash
chmod +x /opt/kafka-connect/docker-run.sh
/opt/kafka-connect/docker-run.sh

# Проверить
docker logs -f kafka-connect
curl http://localhost:8083/
```

### 5. Systemd Service (рекомендуется)

Создать `/etc/systemd/system/kafka-connect.service`:

```ini
[Unit]
Description=Kafka Connect Distributed Worker
Documentation=https://docs.confluent.io/platform/current/connect/index.html
After=docker.service
Requires=docker.service

[Service]
Type=simple
User=root
EnvironmentFile=/opt/kafka-connect/.env
ExecStartPre=-/usr/bin/docker stop kafka-connect
ExecStartPre=-/usr/bin/docker rm kafka-connect
ExecStart=/opt/kafka-connect/docker-run.sh
ExecStop=/usr/bin/docker stop kafka-connect
Restart=always
RestartSec=10

# Health check
ExecHealthCheck=/usr/bin/docker exec kafka-connect curl -f http://localhost:8083/

[Install]
WantedBy=multi-user.target
```

**Активация**:
```bash
systemctl daemon-reload
systemctl enable kafka-connect
systemctl start kafka-connect
systemctl status kafka-connect
```

### 6. Проверка кластера

```bash
# REST API доступен?
curl http://localhost:8083/

# Statuses всех узлов
curl http://localhost:8083/connectors

# Информация о worker
curl http://localhost:8083/ | jq

# Плагины установлены?
curl http://localhost:8083/connector-plugins | jq '.[] | .class' | grep s3
```

---

## Конфигурация

### Переменные окружения

| Переменная | Описание | Обязательно | Пример |
|------------|----------|-------------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers для метаданных | Да | `broker1:9092,broker2:9092,broker3:9092` |
| `CONNECT_GROUP_ID` | ID группы (worker cluster) | Да | `kafka-connect-cluster` |
| `CONNECT_REST_ADVERTISED_HOST_NAME` | Hostname для REST API | Да | `kafka-connect-1` |
| `CONNECT_REST_PORT` | Port для REST API | Да | `8083` |
| `CONNECT_CONFIG_STORAGE_TOPIC` | Topic для конфиг | Да | `_connect-configs` |
| `CONNECT_OFFSET_STORAGE_TOPIC` | Topic для offset | Да | `_connect-offsets` |
| `CONNECT_STATUS_STORAGE_TOPIC` | Topic для статуса | Да | `_connect-status` |
| `CONNECT_*_STORAGE_REPLICATION_FACTOR` | Replication factor | Да | `3` |
| `CONNECT_KEY_CONVERTER` | Converter для ключей | Да | `io.confluent.connect.avro.AvroConverter` |
| `CONNECT_VALUE_CONVERTER` | Converter для значений | Да | `io.confluent.connect.avro.AvroConverter` |
| `CONNECT_*_CONVERTER_SCHEMA_REGISTRY_URL` | Schema Registry URL | Да | `http://schema-registry:8081` |
| `CONNECT_PLUGIN_PATH` | Путь к плагинам | Да | `/usr/share/java,/usr/share/confluent-hub-components` |
| `KAFKA_HEAP_OPTS` | JVM heap | Рекомендуется | `-Xms4g -Xmx8g` |
| `CONNECT_PRODUCER_MAX_REQUEST_SIZE` | Max request size | Рекомендуется | `10485760` (10MB) |
| `CONNECT_CONSUMER_MAX_POLL_RECORDS` | Batch size | Рекомендуется | `5000` |

### Consumer Overrides

Для оптимизации Kafka Consumer (S3 Sink читает из topics):

```bash
CONNECT_CONSUMER_OVERRIDE_MAX_POLL_RECORDS=5000
CONNECT_CONSUMER_OVERRIDE_MAX_PARTITION_FETCH_BYTES=10485760
CONNECT_CONSUMER_OVERRIDE_SESSION_TIMEOUT_MS=30000
CONNECT_CONSUMER_OVERRIDE_AUTO_OFFSET_RESET=earliest
```

---

## S3 Sink Коннектор

### Создание коннектора через REST API

#### Пример конфигурации для order-events

Использовать JSON из `data-lake/kafka-connect/connectors/s3-sink-order-events.json`:

```json
{
  "name": "s3-sink-order-events",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "12",
    "topics": "order-events",

    "s3.bucket.name": "datalake",
    "s3.region": "us-east-1",
    "store.url": "http://minio:9000",
    "s3.path.style.access.enabled": "true",
    "s3.part.size": "5242880",

    "aws.access.key.id": "minioadmin",
    "aws.secret.access.key": "minioadmin",

    "flush.size": "50000",
    "rotate.interval.ms": "300000",
    "rotate.schedule.interval.ms": "3600000",

    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "parquet.codec": "snappy",

    "schema.compatibility": "BACKWARD",
    "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",

    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'calc_id='yyyyMMdd-HHmmss'/dt='yyyy-MM-dd'/hour='HH",
    "partition.duration.ms": "3600000",
    "locale": "en-US",
    "timezone": "UTC",
    "timestamp.extractor": "Record",

    "topics.dir": "topics",
    "directory.delim": "/",

    "behavior.on.null.values": "ignore",
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "consumer.override.max.poll.records": "5000",
    "consumer.override.max.partition.fetch.bytes": "10485760",
    "consumer.override.session.timeout.ms": "30000",
    "consumer.override.auto.offset.reset": "earliest"
  }
}
```

#### Развертывание коннектора

**Via REST API**:
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/kafka-connect/connectors/s3-sink-order-events.json

# Проверить статус
curl http://localhost:8083/connectors/s3-sink-order-events/status | jq

# Вывод успешного статуса:
# {
#   "name": "s3-sink-order-events",
#   "connector": {
#     "state": "RUNNING",
#     "worker_id": "kafka-connect-1:8083"
#   },
#   "tasks": [
#     {"id": 0, "state": "RUNNING", "worker_id": "kafka-connect-1:8083"},
#     {"id": 1, "state": "RUNNING", "worker_id": "kafka-connect-2:8083"},
#     ...
#   ]
# }
```

**Или сохранить в файл и запустить**:
```bash
# Создать файл конфигурации
cp /path/to/s3-sink-order-events.json /opt/kafka-connect/connectors/s3-sink-order-events.json

# Создать через скрипт
#!/bin/bash
CONNECTOR_FILE="/opt/kafka-connect/connectors/s3-sink-order-events.json"
CONNECT_HOST="kafka-connect-1"
CONNECT_PORT="8083"

curl -X POST http://${CONNECT_HOST}:${CONNECT_PORT}/connectors \
  -H "Content-Type: application/json" \
  -d @${CONNECTOR_FILE}
```

### Параметры S3 Sink

| Параметр | Описание | Пример | Обязательно |
|----------|----------|--------|-------------|
| `topics` | Kafka topics для подписки | `order-events,payment-events` | Да |
| `tasks.max` | Максимум tasks | `12` | Да |
| `s3.bucket.name` | Bucket name | `datalake` | Да |
| `store.url` | MinIO S3 endpoint | `http://minio:9000` | Да (для MinIO) |
| `aws.access.key.id` | Access Key | `minioadmin` | Да |
| `aws.secret.access.key` | Secret Key | `minioadmin` | Да |
| `s3.part.size` | Part size for multipart upload | `5242880` (5MB) | Нет (default 5MB) |
| `flush.size` | Records before flush | `50000` | Да |
| `rotate.interval.ms` | Rotate interval | `300000` (5 min) | Нет |
| `rotate.schedule.interval.ms` | Rotate по расписанию | `3600000` (1 hour) | Нет |
| `parquet.codec` | Compression | `snappy` | Нет |
| `partition.duration.ms` | Partition duration | `3600000` (1 hour) | Да (для time-based) |
| `path.format` | Path template | `'calc_id='yyyyMMdd-HHmmss'/dt='yyyy-MM-dd'/hour='HH` | Да |

### Partitioning стратегия

В примере используется **TimeBasedPartitioner**:
```
s3://datalake/topics/order-events/
  calc_id=20241225-143050/
    dt=2024-12-25/
      hour=14/
        order-events+0+0000000000.parquet
        order-events+0+0000050000.parquet
      hour=15/
        order-events+0+0000100000.parquet
```

**Альтернативные partitioner'ы**:
- `DefaultPartitioner` - partition по Kafka partitions (простая структура)
- `FieldPartitioner` - по полям в значении события (нужна field config)

---

## Высокая доступность

### Distributed Mode (2-3 узла)

Kafka Connect в distributed режиме обеспечивает:
- **Автоматический балансинг**: задачи распределяются между узлами
- **Отказоустойчивость**: при падении узла его задачи мигрируют на другие
- **Масштабируемость**: добавление узлов автоматически балансирует нагрузку
- **State Management**: состояние хранится в Kafka topics

#### Конфигурация кластера из 3 узлов

На каждом узле разные `CONNECT_REST_ADVERTISED_HOST_NAME`:

```bash
# kafka-connect-1
CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect-1

# kafka-connect-2
CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect-2

# kafka-connect-3
CONNECT_REST_ADVERTISED_HOST_NAME=kafka-connect-3
```

#### Member Discovery

Узлы находят друг друга через общий `CONNECT_GROUP_ID`:

```bash
# Все узлы в одной группе
CONNECT_GROUP_ID=kafka-connect-cluster

# Узлы коннектятся к одному Kafka cluster'у
KAFKA_BOOTSTRAP_SERVERS=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092

# Через Kafka metadata узлы узнают друг о друге
```

#### Проверка кластера

```bash
# Посмотреть все узлы в группе
curl http://kafka-connect-1:8083/ | jq

# Вывод:
# {
#   "version": "7.5.0",
#   "commit": "...",
#   "kafka_cluster_id": "...",
#   "connect": {
#     "status": "HEALTHY"
#   }
# }

# Посмотреть распределение задач
curl http://kafka-connect-1:8083/connectors/s3-sink-order-events/status | jq '.tasks'

# Посмотреть config topic
kafka-console-consumer.sh --bootstrap-servers kafka-broker-1:9092 \
  --topic _connect-configs \
  --from-beginning \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

#### Failover поведение

```bash
# Scenario: Node 1 has tasks 0-3, Node 2 has tasks 4-7, Node 3 has tasks 8-11

# 1. Node 1 падает
systemctl stop kafka-connect

# 2. Другие узлы (Node 2, 3) детектят отсутствие Node 1
# 3. Координатор (один из Node 2/3) триггерит rebalance
# 4. Tasks перераспределяются:
#    - Node 2: tasks 0-3 (было 4-7) + tasks 4-7 = 0-7
#    - Node 3: tasks 8-11 (неизменно)

# 5. Node 1 восстанавливается
systemctl start kafka-connect

# 6. Новый rebalance: задачи переделятся равномерно
#    - Node 1: tasks 0-3
#    - Node 2: tasks 4-7
#    - Node 3: tasks 8-11
```

### Load Balancing (опционально)

Для балансировки REST API запросов используется Nginx/HAProxy:

```nginx
upstream kafka_connect {
    least_conn;
    server kafka-connect-1:8083;
    server kafka-connect-2:8083;
    server kafka-connect-3:8083;
}

server {
    listen 8083;
    server_name kafka-connect.company.com;

    location / {
        proxy_pass http://kafka_connect;
        proxy_set_header Host $http_host;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }
}
```

---

## Мониторинг

### JMX Metrics

Kafka Connect экспортирует JMX метрики. Включить JMX мониторинг:

```bash
# В .env добавить
KAFKA_JMX_PORT=9999
KAFKA_JMX_HOSTNAME=kafka-connect-1
```

Docker запуск с JMX:
```bash
docker run -d \
  ... \
  -e KAFKA_JMX_PORT=9999 \
  -e KAFKA_JMX_HOSTNAME=$(hostname -I | awk '{print $1}') \
  -p 9999:9999 \
  kafka-connect:7.5.0
```

### Prometheus JMX Exporter

Использовать jmx_exporter для преобразования JMX в Prometheus метрики:

```yaml
# /opt/kafka-connect/jmx-exporter-config.yml
lowercaseOutputName: true
lowercaseOutputLabelNames: true

whitelistObjectNames:
  - "kafka.connect:type=connect-worker-metrics,*"
  - "kafka.connect:type=connect-task-metrics,*"
  - "kafka.connect:type=sink-task-metrics,*"
  - "kafka.connect:type=source-task-metrics,*"

rules:
  - pattern: "kafka.connect<type=(.+)><(.+)=(.+)><(.+)=(.+)><(.+)>(.+):"
    name: kafka_connect_$1_$6_$7
    labels:
      "$2": "$3"
      "$4": "$5"
  - pattern: "kafka.connect<type=(.+)><(.+)=(.+)><(.+)>(.+):"
    name: kafka_connect_$1_$4_$5
    labels:
      "$2": "$3"
```

```bash
docker run -d \
  --name jmx-exporter \
  -p 5556:5556 \
  -v /opt/kafka-connect/jmx-exporter-config.yml:/etc/jmx_exporter/config.yml \
  sscaling/jmx-exporter \
  5556 /etc/jmx_exporter/config.yml
```

### REST API Metrics

Kafka Connect предоставляет встроенные метрики через REST API:

```bash
# Connector metrics
curl http://localhost:8083/connectors/s3-sink-order-events | jq

# Task metrics
curl http://localhost:8083/connectors/s3-sink-order-events/tasks | jq

# Status и статистика
curl http://localhost:8083/connectors/s3-sink-order-events/status | jq
```

### Ключевые метрики для мониторинга

| Метрика | Значение | Threshold | Alert |
|---------|----------|-----------|-------|
| `connect_task_status` | Running/Paused/Failed | Running | Failed |
| `connect_connector_status` | Running/Paused/Failed | Running | Failed |
| `kafka_connect_connector_batch_size_total` | Количество обработанных records | Monitor | - |
| `kafka_connect_connector_offset_committed_total` | Committed offsets | Monitor | - |
| `kafka_connect_task_put_batch_total_records_sent` | Records sent to sink | > 0 | Stuck |
| `kafka_connect_task_put_batch_total_duration_us` | Duration per batch | < 30s | Slow |
| `jvm_memory_used_bytes` | Used memory | < heap * 0.8 | High |
| `jvm_threads_live` | Live threads | < 1000 | Leak |

### Prometheus Scrape Config

```yaml
scrape_configs:
  - job_name: 'kafka-connect'
    static_configs:
      - targets:
        - kafka-connect-1:8083
        - kafka-connect-2:8083
        - kafka-connect-3:8083
    scrape_interval: 30s

  - job_name: 'kafka-connect-jmx'
    static_configs:
      - targets:
        - kafka-connect-1:5556
        - kafka-connect-2:5556
        - kafka-connect-3:5556
    scrape_interval: 30s
```

### Grafana Dashboard

Создать dashboard с metrics:
- Connector status (Running/Failed)
- Task distribution across workers
- Records per second (throughput)
- Latency (put batch duration)
- JVM heap usage
- Thread count

---

## Операции

### Запуск/Остановка коннектора

```bash
# Получить список коннекторов
curl http://localhost:8083/connectors

# Получить конфиг коннектора
curl http://localhost:8083/connectors/s3-sink-order-events/config | jq

# Остановить коннектор (паузировать)
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/pause

# Возобновить коннектор
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/resume

# Удалить коннектор
curl -X DELETE http://localhost:8083/connectors/s3-sink-order-events

# Перезагрузить коннектор
curl -X POST http://localhost:8083/connectors/s3-sink-order-events/restart
```

### Изменение конфигурации коннектора

```bash
# Увеличить количество tasks (parallelize)
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/config \
  -H "Content-Type: application/json" \
  -d '{"tasks.max": "16"}'

# Изменить flush size
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/config \
  -H "Content-Type: application/json" \
  -d '{"flush.size": "100000"}'

# Полная переконфигурация
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/config \
  -H "Content-Type: application/json" \
  -d @/opt/kafka-connect/connectors/s3-sink-order-events.json
```

### Создание нового коннектора

```bash
# Для нового topic, например payment-events
cat > /opt/kafka-connect/connectors/s3-sink-payment-events.json <<'EOF'
{
  "name": "s3-sink-payment-events",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "8",
    "topics": "payment-events",
    "s3.bucket.name": "datalake",
    "s3.region": "us-east-1",
    "store.url": "http://minio:9000",
    "s3.path.style.access.enabled": "true",
    "aws.access.key.id": "minioadmin",
    "aws.secret.access.key": "minioadmin",
    "flush.size": "40000",
    "rotate.interval.ms": "300000",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "parquet.codec": "snappy",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'payment_id='yyyyMMdd-HHmmss'/dt='yyyy-MM-dd'/hour='HH",
    "partition.duration.ms": "3600000",
    "timezone": "UTC",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
EOF

# Создать коннектор
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/kafka-connect/connectors/s3-sink-payment-events.json

# Проверить статус
curl http://localhost:8083/connectors/s3-sink-payment-events/status | jq
```

### Масштабирование (Scaling)

#### Горизонтальное масштабирование (добавление узлов)

```bash
# 1. На новом узле выполнить шаги из "Production развертывание"
# 2. Убедиться, что используется одинаковый CONNECT_GROUP_ID
# 3. Запустить Kafka Connect на новом узле
systemctl start kafka-connect

# 4. Автоматический rebalance произойдет в течение 30 сек
# Проверить статус
curl http://kafka-connect-4:8083/connectors/s3-sink-order-events/status | jq

# 5. Tasks будут переделены между узлами
```

#### Вертикальное масштабирование (увеличение tasks на коннекторе)

```bash
# Увеличить tasks для параллелизма
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/config \
  -H "Content-Type: application/json" \
  -d '{"tasks.max": "24"}'

# Новые tasks будут созданы и распределены между узлами
# Проверить
curl http://localhost:8083/connectors/s3-sink-order-events/status | jq '.tasks | length'
```

#### Удаление узла (decommission)

```bash
# 1. На узле, который нужно удалить
systemctl stop kafka-connect

# 2. Его задачи мигрируют на другие узлы
# 3. Дождаться завершения migration (смотреть логи)
# 4. Удалить контейнер и данные
docker rm -f kafka-connect
rm -rf /opt/kafka-connect/data

# 5. Убедиться, что все tasks снова Running
curl http://kafka-connect-1:8083/connectors/s3-sink-order-events/status | jq
```

### Upgrade Kafka Connect

```bash
# 1. Создать backup конфигураций коннекторов
mkdir -p /opt/kafka-connect/backups/$(date +%Y%m%d)
curl http://localhost:8083/connectors > /opt/kafka-connect/backups/$(date +%Y%m%d)/connectors.json

# 2. Остановить все узлы (с интервалом, чтобы другие узлы приняли задачи)
for node in kafka-connect-1 kafka-connect-2 kafka-connect-3; do
  ssh $node systemctl stop kafka-connect
  sleep 30
done

# 3. Обновить Docker image
docker pull confluentinc/cp-kafka-connect:7.6.0
docker tag confluentinc/cp-kafka-connect:7.6.0 kafka-connect:7.6.0

# 4. Обновить VERSION в docker-run.sh или Dockerfile

# 5. Запустить все узлы
for node in kafka-connect-1 kafka-connect-2 kafka-connect-3; do
  ssh $node systemctl start kafka-connect
  sleep 30
done

# 6. Проверить, что коннекторы восстановились
curl http://localhost:8083/connectors | jq
```

---

## Troubleshooting

### Connector в состоянии FAILED

```bash
# Посмотреть ошибку
curl http://localhost:8083/connectors/s3-sink-order-events/status | jq '.tasks[0].trace'

# Посмотреть логи контейнера
docker logs kafka-connect | tail -100

# Частые причины:
# 1. MinIO недоступен
curl http://minio:9000

# 2. Schema Registry недоступен
curl http://schema-registry:8081

# 3. Неправильный access key/secret в конфиге
# Проверить MinIO credentials

# 4. Broker недоступен
kafka-broker-api-versions.sh --bootstrap-server kafka-broker-1:9092

# 5. Parquet schema issue
# Проверить Schema Registry https://schema-registry:8081/subjects
```

### Task зависает (stuck, не обрабатывает данные)

```bash
# 1. Проверить, есть ли данные в topic
kafka-console-consumer.sh --bootstrap-server kafka-broker-1:9092 \
  --topic order-events \
  --from-beginning \
  --max-messages 1

# 2. Проверить, что consumer группа продвигается
kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9092 \
  --group kafka-connect-cluster \
  --describe

# 3. Проверить JVM logs на OOM
docker logs kafka-connect | grep -i "java.lang.OutOfMemoryError"

# 4. Увеличить heap если нужно
KAFKA_HEAP_OPTS="-Xms8g -Xmx16g"  # Было -Xms4g -Xmx8g

# 5. Перезагрузить task
curl -X POST http://localhost:8083/connectors/s3-sink-order-events/tasks/0/restart
```

### Высокая latency / slow writing

```bash
# 1. Проверить flush.size и rotate settings
curl http://localhost:8083/connectors/s3-sink-order-events/config | jq '.flush.size'

# 2. Посмотреть метрики
curl http://localhost:8083/connectors/s3-sink-order-events | jq '.tasks'

# 3. Проверить network latency к MinIO
ping -c 5 minio
nc -zv minio 9000

# 4. Проверить disk I/O на MinIO
# На MinIO сервере: iostat -x 1

# 5. Проверить S3 performance
# Увеличить s3.part.size для больших файлов
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/config \
  -H "Content-Type: application/json" \
  -d '{"s3.part.size": "10485760"}'

# 6. Проверить consumer.override settings
curl http://localhost:8083/connectors/s3-sink-order-events/config | \
  jq '.consumer'
```

### MinIO connection issues

```bash
# 1. Проверить доступность MinIO
curl -v http://minio:9000

# 2. Проверить credentials
# AWS CLI test
aws s3 ls s3://datalake/ \
  --endpoint-url http://minio:9000 \
  --region us-east-1

# 3. Проверить bucket существует
curl -H "Authorization: AWS minioadmin:minioadmin" \
  http://minio:9000/datalake/

# 4. Проверить permissions на bucket
# Нужны rights: PutObject, GetObject, ListBucket

# 5. Проверить path.style.access (обязательно для MinIO)
# s3.path.style.access.enabled: "true"

# 6. Проверить Region
# Должно совпадать с MINIO_REGION_NAME на MinIO сервере
```

### Schema Registry issues

```bash
# 1. Проверить Schema Registry доступен
curl http://schema-registry:8081/

# 2. Проверить что schema существует
curl http://schema-registry:8081/subjects

# 3. Проверить конкретный subject
curl http://schema-registry:8081/subjects/order-events-value/versions

# 4. Если ошибка "Schema not found"
# Проверить что producer публикует с Avro
# Или зарегистрировать schema вручную

# 5. Если ошибка compatibility
# Проверить compatibility mode
curl http://schema-registry:8081/config | jq '.compatibilityLevel'
```

### Memory leaks / OOM

```bash
# 1. Проверить текущее heap usage
curl http://localhost:8083/ | jq '.java_version' # или через JMX

# 2. Включить heap dumps
KAFKA_HEAP_OPTS="-Xms8g -Xmx16g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/opt/kafka-connect/heap-dumps"

# 3. Посмотреть логи на OOM
docker logs kafka-connect | grep -i "OutOfMemory"

# 4. Уменьшить batch size если большой
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/config \
  -H "Content-Type: application/json" \
  -d '{"consumer.override.max.poll.records": "3000"}'

# 5. Уменьшить количество tasks если много памяти занимают buffer'ы
```

### Worker node crash / restart

```bash
# 1. При restart автоматический failover
# Задачи мигрируют на другие узлы
# Offset'ы сохранены в Kafka, данные не потеряны

# 2. Проверить что tasks перенеслись
curl http://kafka-connect-2:8083/connectors/s3-sink-order-events/status | jq '.tasks'

# 3. После restart того же узла
systemctl start kafka-connect

# 4. Произойдет новый rebalance
# Tasks могут вернуться на этот узел

# 5. Проверить health
curl http://localhost:8083/
```

---

## Дополнительные ресурсы

- [Confluent Kafka Connect Documentation](https://docs.confluent.io/platform/current/connect/index.html)
- [Confluent S3 Sink Connector](https://docs.confluent.io/kafka-connect-s3/current/index.html)
- [Kafka Connect REST API](https://docs.confluent.io/platform/current/connect/references/restapi.html)
- [Apache Kafka GitHub](https://github.com/apache/kafka)
- [MinIO S3 Compatibility](https://min.io/docs/minio/linux/integrations/kafka-connect-minio-quickstart.html)
