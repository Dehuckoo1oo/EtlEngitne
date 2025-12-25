# Kafka Connect - MVP Deployment

## Назначение

Сервис для выгрузки событий из Kafka в MinIO в формате Parquet через S3 Sink Connector.
MVP: один worker, без кластера и балансировщиков.

---

## Требования к машине

| Параметр | Значение |
|----------|----------|
| CPU | 8 cores |
| RAM | 16 GB |
| Storage | 100 GB SSD |
| Network | 10 Gbit/s |
| Hostname | kafka-connect.company.com |

---

## GitLab структура

```
kafka-connect/
├── Dockerfile
├── config/
│   └── connect-distributed.properties
├── connectors/
│   └── s3-sink-order-events.json
├── .env.example
├── .gitlab-ci.yml
└── README.md
```

---

## Dockerfile

`kafka-connect/Dockerfile`:

```dockerfile
FROM confluentinc/cp-kafka-connect:7.5.0

USER root

# Установить S3 Sink Connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.0

# Скопировать конфиги
COPY config/connect-distributed.properties /etc/kafka/connect-distributed.properties
COPY connectors/ /opt/connectors/

USER appuser

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8083/ || exit 1

EXPOSE 8083
```

---

## Конфигурационные файлы

### connect-distributed.properties

`config/connect-distributed.properties`:

```properties
bootstrap.servers=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
group.id=kafka-connect-mvp

rest.port=8083
rest.advertised.host.name=kafka-connect.company.com

config.storage.topic=_connect-configs
offset.storage.topic=_connect-offsets
status.storage.topic=_connect-status
config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3

key.converter=io.confluent.connect.avro.AvroConverter
value.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://schema-registry.company.com:8081
value.converter.schema.registry.url=http://schema-registry.company.com:8081

internal.key.converter=org.apache.kafka.connect.json.JsonConverter
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false

plugin.path=/usr/share/java,/usr/share/confluent-hub-components

# Performance (2+ TB)
offset.flush.interval.ms=10000
consumer.max.poll.records=5000
consumer.max.partition.fetch.bytes=10485760
consumer.fetch.max.bytes=52428800
producer.max.request.size=10485760
producer.compression.type=snappy
```

### S3 Sink Connector

`connectors/s3-sink-order-events.json`:

```json
{
  "name": "s3-sink-order-events",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "12",
    "topics": "order-events",

    "s3.bucket.name": "datalake",
    "s3.region": "us-east-1",
    "store.url": "https://minio.company.com:9000",
    "s3.path.style.access.enabled": "true",
    "s3.part.size": "67108864",

    "aws.access.key.id": "<S3_ACCESS_KEY>",
    "aws.secret.access.key": "<S3_SECRET_KEY>",

    "flush.size": "100000",
    "rotate.interval.ms": "600000",
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
    "value.converter.schema.registry.url": "http://schema-registry.company.com:8081",

    "consumer.override.max.poll.records": "5000",
    "consumer.override.max.partition.fetch.bytes": "20971520",
    "consumer.override.fetch.max.bytes": "52428800",
    "consumer.override.auto.offset.reset": "earliest"
  }
}
```

---

## Kafka topics (2+ TB)

Конфигурация топиков обязательна для стабильной работы на объёмах 2+ TB.
См. рекомендации в [doc/datalake/README.md](README.md).

---

## Environment Variables

`.env.example`:

```bash
# === Kafka Connect JVM ===
KAFKA_HEAP_OPTS="-Xms8g -Xmx8g"
```

---

## Build & Deploy

### Вариант 1: Вручную

```bash
# На машине kafka-connect.company.com

# 1. Клонировать репозиторий
git clone https://gitlab.company.com/datalake/infrastructure.git
cd infrastructure/kafka-connect

# 2. Build образа
docker build -t kafka-connect-datalake:latest .

# 3. Создать .env
cp .env.example .env
nano .env  # Установить heap при необходимости

# 4. Запуск контейнера
docker run -d \
  --name kafka-connect \
  --restart unless-stopped \
  -p 8083:8083 \
  --env-file .env \
  kafka-connect-datalake:latest

# 5. Health check
curl -f http://kafka-connect.company.com:8083/
```

### Вариант 2: GitLab CI/CD

`.gitlab-ci.yml`:

```yaml
stages:
  - build
  - deploy

variables:
  IMAGE_TAG: ${CI_COMMIT_REF_NAME}-${CI_COMMIT_SHORT_SHA}
  REGISTRY: registry.company.com
  TARGET_HOST: kafka-connect.company.com

build:
  stage: build
  script:
    - docker build -t ${REGISTRY}/kafka-connect-datalake:${IMAGE_TAG} .
    - docker tag ${REGISTRY}/kafka-connect-datalake:${IMAGE_TAG} ${REGISTRY}/kafka-connect-datalake:latest
    - docker push ${REGISTRY}/kafka-connect-datalake:${IMAGE_TAG}
    - docker push ${REGISTRY}/kafka-connect-datalake:latest
  only:
    - main

deploy:
  stage: deploy
  script:
    - ssh deploy@${TARGET_HOST} "docker pull ${REGISTRY}/kafka-connect-datalake:latest"
    - ssh deploy@${TARGET_HOST} "docker stop kafka-connect || true && docker rm kafka-connect || true"
    - ssh deploy@${TARGET_HOST} "docker run -d --name kafka-connect --restart unless-stopped -p 8083:8083 --env-file /opt/kafka-connect/.env ${REGISTRY}/kafka-connect-datalake:latest"
  only:
    - main
  when: manual
```

---

## Развертывание коннектора

После запуска сервиса применить конфиг:

```bash
curl -X POST http://kafka-connect.company.com:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/s3-sink-order-events.json
```

Проверить статус:

```bash
curl http://kafka-connect.company.com:8083/connectors/s3-sink-order-events/status
```

---

## Health Check

```bash
curl -f http://kafka-connect.company.com:8083/
docker inspect kafka-connect | grep -A 5 Health
```

---

## Следующий шаг

После успешного запуска Kafka Connect переходите к:
👉 [Trino](trino.md)
