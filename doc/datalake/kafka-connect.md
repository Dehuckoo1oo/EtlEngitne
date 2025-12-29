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
variables:
  GIT_STRATEGY: clone

stages:
  - deploy

Deploy Kafka Connect to TEST:
  stage: deploy
  tags: [your_runner_tag]
  needs: []
  when: manual
  allow_failure: false
  before_script:
    - SRV_APP="kafka-connect.company.com"
    - KAFKA_BOOTSTRAP="kafka-broker1:9092,kafka-broker2:9092,kafka-broker3:9092"
  script:
    - |
      # Сборка Docker образа
      echo "Собираем Docker образ..."
      docker build -t kafka-connect-datalake:${CI_COMMIT_SHORT_SHA} .
      docker tag kafka-connect-datalake:${CI_COMMIT_SHORT_SHA} kafka-connect-datalake:latest

      # Создаем переменную с названием образа
      ImageName=kafka-connect-datalake:latest

      # Создаем переменную с названием контейнера
      ContainerName=kafka-connect

      # Создаем скрипт деплоя
      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo 'Останавливаем и удаляем старый контейнер...'
      docker stop ${ContainerName} && docker rm ${ContainerName} && echo 'Старый контейнер остановлен и удален.' || echo 'Старого контейнера нет, останавливать нечего.'

      echo 'Создаем новый контейнер...'
      docker run \
        -d \
        --name ${ContainerName} \
        --restart=always \
        -e CONNECT_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP}" \
        -e CONNECT_REST_ADVERTISED_HOST_NAME="${SRV_APP}" \
        -e CONNECT_GROUP_ID="datalake-connect-cluster" \
        -e CONNECT_CONFIG_STORAGE_TOPIC="datalake-connect-configs" \
        -e CONNECT_OFFSET_STORAGE_TOPIC="datalake-connect-offsets" \
        -e CONNECT_STATUS_STORAGE_TOPIC="datalake-connect-status" \
        -e CONNECT_KEY_CONVERTER="org.apache.kafka.connect.storage.StringConverter" \
        -e CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
        -e CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE="false" \
        -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
        -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
        -p 8083:8083 \
        -h ${SRV_APP} \
        ${ImageName}

      echo "=========================================================================================="
      echo 'ГОТОВО!'
      echo "=========================================================================================="
      echo 'Проверяем состояние контейнера:'
      sleep 10
      docker ps -a --filter name=${ContainerName}
      echo '------------------------------------------------------------------------------------------'
      echo 'Логи контейнера:'
      docker logs ${ContainerName}
      echo '------------------------------------------------------------------------------------------'
      echo 'Проверяем доступность REST API:'
      sleep 5
      curl -f http://localhost:8083/ || echo 'ВНИМАНИЕ: REST API еще не доступен. Дождитесь полной инициализации.'
      echo '------------------------------------------------------------------------------------------'

      DEPLOY_SCRIPT

      echo "Копируем конфигурационные файлы на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"
      rsync -avz ./ svc_user@${SRV_APP}:~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}

      # Экспортируем и копируем Docker образ на целевой сервер
      echo "Экспортируем Docker образ..."
      docker save kafka-connect-datalake:latest | gzip > kafka-connect-latest.tar.gz

      echo "Копируем образ на ${SRV_APP}..."
      rsync -avz ./kafka-connect-latest.tar.gz svc_user@${SRV_APP}:~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/

      echo "Загружаем образ на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "cd ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/ && \
        docker load < kafka-connect-latest.tar.gz"

      echo "Запускаем скрипт деплоя на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "cd ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/ && \
        chmod u+x ./build.sh && ./build.sh"

      echo "Удаляем временные файлы с ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"
```

**Настройка переменных окружения в GitLab**:

В настройках CI/CD вашего проекта GitLab (`Settings > CI/CD > Variables`) добавьте:

| Переменная | Значение | Тип |
|-----------|----------|-----|
| `AWS_ACCESS_KEY_ID` | `kafka-connect` | Variable |
| `AWS_SECRET_ACCESS_KEY` | `<password_from_minio>` | Variable (Masked) |

**Примечание**:
- Замените `your_runner_tag` на тег вашего GitLab Runner
- Замените `svc_user` на пользователя для SSH подключения
- Укажите корректные адреса Kafka брокеров в `KAFKA_BOOTSTRAP`
- Docker образ копируется на целевой сервер для изоляции от registry

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
