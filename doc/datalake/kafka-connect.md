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
FROM registry.company.com/confluentinc/cp-kafka-connect:7.5.0

USER root

# Скачать и установить S3 Sink Connector из корпоративного репозитория
RUN curl -o /tmp/confluentinc-kafka-connect-s3-11.0.8.zip \
    <NEXUS_URL>/confluentinc-kafka-connect-s3-11.0.8.zip && \
    unzip /tmp/confluentinc-kafka-connect-s3-11.0.8.zip -d /usr/share/confluent-hub-components/ && \
    rm /tmp/confluentinc-kafka-connect-s3-11.0.8.zip

# Скопировать конфиги
COPY config/connect-distributed.properties /etc/kafka/connect-distributed.properties
COPY connectors/ /opt/connectors/

USER appuser

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8083/ || exit 1

EXPOSE 8083
```

**Примечание**: В основном Dockerfile используется установка коннектора версии 11.0.8 из корпоративного Nexus/Artifactory.

**Подготовка:** Замените `<NEXUS_URL>` на URL вашего корпоративного репозитория. Например:
- `https://nexus.company.com/repository/raw/kafka-connectors`
- `https://artifactory.company.com/artifactory/libs-release`

**Альтернативные способы установки:**

1. **Через confluent-hub** (если доступен в корпоративной сети):
```dockerfile
# Установить S3 Sink Connector через confluent-hub
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:11.0.8
```

2. **Из локального архива** (если скачали архив заранее):
```dockerfile
# Скопировать локальный архив и распаковать
COPY connectors/confluentinc-kafka-connect-s3-11.0.8.zip /tmp/
RUN unzip /tmp/confluentinc-kafka-connect-s3-11.0.8.zip -d /usr/share/confluent-hub-components/ && \
    rm /tmp/confluentinc-kafka-connect-s3-11.0.8.zip
```

---

## Конфигурационные файлы

### connect-distributed.properties

`config/connect-distributed.properties`:

```properties
# === Подключение к Kafka кластеру ===

# Список брокеров Kafka для подключения (запятая как разделитель)
bootstrap.servers=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092

# Уникальный идентификатор группы Kafka Connect воркеров
# Все воркеры с одинаковым group.id формируют единый кластер
group.id=kafka-connect-mvp

# === REST API настройки ===

# Порт для REST API Kafka Connect
rest.port=8083

# Имя хоста, которое будет анонсироваться другим воркерам и клиентам
rest.advertised.host.name=kafka-connect.company.com

# === Внутренние топики для хранения состояния ===

# Топик для хранения конфигураций коннекторов
config.storage.topic=_connect-configs

# Топик для хранения offset'ов (смещений) коннекторов
offset.storage.topic=_connect-offsets

# Топик для хранения статусов tasks и коннекторов
status.storage.topic=_connect-status

# Фактор репликации для топика с конфигурациями (3 для высокой доступности)
config.storage.replication.factor=3

# Фактор репликации для топика с offset'ами (3 для отказоустойчивости)
offset.storage.replication.factor=3

# Фактор репликации для топика со статусами (3 для надежности)
status.storage.replication.factor=3

# === Конвертеры данных для коннекторов ===

# Конвертер для ключей сообщений из Kafka (Avro формат)
key.converter=io.confluent.connect.avro.AvroConverter

# Конвертер для значений сообщений из Kafka (Avro формат)
value.converter=io.confluent.connect.avro.AvroConverter

# URL Schema Registry для работы с Avro схемами ключей
key.converter.schema.registry.url=http://schema-registry.company.com:8081

# URL Schema Registry для работы с Avro схемами значений
value.converter.schema.registry.url=http://schema-registry.company.com:8081

# === Внутренние конвертеры для служебных топиков ===

# Конвертер для ключей внутренних топиков Connect (JSON формат)
internal.key.converter=org.apache.kafka.connect.json.JsonConverter

# Конвертер для значений внутренних топиков Connect (JSON формат)
internal.value.converter=org.apache.kafka.connect.json.JsonConverter

# Отключить схемы для ключей внутренних топиков (упрощает формат)
internal.key.converter.schemas.enable=false

# Отключить схемы для значений внутренних топиков (упрощает формат)
internal.value.converter.schemas.enable=false

# === Плагины ===

# Пути к директориям с плагинами и коннекторами (через запятую)
plugin.path=/usr/share/java,/usr/share/confluent-hub-components

# === Настройки производительности для больших объемов (2+ TB) ===

# Интервал сохранения offset'ов в миллисекундах (10 секунд)
# Увеличение снижает нагрузку на Kafka, но увеличивает риск повторной обработки при сбое
offset.flush.interval.ms=10000

# Максимальное количество записей в одном poll запросе consumer'а (5000 записей)
# Увеличение повышает throughput, но увеличивает потребление памяти
consumer.max.poll.records=5000

# Максимальный размер данных одной партиции за один fetch запрос (10 MB)
# Увеличение улучшает пропускную способность для больших сообщений
consumer.max.partition.fetch.bytes=10485760

# Максимальный размер данных всех партиций за один fetch запрос (50 MB)
# Должен быть >= consumer.max.partition.fetch.bytes * количество партиций
consumer.fetch.max.bytes=52428800

# Максимальный размер одного producer request (10 MB)
# Должен соответствовать размерам сообщений и пакетной обработке
producer.max.request.size=10485760

# Тип компрессии для producer'а (snappy - быстрый и эффективный)
# Снижает сетевую нагрузку и размер данных в Kafka
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

#### Описание параметров S3 Sink Connector

##### Базовые параметры

| Параметр | Описание |
|----------|----------|
| `name` | Уникальное имя коннектора в Kafka Connect |
| `connector.class` | Полное имя класса коннектора (S3 Sink от Confluent) |
| `tasks.max` | Максимальное количество параллельных задач (tasks) для обработки данных. 12 задач для высокой пропускной способности |
| `topics` | Список топиков Kafka для чтения (через запятую). Здесь: `order-events` |

##### S3/MinIO подключение

| Параметр | Описание |
|----------|----------|
| `s3.bucket.name` | Имя S3 bucket для записи файлов. Должен быть создан заранее |
| `s3.region` | AWS регион. Для MinIO можно использовать любое значение, обычно `us-east-1` |
| `store.url` | URL эндпоинта S3-совместимого хранилища (MinIO). Для AWS S3 не указывается |
| `s3.path.style.access.enabled` | `true` для MinIO (path-style URLs: `http://host/bucket/key`). AWS S3 использует virtual-hosted style |
| `s3.part.size` | Размер части для multipart upload в байтах (64 MB). Влияет на производительность загрузки больших файлов |
| `aws.access.key.id` | Access Key для аутентификации в S3/MinIO |
| `aws.secret.access.key` | Secret Key для аутентификации в S3/MinIO |

##### Управление записью файлов (Flush/Rotate)

| Параметр | Описание |
|----------|----------|
| `flush.size` | Количество записей в буфере перед сбросом в файл (100K записей). Увеличение создает меньше, но больше файлов |
| `rotate.interval.ms` | Максимальное время в миллисекундах между ротациями файлов (10 минут). Гарантирует регулярное создание файлов даже при низком потоке |
| `rotate.schedule.interval.ms` | Интервал планового создания новых файлов по времени (1 час). Работает совместно с `rotate.interval.ms` |

##### Формат и компрессия

| Параметр | Описание |
|----------|----------|
| `storage.class` | Класс для взаимодействия с хранилищем (S3Storage для S3/MinIO) |
| `format.class` | Формат выходных файлов. `ParquetFormat` - колоночный формат для аналитики |
| `parquet.codec` | Кодек компрессии для Parquet файлов. `snappy` - баланс между скоростью и степенью сжатия |

##### Схема данных

| Параметр | Описание |
|----------|----------|
| `schema.compatibility` | Режим совместимости схем. `BACKWARD` - новая схема может читать старые данные |
| `schema.generator.class` | Класс для генерации схем. `DefaultSchemaGenerator` - стандартный генератор для Hive-совместимых таблиц |

##### Партиционирование (по времени)

| Параметр | Описание |
|----------|----------|
| `partitioner.class` | Класс партиционера. `TimeBasedPartitioner` - разбивает данные по временным меткам |
| `path.format` | Шаблон пути для партиций. `'calc_id='yyyyMMdd-HHmmss'/dt='yyyy-MM-dd'/hour='HH` создает иерархию папок по дате и часу |
| `partition.duration.ms` | Длительность одной партиции в миллисекундах (1 час). Определяет гранулярность партиционирования |
| `locale` | Локаль для форматирования дат (`en-US`) |
| `timezone` | Часовой пояс для партиционирования (`UTC`) |
| `timestamp.extractor` | Источник timestamp для партиционирования. `Record` - использует время записи в Kafka |

##### Структура путей

| Параметр | Описание |
|----------|----------|
| `topics.dir` | Корневая директория для топиков в bucket. Путь будет: `s3://bucket/topics/topic-name/...` |
| `directory.delim` | Разделитель директорий в пути (стандартный `/`) |

##### Обработка ошибок

| Параметр | Описание |
|----------|----------|
| `behavior.on.null.values` | Поведение при получении `null` значений. `ignore` - пропускает такие записи |
| `errors.tolerance` | Толерантность к ошибкам. `none` - останавливает коннектор при любой ошибке. Альтернатива: `all` - продолжает работу |
| `errors.log.enable` | Включить логирование ошибок (`true`) |
| `errors.log.include.messages` | Включать сами сообщения в логи ошибок (`true`). Полезно для отладки |

##### Конвертеры данных (переопределение на уровне коннектора)

| Параметр | Описание |
|----------|----------|
| `key.converter` | Конвертер для ключей записей. `StringConverter` - ключи как простые строки |
| `value.converter` | Конвертер для значений записей. `AvroConverter` - значения в Avro формате со схемой |
| `value.converter.schema.registry.url` | URL Schema Registry для получения Avro схем значений |

##### Настройки производительности consumer'а (переопределение)

| Параметр | Описание |
|----------|----------|
| `consumer.override.max.poll.records` | Максимальное количество записей за один poll (5000). Переопределяет настройку из `connect-distributed.properties` |
| `consumer.override.max.partition.fetch.bytes` | Максимальный размер данных одной партиции за fetch (20 MB). Увеличено для больших сообщений |
| `consumer.override.fetch.max.bytes` | Максимальный размер данных всех партиций за fetch (50 MB). Должен покрывать все читаемые партиции |
| `consumer.override.auto.offset.reset` | Поведение при отсутствии offset'а. `earliest` - читать с начала топика. Альтернатива: `latest` - с конца |

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
      # Создаем переменную с названием образа
      ImageName=kafka-connect-datalake:latest

      # Создаем переменную с названием контейнера
      ContainerName=kafka-connect

      # Создаем скрипт деплоя
      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo 'Собираем Docker образ...'
      docker build -t ${ImageName} .

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

      echo "Копируем папку на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"
      rsync -avz ./ svc_user@${SRV_APP}:~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}

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
- Docker образ собирается локально на целевом сервере из скопированного проекта

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
