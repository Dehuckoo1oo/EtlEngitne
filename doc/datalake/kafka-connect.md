# Kafka Connect - MVP Deployment

## Назначение

Сервис для выгрузки событий из Kafka в MinIO в формате Parquet через S3 Sink Connector.
MVP: один worker, без кластера и балансировщиков.

**ВАЖНО**: Конфигурация использует переменные окружения (а не файл `connect-distributed.properties`) для поддержки Avro формата и Schema Registry. Это стандартный подход для Confluent Platform.

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
├── connectors/
│   ├── s3-sink-order-events.json
│   ├── s3-sink-full-avro.json (пример)
│   └── s3-sink-json.json (пример)
├── .env.example (опционально)
├── .gitlab-ci.yml
└── README.md
```

**Примечание**: Директория `config/` не требуется, так как все параметры передаются через переменные окружения.

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

# Скопировать конфиги коннекторов (для примеров)
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

**ПРИМЕЧАНИЕ**: Файл `connect-distributed.properties` приведен ниже для справки, но **НЕ используется** в текущей конфигурации. Все параметры передаются через переменные окружения при запуске контейнера (см. раздел "Build & Deploy").

### connect-distributed.properties (для справки)

`config/connect-distributed.properties` (НЕ ИСПОЛЬЗУЕТСЯ):

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

    "aws.access.key.id": "${env:AWS_ACCESS_KEY_ID}",
    "aws.secret.access.key": "${env:AWS_SECRET_ACCESS_KEY}",

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

    "_comment_converters": "Конвертеры для коннектора (переопределяют глобальные настройки)",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry.company.com:8081",
    "value.converter.enhanced.avro.schema.support": "true",

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
| `aws.access.key.id` | Access Key для аутентификации в S3/MinIO. Используйте `${env:AWS_ACCESS_KEY_ID}` для подстановки из переменных окружения |
| `aws.secret.access.key` | Secret Key для аутентификации в S3/MinIO. Используйте `${env:AWS_SECRET_ACCESS_KEY}` для подстановки из переменных окружения |

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
| `value.converter.enhanced.avro.schema.support` | Включает расширенную поддержку Avro типов данных (Date, Time, Timestamp, Decimal). Рекомендуется `true` |

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

**ВАЖНО**: Для работы с Avro и Schema Registry все параметры передаются через переменные окружения при запуске контейнера (см. "Вариант 1: Вручную" ниже).

Основные группы переменных:

1. **Подключение к Kafka**: `CONNECT_BOOTSTRAP_SERVERS`
2. **Schema Registry (Avro)**: `CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL`, `CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL`
3. **Внутренние топики**: `CONNECT_CONFIG_STORAGE_TOPIC`, `CONNECT_OFFSET_STORAGE_TOPIC`, `CONNECT_STATUS_STORAGE_TOPIC`
4. **JVM**: `KAFKA_HEAP_OPTS="-Xms8g -Xmx8g"`
5. **S3/MinIO**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

`.env.example` (опционально, если не используете переменные окружения напрямую):

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

# 3. Запуск контейнера (все параметры передаются через -e флаги)
docker run -d \
  --name kafka-connect \
  --restart unless-stopped \
  -p 8083:8083 \
  -e CONNECT_BOOTSTRAP_SERVERS="kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092" \
  -e CONNECT_REST_ADVERTISED_HOST_NAME="kafka-connect.company.com" \
  -e CONNECT_GROUP_ID="kafka-connect-mvp" \
  -e CONNECT_CONFIG_STORAGE_TOPIC="_connect-configs" \
  -e CONNECT_OFFSET_STORAGE_TOPIC="_connect-offsets" \
  -e CONNECT_STATUS_STORAGE_TOPIC="_connect-status" \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR="3" \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR="3" \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR="3" \
  -e CONNECT_KEY_CONVERTER="io.confluent.connect.avro.AvroConverter" \
  -e CONNECT_VALUE_CONVERTER="io.confluent.connect.avro.AvroConverter" \
  -e CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL="http://schema-registry.company.com:8081" \
  -e CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL="http://schema-registry.company.com:8081" \
  -e CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_INTERNAL_KEY_CONVERTER_SCHEMAS_ENABLE="false" \
  -e CONNECT_INTERNAL_VALUE_CONVERTER_SCHEMAS_ENABLE="false" \
  -e CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components" \
  -e KAFKA_HEAP_OPTS="-Xms8g -Xmx8g" \
  -e AWS_ACCESS_KEY_ID="kafka-connect" \
  -e AWS_SECRET_ACCESS_KEY="<password_from_minio>" \
  kafka-connect-datalake:latest

# 4. Health check
curl -f http://kafka-connect.company.com:8083/

# 5. Проверка доступности Schema Registry
docker exec kafka-connect curl -f http://schema-registry.company.com:8081/subjects
```

**Перед запуском замените**:
- `kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092` - на ваши адреса Kafka брокеров
- `kafka-connect.company.com` - на ваш hostname
- `schema-registry.company.com:8081` - на адрес вашего Schema Registry
- `<password_from_minio>` - на реальный пароль от MinIO

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
    - SCHEMA_REGISTRY_URL="http://schema-registry.company.com:8081"
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
        -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR="3" \
        -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR="3" \
        -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR="3" \
        -e CONNECT_KEY_CONVERTER="io.confluent.connect.avro.AvroConverter" \
        -e CONNECT_VALUE_CONVERTER="io.confluent.connect.avro.AvroConverter" \
        -e CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL}" \
        -e CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL}" \
        -e CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
        -e CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
        -e CONNECT_INTERNAL_KEY_CONVERTER_SCHEMAS_ENABLE="false" \
        -e CONNECT_INTERNAL_VALUE_CONVERTER_SCHEMAS_ENABLE="false" \
        -e CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components" \
        -e KAFKA_HEAP_OPTS="-Xms8g -Xmx8g" \
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
      echo 'Проверяем доступность Schema Registry из контейнера:'
      docker exec ${ContainerName} curl -f ${SCHEMA_REGISTRY_URL}/subjects || echo 'ВНИМАНИЕ: Schema Registry недоступен из контейнера.'
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

## Примеры конфигураций коннекторов

### Пример 1: String ключи + Avro значения (из документа выше)

Стандартная конфигурация `connectors/s3-sink-order-events.json` - см. выше.

### Пример 2: Avro ключи + Avro значения

Если и ключи, и значения в формате Avro:

`connectors/s3-sink-full-avro.json`:

```json
{
  "name": "s3-sink-full-avro",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "12",
    "topics": "your-full-avro-topic",

    "s3.bucket.name": "datalake",
    "s3.region": "us-east-1",
    "store.url": "https://minio.company.com:9000",
    "s3.path.style.access.enabled": "true",

    "aws.access.key.id": "${env:AWS_ACCESS_KEY_ID}",
    "aws.secret.access.key": "${env:AWS_SECRET_ACCESS_KEY}",

    "flush.size": "100000",
    "rotate.interval.ms": "600000",
    "rotate.schedule.interval.ms": "3600000",

    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "parquet.codec": "snappy",

    "schema.compatibility": "BACKWARD",

    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'dt='yyyy-MM-dd'/hour='HH",
    "partition.duration.ms": "3600000",
    "timezone": "UTC",
    "timestamp.extractor": "Record",

    "topics.dir": "topics",

    "behavior.on.null.values": "ignore",
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",

    "_comment": "Оба конвертера Avro для полной типизации",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry.company.com:8081",
    "value.converter.schema.registry.url": "http://schema-registry.company.com:8081",
    "key.converter.enhanced.avro.schema.support": "true",
    "value.converter.enhanced.avro.schema.support": "true",

    "consumer.override.max.poll.records": "5000",
    "consumer.override.auto.offset.reset": "earliest"
  }
}
```

### Пример 3: JSON конвертеры (без Schema Registry)

Если данные в Kafka в простом JSON формате без схем:

`connectors/s3-sink-json.json`:

```json
{
  "name": "s3-sink-json",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "12",
    "topics": "your-json-topic",

    "s3.bucket.name": "datalake",
    "s3.region": "us-east-1",
    "store.url": "https://minio.company.com:9000",
    "s3.path.style.access.enabled": "true",

    "aws.access.key.id": "${env:AWS_ACCESS_KEY_ID}",
    "aws.secret.access.key": "${env:AWS_SECRET_ACCESS_KEY}",

    "flush.size": "100000",
    "rotate.interval.ms": "600000",

    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",

    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'dt='yyyy-MM-dd'/hour='HH",
    "partition.duration.ms": "3600000",
    "timezone": "UTC",

    "topics.dir": "topics",

    "errors.tolerance": "none",

    "_comment": "JSON конвертеры без схем",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",

    "consumer.override.auto.offset.reset": "earliest"
  }
}
```

---

## Партиционирование по полям из топика

### Общие сведения

По умолчанию в примерах выше используется **временное партиционирование** (`TimeBasedPartitioner`), которое создает папки на основе даты и времени записи в Kafka.

Однако часто требуется **партиционирование по полям из сообщений** (например, по `region`, `user_id`, `product_category` и т.д.). Для этого используется `FieldPartitioner`.

### Различия между партиционерами

| Партиционер | Описание | Пример пути |
|------------|----------|-------------|
| `TimeBasedPartitioner` | Партиционирует по timestamp записи в Kafka | `topics/order-events/dt=2026-01-12/hour=14/` |
| `FieldPartitioner` | Партиционирует по значению поля из сообщения | `topics/order-events/region=EU/status=completed/` |
| `DailyPartitioner` | Партиционирует по дням (упрощенный вариант TimeBasedPartitioner) | `topics/order-events/year=2026/month=01/day=12/` |
| `HourlyPartitioner` | Партиционирует по часам | `topics/order-events/year=2026/month=01/day=12/hour=14/` |
| `DefaultPartitioner` | Без партиционирования, все файлы в одной директории | `topics/order-events/` |

### Пример 4: Партиционирование по одному полю

Партиционирование по полю `region` из сообщения:

`connectors/s3-sink-field-partition-single.json`:

```json
{
  "name": "s3-sink-field-partition-single",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "12",
    "topics": "order-events",

    "s3.bucket.name": "datalake",
    "s3.region": "us-east-1",
    "store.url": "https://minio.company.com:9000",
    "s3.path.style.access.enabled": "true",

    "aws.access.key.id": "${env:AWS_ACCESS_KEY_ID}",
    "aws.secret.access.key": "${env:AWS_SECRET_ACCESS_KEY}",

    "flush.size": "100000",
    "rotate.interval.ms": "600000",

    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "parquet.codec": "snappy",

    "schema.compatibility": "BACKWARD",

    "_comment_partitioner": "Партиционирование по полю region из сообщения",
    "partitioner.class": "io.confluent.connect.storage.partitioner.FieldPartitioner",
    "partition.field.name": "region",

    "topics.dir": "topics",
    "directory.delim": "/",

    "behavior.on.null.values": "ignore",
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry.company.com:8081",
    "value.converter.enhanced.avro.schema.support": "true",

    "consumer.override.max.poll.records": "5000",
    "consumer.override.auto.offset.reset": "earliest"
  }
}
```

**Результат**: Файлы будут размещаться в папках типа:
```
s3://datalake/topics/order-events/region=EU/
s3://datalake/topics/order-events/region=US/
s3://datalake/topics/order-events/region=APAC/
```

### Пример 5: Партиционирование по нескольким полям

Партиционирование по полям `region` и `status`:

`connectors/s3-sink-field-partition-multi.json`:

```json
{
  "name": "s3-sink-field-partition-multi",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "12",
    "topics": "order-events",

    "s3.bucket.name": "datalake",
    "s3.region": "us-east-1",
    "store.url": "https://minio.company.com:9000",
    "s3.path.style.access.enabled": "true",

    "aws.access.key.id": "${env:AWS_ACCESS_KEY_ID}",
    "aws.secret.access.key": "${env:AWS_SECRET_ACCESS_KEY}",

    "flush.size": "100000",
    "rotate.interval.ms": "600000",

    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "parquet.codec": "snappy",

    "schema.compatibility": "BACKWARD",

    "_comment_partitioner": "Партиционирование по полям region и status",
    "partitioner.class": "io.confluent.connect.storage.partitioner.FieldPartitioner",
    "partition.field.name": "region,status",

    "topics.dir": "topics",
    "directory.delim": "/",

    "behavior.on.null.values": "ignore",
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry.company.com:8081",
    "value.converter.enhanced.avro.schema.support": "true",

    "consumer.override.max.poll.records": "5000",
    "consumer.override.auto.offset.reset": "earliest"
  }
}
```

**Результат**: Файлы будут размещаться в иерархии папок:
```
s3://datalake/topics/order-events/region=EU/status=completed/
s3://datalake/topics/order-events/region=EU/status=pending/
s3://datalake/topics/order-events/region=US/status=completed/
s3://datalake/topics/order-events/region=US/status=cancelled/
```

### Пример 6: Комбинированное партиционирование (поле + время)

Если нужно сочетать партиционирование по полю и по времени, используйте **два подхода**:

#### Вариант А: Поле + DailyPartitioner

К сожалению, S3 Sink Connector не поддерживает одновременное использование `FieldPartitioner` и `TimeBasedPartitioner` напрямую. Но можно использовать **вложенные партиционеры**:

`connectors/s3-sink-field-and-time.json`:

```json
{
  "name": "s3-sink-field-and-time",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "12",
    "topics": "order-events",

    "s3.bucket.name": "datalake",
    "s3.region": "us-east-1",
    "store.url": "https://minio.company.com:9000",
    "s3.path.style.access.enabled": "true",

    "aws.access.key.id": "${env:AWS_ACCESS_KEY_ID}",
    "aws.secret.access.key": "${env:AWS_SECRET_ACCESS_KEY}",

    "flush.size": "100000",
    "rotate.interval.ms": "600000",

    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "parquet.codec": "snappy",

    "schema.compatibility": "BACKWARD",

    "_comment_partitioner": "Партиционирование сначала по region, затем по дням",
    "partitioner.class": "io.confluent.connect.storage.partitioner.DailyPartitioner",
    "partition.field.name": "region",
    "locale": "en-US",
    "timezone": "UTC",

    "topics.dir": "topics",
    "directory.delim": "/",

    "behavior.on.null.values": "ignore",
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry.company.com:8081",
    "value.converter.enhanced.avro.schema.support": "true",

    "consumer.override.max.poll.records": "5000",
    "consumer.override.auto.offset.reset": "earliest"
  }
}
```

**Результат**:
```
s3://datalake/topics/order-events/region=EU/year=2026/month=01/day=12/
s3://datalake/topics/order-events/region=US/year=2026/month=01/day=12/
```

#### Вариант Б: Использование timestamp поля из сообщения

Если в вашем сообщении есть поле с timestamp (например, `event_date`), можно партиционировать по нему:

```json
{
  "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
  "path.format": "'region='region'/dt='yyyy-MM-dd",
  "partition.duration.ms": "86400000",
  "timestamp.extractor": "RecordField",
  "timestamp.field": "event_date",
  "timezone": "UTC"
}
```

**Важно**: `timestamp.field` указывает на поле в сообщении, которое содержит timestamp (должен быть тип `long` в миллисекундах или ISO 8601 string).

### Параметры для FieldPartitioner

| Параметр | Описание | Пример |
|----------|----------|--------|
| `partitioner.class` | Класс партиционера | `io.confluent.connect.storage.partitioner.FieldPartitioner` |
| `partition.field.name` | Имя поля (или список полей через запятую) для партиционирования | `region` или `region,status,product_type` |

### Важные замечания

1. **Схема Avro обязательна**: Поля для партиционирования должны быть определены в Avro схеме сообщения.

2. **Поддерживаемые типы полей**:
   - String
   - Int/Long
   - Enum
   - Boolean

3. **Поля с null значениями**:
   - Если поле партиционирования содержит `null`, файл будет помещен в папку `field=null`
   - Используйте `behavior.on.null.values=ignore` для пропуска таких записей

4. **Производительность**:
   - Большое количество уникальных значений поля создаст много партиций
   - Каждая партиция = отдельная директория
   - Рекомендуется использовать поля с умеренной кардинальностью (10-1000 значений)

5. **Структура пути**:
   - Порядок полей в `partition.field.name` определяет иерархию папок
   - `region,status` → `region=EU/status=completed/`
   - `status,region` → `status=completed/region=EU/`

### Пример структуры сообщения в Kafka (Avro)

Для партиционирования по полям ваше сообщение должно выглядеть так:

```json
{
  "order_id": "12345",
  "region": "EU",
  "status": "completed",
  "user_id": 67890,
  "amount": 150.00,
  "event_date": 1704988800000
}
```

Avro схема:

```json
{
  "type": "record",
  "name": "OrderEvent",
  "namespace": "com.company.events",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "region", "type": "string"},
    {"name": "status", "type": "string"},
    {"name": "user_id", "type": "long"},
    {"name": "amount", "type": "double"},
    {"name": "event_date", "type": "long"}
  ]
}
```

---

## Следующий шаг

После успешного запуска Kafka Connect переходите к:
👉 [Trino](trino.md)
