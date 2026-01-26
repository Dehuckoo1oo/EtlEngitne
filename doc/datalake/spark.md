# Apache Spark - MVP Deployment

## Назначение

Распределенный движок для обработки больших данных с поддержкой batch и stream processing. Интегрируется с существующим Data Lake через Hive Metastore и MinIO.
MVP: один master, один или несколько workers, без HA и балансировщиков.

**Ключевые возможности**:
- Распределенная обработка данных (Spark SQL, DataFrame API)
- Интеграция с Hive Metastore (общий каталог таблиц с Trino)
- Чтение/запись Parquet файлов из MinIO (S3A)
- Поддержка Delta Lake (ACID транзакции, time travel)
- Работа через Jupyter notebooks (PySpark)

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                        Spark Cluster                            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │ Spark Master │    │Spark Worker 1│    │Spark Worker N│      │
│  │   (7077)     │◄───│   (8081)     │    │   (8081)     │      │
│  │   UI: 8080   │    │              │    │              │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
└─────────────────────────────────────────────────────────────────┘
         │                     │                    │
         └─────────────────────┼────────────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         │                     │                     │
         ▼                     ▼                     ▼
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│    MinIO     │      │Hive Metastore│      │   Jupyter    │
│    (S3A)     │      │   (Thrift)   │      │  (PySpark)   │
└──────────────┘      └──────────────┘      └──────────────┘
```

**Компоненты**:
- **Spark Master**: Координатор кластера, распределяет задачи между workers
- **Spark Workers**: Выполняют задачи, масштабируются горизонтально
- **Jupyter**: Клиент для интерактивной работы с кластером через PySpark

---

## Требования к машинам

### Spark Master

| Параметр | Значение |
|----------|----------|
| CPU | 4 cores |
| RAM | 8 GB |
| Storage | 50 GB SSD |
| Network | 1 Gbit/s |
| Hostname | spark-master.company.com |

### Spark Worker (на каждую ноду)

| Параметр | Значение |
|----------|----------|
| CPU | 16 cores |
| RAM | 64 GB |
| Storage | 500 GB SSD |
| Network | 10 Gbit/s |
| Hostname | spark-worker-N.company.com |

**Рекомендации по масштабированию**:
- Минимум: 1 worker (для разработки)
- Рекомендуется: 3+ workers (для продакшена)
- Для 2+ TB данных: 5-10 workers

### Минимальная конфигурация (ограниченные ресурсы)

| Параметр | Значение |
|----------|----------|
| CPU | 12 cores |
| RAM | 64 GB |
| Storage | **64 GB** (минимум) |
| Network | 10 Gbit/s |

⚠️ **Ограничения минимальной конфигурации**:
- Обработка будет медленной, но стабильной (без OOM)
- Можно обрабатывать ~1 TB данных
- Рекомендуется работа с датасетами до 500GB-1TB

📋 **См. раздел**: [Конфигурация для ограниченных ресурсов](#конфигурация-для-ограниченных-ресурсов-64gb-ram-12-cores-64gb-disk)

---

## Порты

| Сервис | Порт | Назначение |
|--------|------|------------|
| Spark Master | 7077 | Cluster communication |
| Spark Master | 8080 | Web UI |
| Spark Worker | 8081 | Worker Web UI |
| Spark Driver | 4040 | Application UI (в Jupyter) |

---

## GitLab структура

```
spark/
├── master/
│   ├── Dockerfile
│   ├── config/
│   │   ├── spark-defaults.conf
│   │   ├── spark-env.sh
│   │   ├── core-site.xml
│   │   ├── hive-site.xml
│   │   └── minio-root-ca.crt
│   ├── .env.example
│   └── .gitlab-ci.yml
├── worker/
│   ├── Dockerfile
│   ├── config/
│   │   ├── spark-defaults.conf
│   │   ├── spark-env.sh
│   │   ├── core-site.xml
│   │   ├── hive-site.xml
│   │   └── minio-root-ca.crt
│   ├── .env.example
│   └── .gitlab-ci.yml
└── README.md
```

**Важно**: Файл `minio-root-ca.crt` должен содержать корневой сертификат вашего MinIO сервера в формате PEM.

---

## Dockerfile

### Spark Master

`spark/master/Dockerfile`:

```dockerfile
FROM registry.company.com/bitnami/spark:3.5.0

USER root

# Версии JAR (совместимые с Spark 3.5.x / Hadoop 3.3.4)
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DELTA_VERSION=3.2.0
ENV SCALA_VERSION=2.12

# Nexus Maven URL (передается через --build-arg)
ARG NEXUS_MAVEN_URL=https://nexus.company.com/repository/maven-public

# Установка утилит
RUN apt-get update && apt-get install -y curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Установка сертификата MinIO в Java truststore
COPY config/minio-root-ca.crt /tmp/minio-root-ca.crt
RUN keytool -import -trustcacerts -keystore $JAVA_HOME/lib/security/cacerts \
    -storepass changeit -noprompt -alias minio-ca -file /tmp/minio-root-ca.crt && \
    rm /tmp/minio-root-ca.crt

# Скачивание JAR для S3A (MinIO) через Nexus
RUN curl -sL ${NEXUS_MAVEN_URL}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o /opt/bitnami/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -sL ${NEXUS_MAVEN_URL}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Скачивание JAR для Delta Lake через Nexus
RUN curl -sL ${NEXUS_MAVEN_URL}/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    -o /opt/bitnami/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -sL ${NEXUS_MAVEN_URL}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    -o /opt/bitnami/spark/jars/delta-storage-${DELTA_VERSION}.jar

# Копирование конфигурации
COPY config/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY config/spark-env.sh /opt/bitnami/spark/conf/spark-env.sh
COPY config/core-site.xml /opt/bitnami/spark/conf/core-site.xml
COPY config/hive-site.xml /opt/bitnami/spark/conf/hive-site.xml

RUN chmod +x /opt/bitnami/spark/conf/spark-env.sh

USER 1001

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

EXPOSE 7077 8080
```

### Spark Worker

`spark/worker/Dockerfile`:

```dockerfile
FROM registry.company.com/bitnami/spark:3.5.0

USER root

# Версии JAR (должны совпадать с Master)
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DELTA_VERSION=3.2.0
ENV SCALA_VERSION=2.12

# Nexus Maven URL (передается через --build-arg)
ARG NEXUS_MAVEN_URL=https://nexus.company.com/repository/maven-public

# Установка утилит
RUN apt-get update && apt-get install -y curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Установка сертификата MinIO в Java truststore
COPY config/minio-root-ca.crt /tmp/minio-root-ca.crt
RUN keytool -import -trustcacerts -keystore $JAVA_HOME/lib/security/cacerts \
    -storepass changeit -noprompt -alias minio-ca -file /tmp/minio-root-ca.crt && \
    rm /tmp/minio-root-ca.crt

# Скачивание JAR для S3A (MinIO) через Nexus
RUN curl -sL ${NEXUS_MAVEN_URL}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o /opt/bitnami/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -sL ${NEXUS_MAVEN_URL}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Скачивание JAR для Delta Lake через Nexus
RUN curl -sL ${NEXUS_MAVEN_URL}/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    -o /opt/bitnami/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -sL ${NEXUS_MAVEN_URL}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    -o /opt/bitnami/spark/jars/delta-storage-${DELTA_VERSION}.jar

# Копирование конфигурации
COPY config/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY config/spark-env.sh /opt/bitnami/spark/conf/spark-env.sh
COPY config/core-site.xml /opt/bitnami/spark/conf/core-site.xml
COPY config/hive-site.xml /opt/bitnami/spark/conf/hive-site.xml

RUN chmod +x /opt/bitnami/spark/conf/spark-env.sh

USER 1001

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8081/ || exit 1

EXPOSE 8081
```

---

## Конфигурационные файлы

### spark-defaults.conf

`spark/master/config/spark-defaults.conf` и `spark/worker/config/spark-defaults.conf`:

```properties
# === S3A/MinIO Configuration ===
spark.hadoop.fs.s3a.endpoint=https://minio.company.com:9000
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider

# === Hive Metastore Integration ===
spark.sql.catalogImplementation=hive
spark.hadoop.hive.metastore.uris=thrift://hive-metastore.company.com:9083
spark.sql.warehouse.dir=s3a://datalake/warehouse

# === Delta Lake ===
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# === Performance Settings ===
spark.sql.parquet.compression.codec=snappy
spark.sql.shuffle.partitions=200
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true

# === Serialization ===
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max=1024m

# === Dynamic Allocation ===
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=1
spark.dynamicAllocation.maxExecutors=10
spark.dynamicAllocation.executorIdleTimeout=60s
spark.shuffle.service.enabled=true

# === History Server (optional) ===
spark.eventLog.enabled=true
spark.eventLog.dir=s3a://datalake/spark-events
```

### spark-env.sh (Master)

`spark/master/config/spark-env.sh`:

```bash
#!/usr/bin/env bash

# Master configuration
export SPARK_MASTER_HOST=spark-master.company.com
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Memory settings
export SPARK_DAEMON_MEMORY=4g

# Logging
export SPARK_LOG_DIR=/opt/bitnami/spark/logs
```

### spark-env.sh (Worker)

`spark/worker/config/spark-env.sh`:

```bash
#!/usr/bin/env bash

# Worker configuration
export SPARK_MASTER_URL=spark://spark-master.company.com:7077
export SPARK_WORKER_CORES=14
export SPARK_WORKER_MEMORY=56g
export SPARK_WORKER_WEBUI_PORT=8081

# Work directory
export SPARK_WORKER_DIR=/opt/bitnami/spark/work

# Logging
export SPARK_LOG_DIR=/opt/bitnami/spark/logs
```

### core-site.xml

`spark/master/config/core-site.xml` и `spark/worker/config/core-site.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>https://minio.company.com:9000</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.connection.ssl.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
    <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>com.amazonaws.auth.EnvironmentVariableCredentialsProvider</value>
    </property>
    <property>
        <name>fs.s3a.block.size</name>
        <value>134217728</value>
        <description>128 MB block size</description>
    </property>
    <property>
        <name>fs.s3a.connection.maximum</name>
        <value>100</value>
    </property>
</configuration>
```

**Важно**: Credentials передаются через переменные окружения `AWS_ACCESS_KEY_ID` и `AWS_SECRET_ACCESS_KEY` (см. `.env.example`). Провайдер `EnvironmentVariableCredentialsProvider` автоматически читает их из окружения.

### hive-site.xml

`spark/master/config/hive-site.xml` и `spark/worker/config/hive-site.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://hive-metastore.company.com:9083</value>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>s3a://datalake/warehouse</value>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>false</value>
    </property>
    <property>
        <name>hive.exec.dynamic.partition</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.exec.dynamic.partition.mode</name>
        <value>nonstrict</value>
    </property>
</configuration>
```

---

## Конфигурация для ограниченных ресурсов (64GB RAM, 12 cores, 64GB disk)

Эта конфигурация позволяет обрабатывать **~1 TB данных** на минимальных ресурсах. Обработка будет медленной, но стабильной (без OOM).

### Принципы

1. **Агрессивный spill на диск** — когда память заканчивается, данные сбрасываются на диск
2. **Больше партиций** — меньше данных в памяти одновременно
3. **Меньше параллелизма** — меньше задач одновременно = меньше потребление памяти
4. **Сжатие везде** — уменьшает объем данных в памяти и на диске

### spark-defaults.conf (для ограниченных ресурсов)

```properties
# === MEMORY CONFIGURATION (консервативные настройки) ===
# Оставляем ~20GB для OS, JVM overhead, и буферов
spark.driver.memory=8g
spark.executor.memory=12g
spark.executor.memoryOverhead=4g

# Уменьшаем долю памяти для хранения данных (больше для execution)
spark.memory.fraction=0.4
spark.memory.storageFraction=0.3

# === DISK SPILL (критически важно!) ===
spark.sql.shuffle.spill.enabled=true
spark.shuffle.spill=true
spark.shuffle.spill.compress=true

# Путь для spill файлов (должен быть на быстром диске)
spark.local.dir=/data/spark-temp

# === PARALLELISM (уменьшаем для экономии памяти) ===
# Больше партиций = меньше данных на партицию = меньше памяти
spark.sql.shuffle.partitions=500
spark.default.parallelism=24

# Меньше одновременных задач
spark.executor.cores=2
spark.task.cpus=1

# === ADAPTIVE QUERY EXECUTION (автоматическая оптимизация) ===
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.coalescePartitions.minPartitionSize=64MB
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB

# === COMPRESSION (уменьшает потребление памяти и диска) ===
spark.sql.parquet.compression.codec=snappy
spark.io.compression.codec=lz4
spark.shuffle.compress=true
spark.rdd.compress=true
spark.broadcast.compress=true

# === BROADCAST (отключаем для больших таблиц) ===
# Уменьшаем порог broadcast join чтобы избежать OOM
spark.sql.autoBroadcastJoinThreshold=10MB

# === NETWORK & TIMEOUTS (увеличиваем для медленных операций) ===
spark.network.timeout=600s
spark.executor.heartbeatInterval=60s
spark.sql.broadcastTimeout=600s

# === GARBAGE COLLECTION ===
spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent
spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=16m

# === S3A TUNING (для MinIO) ===
spark.hadoop.fs.s3a.endpoint=https://minio.company.com:9000
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider
spark.hadoop.fs.s3a.fast.upload=true
spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer
spark.hadoop.fs.s3a.multipart.size=104857600
spark.hadoop.fs.s3a.connection.maximum=30

# === HIVE METASTORE ===
spark.sql.catalogImplementation=hive
spark.hadoop.hive.metastore.uris=thrift://hive-metastore.company.com:9083

# === DELTA LAKE ===
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# === SERIALIZATION ===
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max=512m
```

### spark-env.sh (для Worker с 64GB RAM)

```bash
#!/usr/bin/env bash

export SPARK_WORKER_CORES=12
export SPARK_WORKER_MEMORY=48g  # Оставляем 16GB для OS и overhead
export SPARK_WORKER_DIR=/data/spark-work
export SPARK_LOCAL_DIRS=/data/spark-temp

# Важно: создать директории с достаточным местом
# mkdir -p /data/spark-temp /data/spark-work
```

### Подготовка диска

```bash
# Создать директории для Spark
mkdir -p /data/spark-temp /data/spark-work

# Проверить свободное место (нужно минимум 50GB для spill)
df -h /data

# Установить права
chown -R spark:spark /data/spark-temp /data/spark-work
```

### Сравнение конфигураций

| Параметр | Стандартная (64GB RAM, 500GB disk) | Ограниченная (64GB RAM, 64GB disk) |
|----------|-----------------------------------|-----------------------------------|
| executor.memory | 48g | 12g |
| executor.cores | 8 | 2 |
| shuffle.partitions | 200 | 500 |
| memory.fraction | 0.6 | 0.4 |
| Пропускная способность | ~100 GB/час | ~20 GB/час |
| Время обработки 1TB | ~10 часов | ~50 часов |

---

## Environment Variables

### .env.example для Master

`spark/master/.env.example`:

```bash
# Spark Master settings
SPARK_MODE=master
SPARK_MASTER_HOST=spark-master.company.com
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080

# S3/MinIO credentials (из GitLab CI/CD Variables)
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

# Java settings
SPARK_DAEMON_JAVA_OPTS=-Xmx4g
```

### .env.example для Worker

`spark/worker/.env.example`:

```bash
# Spark Worker settings
SPARK_MODE=worker
SPARK_MASTER_URL=spark://${SPARK_MASTER_HOST}:7077
SPARK_WORKER_CORES=${SPARK_WORKER_CORES}
SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}
SPARK_WORKER_WEBUI_PORT=8081

# S3/MinIO credentials (из GitLab CI/CD Variables)
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

# Java settings
SPARK_DAEMON_JAVA_OPTS=-Xmx4g
```

---

## Зависимости

**ВАЖНО**: Spark зависит от следующих сервисов и должен быть запущен ПОСЛЕ их успешного развертывания:

1. **MinIO** - объектное хранилище (S3 storage)
   - Должен быть доступен по адресу `https://minio.company.com:9000`
   - Health check: `curl -f https://minio.company.com:9000/minio/health/live`

2. **Hive Metastore** - каталог таблиц
   - Должен быть доступен по адресу `thrift://hive-metastore.company.com:9083`
   - Health check: `nc -zv hive-metastore.company.com 9083`

### Проверка готовности зависимостей

```bash
# Проверить MinIO
curl -f https://minio.company.com:9000/minio/health/live

# Проверить Hive Metastore
nc -zv hive-metastore.company.com 9083
```

---

## Build & Deploy

### Этап 1: Создание пользователя MinIO для Spark

```bash
# На машине с доступом к MinIO
mc alias set datalake https://minio.company.com:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

# Создать пользователя для Spark
mc admin user add datalake spark-user ${SPARK_MINIO_PASSWORD}

# Создать политику доступа
cat > /tmp/spark-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::datalake",
                "arn:aws:s3:::datalake/*"
            ]
        }
    ]
}
EOF

mc admin policy create datalake spark-policy /tmp/spark-policy.json
mc admin policy attach datalake spark-policy --user spark-user
```

### Этап 2: Развертывание Spark Master

#### Вариант 1: Вручную

```bash
# На машине spark-master.company.com

# 1. Клонировать репозиторий
git clone https://gitlab.company.com/datalake/infrastructure.git
cd infrastructure/spark/master

# 2. Build образа
docker build \
    --build-arg NEXUS_MAVEN_URL=${NEXUS_MAVEN_URL} \
    -t spark-master:latest .

# 3. Создать .env
cp .env.example .env
nano .env  # Заполнить переменные

# 4. Запуск контейнера
docker run -d \
    --name spark-master \
    --hostname spark-master \
    --restart=always \
    --env-file .env \
    -p 7077:7077 \
    -p 8080:8080 \
    spark-master:latest

# 5. Health check
curl -f http://localhost:8080/
```

#### Вариант 2: GitLab CI/CD

`spark/master/.gitlab-ci.yml`:

```yaml
variables:
  GIT_STRATEGY: clone

stages:
  - deploy

Deploy Spark Master to TEST:
  stage: deploy
  tags: [your_runner_tag]
  needs: []
  when: manual
  allow_failure: false
  before_script:
    - SRV_APP="spark-master.company.com"
    - MINIO_HOST="minio.company.com"
    - HIVE_METASTORE_HOST="hive-metastore.company.com"
  script:
    - |
      # Создаем переменную с названием образа
      ImageName=spark-master:latest

      # Создаем переменную с названием контейнера
      ContainerName=spark-master

      # Создаем скрипт деплоя
      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo '==========================================================================================='
      echo 'Проверка зависимостей перед деплоем...'
      echo '==========================================================================================='

      # Проверить MinIO
      echo 'Проверяем доступность MinIO...'
      nc -zv ${MINIO_HOST} 9000 || \
        (echo 'ОШИБКА: MinIO недоступен!' && exit 1)

      # Проверить Hive Metastore
      echo 'Проверяем доступность Hive Metastore...'
      nc -zv ${HIVE_METASTORE_HOST} 9083 || \
        (echo 'ОШИБКА: Hive Metastore недоступен!' && exit 1)

      echo 'Все зависимости доступны. Продолжаем деплой...'
      echo '==========================================================================================='

      echo 'Собираем Docker образ...'
      docker build \
        --build-arg NEXUS_MAVEN_URL=${NEXUS_MAVEN_URL} \
        -t ${ImageName} .

      echo 'Останавливаем и удаляем старый контейнер...'
      docker stop ${ContainerName} && docker rm ${ContainerName} && echo 'Старый контейнер остановлен и удален.' || echo 'Старого контейнера нет, останавливать нечего.'

      echo 'Создаем новый контейнер...'
      docker run \
        -d \
        --name ${ContainerName} \
        --hostname spark-master \
        --restart=always \
        -e SPARK_MODE=master \
        -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
        -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
        -p 7077:7077 \
        -p 8080:8080 \
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
      echo 'Проверяем доступность Web UI:'
      sleep 5
      curl -f http://localhost:8080/ || echo 'ВНИМАНИЕ: Web UI еще не доступен. Дождитесь полной инициализации.'
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

### Этап 3: Развертывание Spark Workers

#### Вариант 1: Вручную

```bash
# На каждой машине spark-worker-N.company.com

# 1. Клонировать репозиторий
git clone https://gitlab.company.com/datalake/infrastructure.git
cd infrastructure/spark/worker

# 2. Build образа
docker build \
    --build-arg NEXUS_MAVEN_URL=${NEXUS_MAVEN_URL} \
    -t spark-worker:latest .

# 3. Создать .env
cp .env.example .env
nano .env  # Заполнить переменные

# 4. Запуск контейнера
docker run -d \
    --name spark-worker \
    --hostname spark-worker-1 \
    --restart=always \
    --env-file .env \
    -p 8081:8081 \
    spark-worker:latest

# 5. Health check
curl -f http://localhost:8081/
```

#### Вариант 2: GitLab CI/CD

`spark/worker/.gitlab-ci.yml`:

```yaml
variables:
  GIT_STRATEGY: clone

stages:
  - deploy

Deploy Spark Worker to TEST:
  stage: deploy
  tags: [your_runner_tag]
  needs: []
  when: manual
  allow_failure: false
  before_script:
    - SRV_APP="spark-worker-1.company.com"
    - SPARK_MASTER_HOST="spark-master.company.com"
  script:
    - |
      # Создаем переменную с названием образа
      ImageName=spark-worker:latest

      # Создаем переменную с названием контейнера
      ContainerName=spark-worker

      # Создаем скрипт деплоя
      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo '==========================================================================================='
      echo 'Проверка зависимостей перед деплоем...'
      echo '==========================================================================================='

      # Проверить Spark Master
      echo 'Проверяем доступность Spark Master...'
      nc -zv ${SPARK_MASTER_HOST} 7077 || \
        (echo 'ОШИБКА: Spark Master недоступен!' && exit 1)

      echo 'Все зависимости доступны. Продолжаем деплой...'
      echo '==========================================================================================='

      echo 'Собираем Docker образ...'
      docker build \
        --build-arg NEXUS_MAVEN_URL=${NEXUS_MAVEN_URL} \
        -t ${ImageName} .

      echo 'Останавливаем и удаляем старый контейнер...'
      docker stop ${ContainerName} && docker rm ${ContainerName} && echo 'Старый контейнер остановлен и удален.' || echo 'Старого контейнера нет, останавливать нечего.'

      echo 'Создаем новый контейнер...'
      docker run \
        -d \
        --name ${ContainerName} \
        --hostname \$(hostname) \
        --restart=always \
        -e SPARK_MODE=worker \
        -e SPARK_MASTER_URL=spark://${SPARK_MASTER_HOST}:7077 \
        -e SPARK_WORKER_CORES=${SPARK_WORKER_CORES} \
        -e SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY} \
        -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
        -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
        -p 8081:8081 \
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
      echo 'Проверяем доступность Worker Web UI:'
      sleep 5
      curl -f http://localhost:8081/ || echo 'ВНИМАНИЕ: Worker Web UI еще не доступен. Дождитесь полной инициализации.'
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

### Этап 4: Проверка кластера

```bash
# Проверить Master UI
curl -f http://spark-master.company.com:8080/

# Проверить количество workers через API
curl -s http://spark-master.company.com:8080/json/ | jq '.workers | length'

# Должны быть видны все workers
# Статус: ALIVE, Workers: N
```

### GitLab CI/CD Variables

Настроить в GitLab → Settings → CI/CD → Variables:

| Переменная | Значение | Тип |
|-----------|----------|-----|
| `AWS_ACCESS_KEY_ID` | `spark-user` | Variable |
| `AWS_SECRET_ACCESS_KEY` | `<password>` | Variable (Masked) |
| `NEXUS_MAVEN_URL` | `https://nexus.company.com/repository/maven-public` | Variable |
| `SPARK_MASTER_HOST` | `spark-master.company.com` | Variable |
| `SPARK_WORKER_CORES` | `14` | Variable |
| `SPARK_WORKER_MEMORY` | `56g` | Variable |
| `MINIO_HOST` | `minio.company.com` | Variable |
| `HIVE_METASTORE_HOST` | `hive-metastore.company.com` | Variable |

**Примечание**:
- Замените `your_runner_tag` на тег вашего GitLab Runner
- Замените `svc_user` на пользователя для SSH подключения
- Docker образ собирается локально на целевом сервере из скопированного проекта

---

## Интеграция с Jupyter

Подробная документация по настройке Jupyter с PySpark и подключению к Spark кластеру: [jupyter.md](jupyter.md)

**Важно**: Версии JAR в Jupyter должны совпадать с версиями в Spark кластере:

| Компонент | Версия |
|-----------|--------|
| Spark | 3.5.0 |
| Hadoop AWS | 3.3.4 |
| AWS SDK | 1.12.262 |
| Delta Lake | 3.2.0 |

---

## Примеры использования

### Подключение к кластеру из Jupyter

```python
from pyspark.sql import SparkSession
import os

# Подключение к Spark кластеру
spark = SparkSession.builder \
    .appName("DataLake-Analysis") \
    .master("spark://spark-master.company.com:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "https://minio.company.com:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print(f"Spark UI: http://spark-master.company.com:8080")
print(f"Application UI: http://localhost:4040")
```

### Чтение Parquet из MinIO

```python
# Чтение данных из S3
df = spark.read.parquet("s3a://datalake/topics/order-events/")

print(f"Total records: {df.count()}")
df.printSchema()
df.show(5)
```

### SQL запросы

```python
# Создание временного view
df.createOrReplaceTempView("orders")

# SQL запрос
result = spark.sql("""
    SELECT
        dt,
        COUNT(*) as order_count,
        SUM(total_amount) as total_revenue
    FROM orders
    WHERE dt >= '2024-01-01'
    GROUP BY dt
    ORDER BY dt DESC
    LIMIT 30
""")
result.show()
```

### Работа с Hive Metastore

```python
# Просмотр баз данных (общие с Trino)
spark.sql("SHOW DATABASES").show()

# Просмотр таблиц
spark.sql("SHOW TABLES IN default").show()

# Создание managed таблицы
spark.sql("""
    CREATE TABLE IF NOT EXISTS default.daily_orders (
        dt DATE,
        order_count BIGINT,
        total_revenue DECIMAL(18,2)
    )
    USING PARQUET
    PARTITIONED BY (dt)
    LOCATION 's3a://datalake/warehouse/daily_orders'
""")
```

### Delta Lake

```python
from delta.tables import DeltaTable

# Запись Delta таблицы
df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("dt") \
    .save("s3a://datalake/delta/orders/")

# Чтение Delta таблицы
delta_df = spark.read.format("delta").load("s3a://datalake/delta/orders/")
print(f"Delta table rows: {delta_df.count()}")

# Time Travel - чтение предыдущей версии
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("s3a://datalake/delta/orders/")

# История изменений
delta_table = DeltaTable.forPath(spark, "s3a://datalake/delta/orders/")
delta_table.history().show()

# MERGE (upsert)
delta_table.alias("target").merge(
    updates_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

---

## Оптимизация запросов для ограниченных ресурсов

При работе с большими данными на малых ресурсах важно писать код правильно:

```python
from pyspark.sql import SparkSession

# Создание сессии с настройками для малых ресурсов
spark = SparkSession.builder \
    .appName("LowMemory-1TB-Processing") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.sql.adaptive.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# === ПРАВИЛО 1: Фильтруй как можно раньше ===
# ПЛОХО: читает все данные
df = spark.read.parquet("s3a://datalake/topics/order-events/")
df_filtered = df.filter(df.dt >= "2024-01-01")

# ХОРОШО: использует partition pruning
df = spark.read.parquet("s3a://datalake/topics/order-events/") \
    .filter("dt >= '2024-01-01'")

# === ПРАВИЛО 2: Выбирай только нужные колонки ===
# ПЛОХО: читает все 34 колонки
df = spark.read.parquet("s3a://datalake/topics/order-events/")

# ХОРОШО: читает только нужные
df = spark.read.parquet("s3a://datalake/topics/order-events/") \
    .select("order_id", "dt", "total_amount", "status")

# === ПРАВИЛО 3: Repartition перед тяжелыми операциями ===
# Увеличиваем партиции для равномерного распределения
df = df.repartition(500)

# === ПРАВИЛО 4: Persist с DISK_ONLY для промежуточных результатов ===
from pyspark import StorageLevel

# Если нужно использовать DataFrame несколько раз
df_aggregated = df.groupBy("dt").agg({"total_amount": "sum"})
df_aggregated.persist(StorageLevel.DISK_ONLY)  # Не MEMORY_ONLY!

# Использовать
df_aggregated.show()
df_aggregated.write.parquet("s3a://datalake/output/")

# Освободить
df_aggregated.unpersist()

# === ПРАВИЛО 5: Избегай collect() на больших данных ===
# ПЛОХО: загружает все в память драйвера
all_data = df.collect()

# ХОРОШО: используй limit или записывай в файл
sample = df.limit(1000).collect()
df.write.parquet("s3a://datalake/output/")

# === ПРАВИЛО 6: Используй coalesce вместо repartition для уменьшения ===
# При записи результата уменьшаем количество файлов
df_result.coalesce(10).write.parquet("s3a://datalake/output/")

# === ПРАВИЛО 7: Checkpoint для очень длинных pipeline ===
spark.sparkContext.setCheckpointDir("s3a://datalake/checkpoints/")

df_step1 = df.filter(...).select(...)
df_step1.checkpoint()  # Сбрасывает на диск, очищает lineage

df_step2 = df_step1.groupBy(...).agg(...)
```

### Обработка 1TB данных по частям

Если данные партиционированы по дате, обрабатывайте по частям:

```python
from datetime import datetime, timedelta

# Обработка по месяцам
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

current = start_date
while current <= end_date:
    month_start = current.strftime("%Y-%m-%d")
    month_end = (current + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    month_end_str = month_end.strftime("%Y-%m-%d")

    print(f"Processing: {month_start} to {month_end_str}")

    # Читаем только один месяц
    df_month = spark.read.parquet("s3a://datalake/topics/order-events/") \
        .filter(f"dt >= '{month_start}' AND dt <= '{month_end_str}'") \
        .select("order_id", "dt", "total_amount", "status")

    # Обрабатываем
    result = df_month.groupBy("dt", "status").agg({"total_amount": "sum"})

    # Записываем результат
    result.write \
        .mode("append") \
        .partitionBy("dt") \
        .parquet(f"s3a://datalake/analytics/monthly_summary/")

    # Очищаем кэш
    spark.catalog.clearCache()

    # Следующий месяц
    current = (current + timedelta(days=32)).replace(day=1)

print("Done!")
```

### Мониторинг памяти

```python
# Проверка использования памяти
def print_memory_usage():
    sc = spark.sparkContext
    print(f"Storage Memory Used: {sc._jvm.org.apache.spark.SparkEnv.get().blockManager().memoryStore().memoryUsed() / 1024 / 1024:.2f} MB")

# Принудительная очистка
def force_gc():
    spark.sparkContext._jvm.System.gc()
    import gc
    gc.collect()
```

---

## Health Check

```bash
# Spark Master
curl -f http://spark-master.company.com:8080/

# Spark Worker
curl -f http://spark-worker-1.company.com:8081/

# Проверка через Master API
curl -s http://spark-master.company.com:8080/json/ | jq '.workers | length'
# Должно вернуть количество workers

# Docker healthcheck
docker inspect spark-master | grep -A 5 Health
docker inspect spark-worker | grep -A 5 Health
```

---

## Мониторинг

### Spark Master UI

- URL: `http://spark-master.company.com:8080`
- Показывает: workers, running applications, completed applications

### Spark Application UI

- URL: `http://localhost:4040` (во время работы приложения)
- Показывает: stages, tasks, storage, executors

### Metrics (Prometheus)

Добавить в `spark-defaults.conf`:

```properties
spark.metrics.conf.*.sink.prometheus.class=org.apache.spark.metrics.sink.PrometheusSink
spark.metrics.conf.*.sink.prometheus.port=8090
spark.metrics.conf.master.source.jvm.class=org.apache.spark.metrics.source.JvmSource
spark.metrics.conf.worker.source.jvm.class=org.apache.spark.metrics.source.JvmSource
```

---

## Troubleshooting

### Worker не подключается к Master

```bash
# Проверить доступность Master
nc -zv spark-master.company.com 7077

# Проверить логи worker
docker logs spark-worker

# Проверить DNS
ping spark-master.company.com
```

### ClassNotFoundException: S3AFileSystem

```bash
# Проверить наличие JAR файлов
docker exec spark-master ls -la /opt/bitnami/spark/jars/ | grep -E "(hadoop-aws|aws-java-sdk)"

# Должны быть:
# hadoop-aws-3.3.4.jar
# aws-java-sdk-bundle-1.12.262.jar
```

### Connection refused to hive-metastore

```bash
# Проверить доступность Hive Metastore
nc -zv hive-metastore.company.com 9083

# Проверить что Hive Metastore запущен
curl -f http://hive-metastore.company.com:9083/
```

### Out of Memory на Worker

```bash
# Увеличить память worker в .env
SPARK_WORKER_MEMORY=64g

# Или уменьшить память executor в приложении
spark.executor.memory=4g
```

### Slow S3A operations

```properties
# Увеличить параллелизм в spark-defaults.conf
spark.hadoop.fs.s3a.threads.max=64
spark.hadoop.fs.s3a.connection.maximum=100
spark.hadoop.fs.s3a.fast.upload=true
spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer
```

### Типичные ошибки и решения

| Ошибка | Причина | Решение |
|--------|---------|---------|
| `java.lang.OutOfMemoryError: Java heap space` | Executor память исчерпана | Уменьшить `spark.executor.memory`, увеличить `spark.sql.shuffle.partitions` |
| `java.lang.OutOfMemoryError: GC overhead limit exceeded` | GC не справляется | Добавить `-XX:+UseG1GC`, уменьшить параллелизм |
| `No space left on device` | Диск для spill заполнен | Очистить `/data/spark-temp`, добавить диск |
| `Container killed by YARN for exceeding memory limits` | memoryOverhead мал | Увеличить `spark.executor.memoryOverhead` |
| `Task not serializable` | Closure содержит несериализуемые объекты | Использовать broadcast переменные |
| `FetchFailedException` | Shuffle файлы недоступны | Увеличить `spark.shuffle.io.maxRetries` |

---

## Docker Compose (для локальной разработки)

Добавить в `compose.yaml`:

```yaml
  spark-master:
    build:
      context: ./data-lake/spark/master
      dockerfile: Dockerfile
      args:
        NEXUS_MAVEN_URL: ${NEXUS_MAVEN_URL:-https://nexus.company.com/repository/maven-public}
    hostname: spark-master
    ports:
      - "7077:7077"
      - "8085:8080"
    networks:
      - kafka
    environment:
      SPARK_MODE: master
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    depends_on:
      - minio
      - hive-metastore
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/"]
      interval: 30s
      timeout: 10s
      retries: 3

  spark-worker-1:
    build:
      context: ./data-lake/spark/worker
      dockerfile: Dockerfile
      args:
        NEXUS_MAVEN_URL: ${NEXUS_MAVEN_URL:-https://nexus.company.com/repository/maven-public}
    hostname: spark-worker-1
    ports:
      - "8086:8081"
    networks:
      - kafka
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_CORES: ${SPARK_WORKER_CORES:-4}
      SPARK_WORKER_MEMORY: ${SPARK_WORKER_MEMORY:-8g}
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    depends_on:
      - spark-master
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/"]
      interval: 30s
      timeout: 10s
      retries: 3

  spark-worker-2:
    build:
      context: ./data-lake/spark/worker
      dockerfile: Dockerfile
      args:
        NEXUS_MAVEN_URL: ${NEXUS_MAVEN_URL:-https://nexus.company.com/repository/maven-public}
    hostname: spark-worker-2
    ports:
      - "8087:8081"
    networks:
      - kafka
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_CORES: ${SPARK_WORKER_CORES:-4}
      SPARK_WORKER_MEMORY: ${SPARK_WORKER_MEMORY:-8g}
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    depends_on:
      - spark-master
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/"]
      interval: 30s
      timeout: 10s
      retries: 3
```

---

## Следующий шаг

После успешного развертывания Spark переходите к:
👉 [Jupyter](jupyter.md)
