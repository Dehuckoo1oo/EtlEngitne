# Apache Spark - MVP Deployment

## Назначение

Распределенный движок для обработки больших данных с поддержкой batch и stream processing. Интегрируется с существующим Data Lake через Hive Metastore и MinIO.

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
spark.hadoop.fs.s3a.endpoint=http://minio.company.com:9000
spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}
spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=false
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
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

### Оптимизация запросов в коде

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

### Типичные ошибки и решения

| Ошибка | Причина | Решение |
|--------|---------|---------|
| `java.lang.OutOfMemoryError: Java heap space` | Executor память исчерпана | Уменьшить `spark.executor.memory`, увеличить `spark.sql.shuffle.partitions` |
| `java.lang.OutOfMemoryError: GC overhead limit exceeded` | GC не справляется | Добавить `-XX:+UseG1GC`, уменьшить параллелизм |
| `No space left on device` | Диск для spill заполнен | Очистить `/data/spark-temp`, добавить диск |
| `Container killed by YARN for exceeding memory limits` | memoryOverhead мал | Увеличить `spark.executor.memoryOverhead` |
| `Task not serializable` | Closure содержит несериализуемые объекты | Использовать broadcast переменные |
| `FetchFailedException` | Shuffle файлы недоступны | Увеличить `spark.shuffle.io.maxRetries` |

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
│   │   └── hive-site.xml
│   ├── .env.example
│   └── .gitlab-ci.yml
├── worker/
│   ├── Dockerfile
│   ├── config/
│   │   ├── spark-defaults.conf
│   │   ├── spark-env.sh
│   │   ├── core-site.xml
│   │   └── hive-site.xml
│   ├── .env.example
│   └── .gitlab-ci.yml
└── README.md
```

---

## Dockerfile

### Spark Master

`spark/master/Dockerfile`:

```dockerfile
FROM bitnamilegacy/spark:3.5.0

USER root

# Версии JAR (совместимые с Spark 3.5.x / Hadoop 3.3.4)
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DELTA_VERSION=3.2.0
ENV SCALA_VERSION=2.12

# Установка утилит
RUN apt-get update && apt-get install -y curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Скачивание JAR для S3A (MinIO)
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o /opt/bitnami/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Скачивание JAR для Delta Lake
RUN curl -sL https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    -o /opt/bitnami/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
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
FROM bitnamilegacy/spark:3.5.0

USER root

# Версии JAR (должны совпадать с Master)
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DELTA_VERSION=3.2.0
ENV SCALA_VERSION=2.12

# Установка утилит
RUN apt-get update && apt-get install -y curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Скачивание JAR для S3A (MinIO)
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o /opt/bitnami/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Скачивание JAR для Delta Lake
RUN curl -sL https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    -o /opt/bitnami/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
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
spark.hadoop.fs.s3a.endpoint=http://minio.company.com:9000
spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}
spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=false
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider

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

### spark-env.sh

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
        <value>http://minio.company.com:9000</value>
    </property>
    <property>
        <name>fs.s3a.access.key</name>
        <value>${env.AWS_ACCESS_KEY_ID}</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>${env.AWS_SECRET_ACCESS_KEY}</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.connection.ssl.enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
    <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
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

## Environment Variables

### .env.example для Master

`spark/master/.env.example`:

```bash
# Spark Master settings
SPARK_MODE=master
SPARK_MASTER_HOST=spark-master.company.com
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=8080

# S3/MinIO credentials
AWS_ACCESS_KEY_ID=spark-user
AWS_SECRET_ACCESS_KEY=<SPARK_MINIO_PASSWORD>

# Java settings
SPARK_DAEMON_JAVA_OPTS=-Xmx4g
```

### .env.example для Worker

`spark/worker/.env.example`:

```bash
# Spark Worker settings
SPARK_MODE=worker
SPARK_MASTER_URL=spark://spark-master.company.com:7077
SPARK_WORKER_CORES=14
SPARK_WORKER_MEMORY=56g
SPARK_WORKER_WEBUI_PORT=8081

# S3/MinIO credentials
AWS_ACCESS_KEY_ID=spark-user
AWS_SECRET_ACCESS_KEY=<SPARK_MINIO_PASSWORD>

# Java settings
SPARK_DAEMON_JAVA_OPTS=-Xmx4g
```

---

## Порядок развертывания

### Зависимости

Spark требует:
1. ✅ MinIO (S3 storage)
2. ✅ Hive Metastore (каталог таблиц)

### Этап 1: Создание пользователя MinIO для Spark

```bash
# На машине с доступом к MinIO
mc alias set datalake http://minio.company.com:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

# Создать пользователя для Spark
mc admin user add datalake spark-user <SPARK_MINIO_PASSWORD>

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

```bash
# На машине spark-master.company.com
cd spark/master
cp .env.example .env
# Отредактировать .env - указать пароль MinIO

docker build -t spark-master:latest .

docker run -d \
    --name spark-master \
    --hostname spark-master \
    --restart=always \
    --env-file .env \
    -p 7077:7077 \
    -p 8080:8080 \
    spark-master:latest
```

### Этап 3: Развертывание Spark Workers

```bash
# На каждой машине spark-worker-N.company.com
cd spark/worker
cp .env.example .env
# Отредактировать .env - указать пароль MinIO и адрес Master

docker build -t spark-worker:latest .

docker run -d \
    --name spark-worker \
    --hostname spark-worker-1 \
    --restart=always \
    --env-file .env \
    -p 8081:8081 \
    spark-worker:latest
```

### Этап 4: Проверка кластера

```bash
# Проверить Master UI
curl -f http://spark-master.company.com:8080/

# Должны быть видны все workers
# Статус: ALIVE, Workers: N
```

---

## Интеграция с Jupyter

Подробная документация по Jupyter с PySpark: [jupyter.md](jupyter.md)

### Ключевые настройки Jupyter для работы со Spark

**ВАЖНО**: Версии JAR должны совпадать между Jupyter и Spark Cluster!

| Компонент | Версия |
|-----------|--------|
| Spark | 3.5.0 |
| Hadoop AWS | 3.3.4 |
| AWS SDK | 1.12.262 |
| Delta Lake | 3.2.0 |
| Scala | 2.12 |

### Dockerfile (ключевые части)

```dockerfile
# Версия Spark должна совпадать с кластером
ENV SPARK_VERSION=3.5.0

# PYTHONPATH для нахождения PySpark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# JAR версии должны совпадать с кластером
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DELTA_VERSION=3.2.0
```

### requirements.txt

```txt
# PySpark (версия должна совпадать с кластером)
pyspark==3.5.0
delta-spark==3.2.0
findspark>=2.0.1
```

### spark-defaults.conf для Jupyter

```properties
# Подключение к кластеру
spark.master=spark://spark-master.company.com:7077

# Память для driver (на Jupyter)
spark.driver.memory=2g
spark.executor.memory=4g

# S3A/MinIO
spark.hadoop.fs.s3a.endpoint=https://minio.company.com:9000
spark.hadoop.fs.s3a.path.style.access=true

# Hive Metastore
spark.sql.catalogImplementation=hive
spark.hadoop.hive.metastore.uris=thrift://hive-metastore.company.com:9083

# Delta Lake
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# Logging (убрать warnings)
spark.sql.debug.maxToStringFields=100
```

### Пример подключения из Jupyter

```python
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Конфигурация загружается из spark-defaults.conf
spark = SparkSession.builder \
    .appName("DataLake-Analysis") \
    .getOrCreate()

# Убрать warnings
spark.sparkContext.setLogLevel("ERROR")

# Чтение из MinIO
df = spark.read.parquet("s3a://datalake/topics/order-events/")
df.show(5)
```

### compose.yaml (для локальной разработки)

```yaml
jupyter:
  build:
    context: ./data-lake/jupyter
    dockerfile: Dockerfile
  hostname: jupyter
  ports:
    - "8888:8888"
    - "4040:4040"
  networks:
    - kafka
  environment:
    JUPYTER_ENABLE_LAB: "yes"
    JUPYTER_TOKEN: "datalake"
    AWS_ACCESS_KEY_ID: minioadmin
    AWS_SECRET_ACCESS_KEY: minioadmin
    SPARK_MASTER_URL: spark://spark-master:7077
  depends_on:
    - minio
    - hive-metastore
    - spark-master
```

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
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.company.com:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
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

## Health Checks

```bash
# Spark Master
curl -f http://spark-master.company.com:8080/

# Spark Worker
curl -f http://spark-worker-1.company.com:8081/

# Проверка через Master API
curl -s http://spark-master.company.com:8080/json/ | jq '.workers | length'
# Должно вернуть количество workers
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

---

## Docker Compose (для локальной разработки)

Добавить в `compose.yaml`:

```yaml
  spark-master:
    build:
      context: ./data-lake/spark/master
      dockerfile: Dockerfile
    hostname: spark-master
    ports:
      - "7077:7077"
      - "8085:8080"
    networks:
      - kafka
    environment:
      SPARK_MODE: master
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
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
    hostname: spark-worker-1
    ports:
      - "8086:8081"
    networks:
      - kafka
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_CORES: 4
      SPARK_WORKER_MEMORY: 8g
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
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
    hostname: spark-worker-2
    ports:
      - "8087:8081"
    networks:
      - kafka
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_CORES: 4
      SPARK_WORKER_MEMORY: 8g
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
    depends_on:
      - spark-master
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/"]
      interval: 30s
      timeout: 10s
      retries: 3
```

---

## Следующие шаги (после MVP)

- **Spark History Server**: Просмотр завершенных приложений
- **Resource Manager**: YARN или Kubernetes для лучшего управления ресурсами
- **Spark Thrift Server**: JDBC/ODBC доступ к Spark SQL
- **Автомасштабирование**: Kubernetes + spark-operator
- **Structured Streaming**: Real-time обработка из Kafka

См. [advanced/next-stage.md](advanced/next-stage.md)
