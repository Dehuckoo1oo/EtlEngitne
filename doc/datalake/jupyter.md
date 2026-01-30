# Jupyter - MVP Deployment

## Назначение

Интерактивная среда для анализа данных Data Lake через:
- **SQL** (Trino) — быстрые аналитические запросы
- **PySpark** (Spark Cluster) — распределенная обработка больших данных
- **Прямой доступ** (PyArrow/S3FS) — чтение Parquet файлов из MinIO

---

## Требования к машине

| Параметр | Значение |
|----------|----------|
| CPU | 4 cores |
| RAM | 16 GB |
| Storage | 200 GB SSD |
| Network | 1 Gbit/s |
| Hostname | jupyter.company.com |
| Firewall | Открыты порты: 8888, 4040, 4041, 4042 |

**Важно**: Hostname `jupyter.company.com` должен быть резолвим с Spark worker nodes для обратной связи executors → driver.

---

## GitLab структура

```
jupyter/
├── Dockerfile
├── conf/
│   ├── spark-defaults.conf       # Конфигурация PySpark
│   ├── pip.conf                  # Nexus PyPI proxy
│   └── minio-root-ca.crt         # Корневой сертификат MinIO
├── requirements.txt
├── .env.example
├── .gitlab-ci.yml
└── README.md
```

**Важно**: Файл `minio-root-ca.crt` должен содержать корневой сертификат вашего MinIO сервера в формате PEM.

---

## Dockerfile

`jupyter/Dockerfile`:

```dockerfile
FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# === FIX SPARK_HOME ===
# Base image uses /usr/local/spark, but delta-spark expects /opt/spark
RUN mkdir -p /opt && ln -s /usr/local/spark /opt/spark
ENV SPARK_HOME=/usr/local/spark

# === УСТАНОВКА КОРНЕВОГО СЕРТИФИКАТА MINIO ===
COPY conf/minio-root-ca.crt /usr/local/share/ca-certificates/minio-root-ca.crt
RUN update-ca-certificates

# === JAR ЗАВИСИМОСТИ ===
# Версии должны совпадать с Spark Cluster
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV DELTA_VERSION=3.2.0
ENV SCALA_VERSION=2.12

RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
    -o ${SPARK_HOME}/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -sL https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    -o ${SPARK_HOME}/jars/delta-storage-${DELTA_VERSION}.jar

# === SPARK КОНФИГУРАЦИЯ ===
COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# === PIP КОНФИГУРАЦИЯ ===
COPY conf/pip.conf /etc/pip.conf

USER jovyan

# === PYTHON ЗАВИСИМОСТИ ===
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create notebooks directory
RUN mkdir -p /home/jovyan/work

WORKDIR /home/jovyan/work

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8888/api || exit 1

EXPOSE 8888 4040 4041 4042

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
```

**Примечание**: Порты 4041 (driver) и 4042 (blockManager) необходимы для обратной связи executors → driver в client mode.

**Примечание**: Базовый образ `jupyter/pyspark-notebook:spark-3.5.0` уже включает Java (JDK 17) и Spark 3.5.0. В MVP отключена аутентификация (`token=''`). Авторизация пользователей — шаг 2 (см. [advanced/next-stage.md](advanced/next-stage.md)).

---

## Конфигурационные файлы

### pip.conf

`conf/pip.conf`:

```ini
[global]
index-url = https://nexus.company.com/repository/pypi/simple
trusted-host = nexus.company.com
```

### spark-defaults.conf

`conf/spark-defaults.conf`:

```properties
# === SPARK CLUSTER CONNECTION ===
spark.master=spark://spark-master.company.com:7077

# === DRIVER NETWORK CONFIGURATION ===
# КРИТИЧНО для client mode: executors должны подключаться обратно к driver
# Укажите hostname/IP сервера Jupyter, который виден worker nodes
spark.driver.host=jupyter.company.com
spark.driver.bindAddress=0.0.0.0
spark.driver.port=4041
spark.blockManager.port=4042

# === MEMORY CONFIGURATION (for driver on Jupyter) ===
spark.driver.memory=2g
spark.executor.memory=4g
spark.executor.memoryOverhead=1g

# Memory fractions
spark.memory.fraction=0.4
spark.memory.storageFraction=0.3

# === PARALLELISM ===
spark.sql.shuffle.partitions=200
spark.default.parallelism=12
spark.executor.cores=2

# === ADAPTIVE QUERY EXECUTION ===
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true

# === COMPRESSION ===
spark.sql.parquet.compression.codec=snappy
spark.io.compression.codec=lz4
spark.shuffle.compress=true
spark.rdd.compress=true

# === BROADCAST ===
spark.sql.autoBroadcastJoinThreshold=10MB

# === NETWORK & TIMEOUTS ===
spark.network.timeout=600s
spark.executor.heartbeatInterval=60s
spark.sql.broadcastTimeout=600s

# === S3A/MinIO Configuration ===
spark.hadoop.fs.s3a.endpoint=https://minio.company.com:9000
spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}
spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.fast.upload=true
spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer

# === HIVE METASTORE ===
spark.sql.catalogImplementation=hive
spark.hadoop.hive.metastore.uris=thrift://hive-metastore.company.com:9083
spark.sql.warehouse.dir=s3a://datalake/warehouse

# === DELTA LAKE ===
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# === SERIALIZATION ===
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max=256m

# === UI ===
spark.ui.enabled=true
spark.ui.port=4040

# === LOGGING (reduce noise) ===
spark.sql.debug.maxToStringFields=100
```

### Описание параметров spark-defaults.conf

#### Подключение к кластеру

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.master` | `spark://spark-master:7077` | URL Spark Master. Формат `spark://host:port` для standalone кластера. Альтернативы: `local[*]` (локальный режим), `yarn`, `k8s://...` |

#### Сетевая конфигурация Driver (критично для client mode)

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.driver.host` | `jupyter.company.com` | Hostname/IP сервера Jupyter, который виден с worker nodes. Executors используют этот адрес для обратного подключения к driver. **Без этого параметра задачи зависнут** |
| `spark.driver.bindAddress` | `0.0.0.0` | IP-адрес для привязки сокета driver. `0.0.0.0` означает прослушивание на всех интерфейсах |
| `spark.driver.port` | `4041` | Порт для RPC-коммуникации между driver и executors. Должен быть открыт в firewall и проброшен в Docker |
| `spark.blockManager.port` | `4042` | Порт для передачи блоков данных между driver и executors. Используется для broadcast переменных и collect операций |

#### Конфигурация памяти

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.driver.memory` | `2g` | Объем heap-памяти для JVM driver (Jupyter). Увеличьте при работе с большими collect() или broadcast |
| `spark.executor.memory` | `4g` | Объем heap-памяти для каждого executor. Основная память для обработки данных |
| `spark.executor.memoryOverhead` | `1g` | Дополнительная память вне heap (off-heap, Python, контейнерные накладные расходы). Формула: `max(384MB, 0.1 * executor.memory)` |
| `spark.memory.fraction` | `0.4` | Доля heap-памяти executor для execution и storage (после вычета 300MB reserved). По умолчанию 0.6. Уменьшено для стабильности |
| `spark.memory.storageFraction` | `0.3` | Доля от `memory.fraction` для хранения кэшированных RDD. Остальное — для shuffle и joins |

#### Параллелизм

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.sql.shuffle.partitions` | `200` | Количество партиций после shuffle операций (joins, aggregations). По умолчанию 200. Для малых данных уменьшите до 20-50 |
| `spark.default.parallelism` | `12` | Параллелизм для RDD операций без явного указания. Рекомендация: 2-3× количество cores в кластере |
| `spark.executor.cores` | `2` | Количество CPU cores на каждый executor. Влияет на параллельные задачи внутри executor |

#### Adaptive Query Execution (AQE)

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.sql.adaptive.enabled` | `true` | Включает адаптивную оптимизацию запросов на основе runtime статистики. Рекомендуется для Spark 3.x |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Автоматически объединяет мелкие партиции после shuffle для уменьшения overhead |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Оптимизирует joins при неравномерном распределении данных (data skew), разбивая большие партиции |

#### Сжатие данных

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.sql.parquet.compression.codec` | `snappy` | Кодек сжатия для записи Parquet файлов. Варианты: `snappy` (быстрый), `gzip` (лучшее сжатие), `zstd`, `lz4`, `none` |
| `spark.io.compression.codec` | `lz4` | Кодек для внутреннего сжатия (RDD сериализация). LZ4 — оптимальный баланс скорости и сжатия |
| `spark.shuffle.compress` | `true` | Сжимать shuffle output файлы. Уменьшает disk I/O и network transfer |
| `spark.rdd.compress` | `true` | Сжимать сериализованные RDD партиции. Уменьшает память за счёт CPU |

#### Broadcast

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` | Максимальный размер таблицы для broadcast join. Таблицы меньше этого размера рассылаются всем executors. `-1` отключает |

#### Сетевые таймауты

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.network.timeout` | `600s` | Таймаут для всех сетевых операций. Увеличьте при медленной сети или больших данных |
| `spark.executor.heartbeatInterval` | `60s` | Интервал heartbeat от executor к driver. Должен быть < `network.timeout` |
| `spark.sql.broadcastTimeout` | `600s` | Таймаут ожидания broadcast данных на executors. Увеличьте для больших broadcast таблиц |

#### S3A/MinIO конфигурация

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.hadoop.fs.s3a.endpoint` | `https://minio:9000` | URL S3-совместимого хранилища (MinIO, Ceph, etc.) |
| `spark.hadoop.fs.s3a.access.key` | `${AWS_ACCESS_KEY_ID}` | Access Key для аутентификации. Подставляется из переменной окружения |
| `spark.hadoop.fs.s3a.secret.key` | `${AWS_SECRET_ACCESS_KEY}` | Secret Key для аутентификации. Подставляется из переменной окружения |
| `spark.hadoop.fs.s3a.path.style.access` | `true` | Использовать path-style URLs (`endpoint/bucket/key`) вместо virtual-hosted (`bucket.endpoint/key`). Обязательно для MinIO |
| `spark.hadoop.fs.s3a.connection.ssl.enabled` | `true` | Использовать HTTPS для подключения к S3. Для HTTP укажите `false` |
| `spark.hadoop.fs.s3a.impl` | `...S3AFileSystem` | Класс реализации S3A FileSystem. Стандартное значение для Hadoop |
| `spark.hadoop.fs.s3a.aws.credentials.provider` | `...SimpleAWSCredentialsProvider` | Провайдер credentials. Simple — из конфига, можно использовать `EnvironmentVariableCredentialsProvider` |
| `spark.hadoop.fs.s3a.fast.upload` | `true` | Включает параллельную загрузку данных в S3. Значительно ускоряет запись больших файлов |
| `spark.hadoop.fs.s3a.fast.upload.buffer` | `bytebuffer` | Тип буфера для fast upload: `bytebuffer` (heap), `array`, `disk`. ByteBuffer оптимален для памяти |

#### Hive Metastore

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.sql.catalogImplementation` | `hive` | Использовать Hive Metastore для хранения метаданных таблиц. Альтернатива: `in-memory` |
| `spark.hadoop.hive.metastore.uris` | `thrift://hive-metastore:9083` | URI Hive Metastore сервиса. Формат Thrift протокола |
| `spark.sql.warehouse.dir` | `s3a://datalake/warehouse` | Директория по умолчанию для managed таблиц Hive |

#### Delta Lake

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.sql.extensions` | `...DeltaSparkSessionExtension` | Регистрирует Delta Lake SQL расширения (MERGE, VACUUM, TIME TRAVEL и др.) |
| `spark.sql.catalog.spark_catalog` | `...DeltaCatalog` | Заменяет стандартный каталог на Delta-aware версию для поддержки Delta таблиц |

#### Сериализация

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.serializer` | `...KryoSerializer` | Сериализатор для RDD данных и shuffle. Kryo в 10× быстрее стандартного Java serializer |
| `spark.kryoserializer.buffer.max` | `256m` | Максимальный размер буфера Kryo. Увеличьте при ошибках "buffer limit exceeded" |

#### Spark UI

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.ui.enabled` | `true` | Включить веб-интерфейс Spark Application. Показывает stages, tasks, storage, executors |
| `spark.ui.port` | `4040` | Порт для Spark UI. Если занят, автоматически пробует 4041, 4042... |

#### Логирование

| Параметр | Значение | Описание |
|----------|----------|----------|
| `spark.sql.debug.maxToStringFields` | `100` | Максимальное количество полей в toString() для DataFrame. Уменьшает шум в логах для wide таблиц |

**Примечание**: Для локальной разработки используйте `http://minio:9000` вместо `https://minio.company.com:9000`.

---

## requirements.txt

`requirements.txt`:

```txt
# === Data Lake Integration ===
pyarrow>=14.0.0
pandas>=2.0.0
s3fs>=2023.12.0
boto3>=1.34.0

# === Trino Client ===
trino>=0.328.0
sqlalchemy>=2.0.25
sqlalchemy-trino>=0.5.0

# === Delta Lake (версия должна совпадать с Spark Cluster) ===
# pyspark уже включен в базовый образ pyspark-notebook
delta-spark==3.2.0

# === Visualization ===
matplotlib>=3.8.0
seaborn>=0.13.0
plotly>=5.18.0

# === Kafka (опционально, для replay данных) ===
confluent-kafka[avro,schemaregistry]>=2.3.0

# === JupyterLab ===
jupyterlab>=4.0.0
```

---

## Environment Variables

`.env.example`:

```bash
# === JupyterLab ===
JUPYTER_ENABLE_LAB=yes
JUPYTER_TOKEN=datalake

# === MinIO/S3 Access ===
AWS_ACCESS_KEY_ID=jupyter
AWS_SECRET_ACCESS_KEY=<PASSWORD_FROM_MINIO>
AWS_ENDPOINT_URL=https://minio.company.com:9000
AWS_REGION=us-east-1

# === Trino Connection ===
TRINO_HOST=trino.company.com
TRINO_PORT=8080
TRINO_USER=analyst

# === Spark Cluster ===
SPARK_MASTER_URL=spark://spark-master.company.com:7077
SPARK_HOME=/usr/local/spark
```

---

## Build & Deploy

### Вариант 1: Вручную

```bash
# На машине jupyter.company.com

# 1. Клонировать репозиторий
git clone https://gitlab.company.com/datalake/infrastructure.git
cd infrastructure/jupyter

# 2. Build образа
docker build -t jupyter-datalake:latest .

# 3. Создать .env
cp .env.example .env
nano .env  # Установить credentials

# 4. Запуск контейнера
docker run -d \
  --name jupyter \
  --restart unless-stopped \
  -p 8888:8888 \
  -p 4040:4040 \
  -p 4041:4041 \
  -p 4042:4042 \
  --env-file .env \
  -v /mnt/data/jupyter/notebooks:/home/jovyan/work \
  jupyter-datalake:latest

# 5. Health check
curl -f http://jupyter.company.com:8888/api
```

### Вариант 2: GitLab CI/CD

`.gitlab-ci.yml`:

```yaml
variables:
  GIT_STRATEGY: clone

stages:
  - deploy

Deploy Jupyter to TEST:
  stage: deploy
  tags: [your_runner_tag]
  needs: []
  when: manual
  allow_failure: false
  before_script:
    - SRV_APP="jupyter.company.com"
  script:
    - |
      ImageName=jupyter-datalake:latest
      ContainerName=jupyter

      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo 'Собираем Docker образ...'
      docker build -t ${ImageName} .

      echo 'Останавливаем и удаляем старый контейнер...'
      docker stop ${ContainerName} && docker rm ${ContainerName} || echo 'Контейнера нет'

      echo 'Создаем директории для notebooks...'
      mkdir -p /mnt/data/jupyter/notebooks

      echo 'Создаем новый контейнер...'
      docker run \
        -d \
        --name ${ContainerName} \
        --restart=always \
        -e JUPYTER_TOKEN=${JUPYTER_TOKEN} \
        -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
        -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
        -e AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL} \
        -e SPARK_MASTER_URL=${SPARK_MASTER_URL} \
        -e TRINO_HOST=${TRINO_HOST} \
        -p 8888:8888 \
        -p 4040:4040 \
        -p 4041:4041 \
        -p 4042:4042 \
        -v /mnt/data/jupyter/notebooks:/home/jovyan/work \
        -h ${SRV_APP} \
        ${ImageName}

      echo 'ГОТОВО!'
      sleep 10
      curl -f http://localhost:8888/api || echo 'Jupyter API еще не доступен'

      DEPLOY_SCRIPT

      echo "Копируем на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "mkdir -p ~/docker_build_${CI_COMMIT_SHORT_SHA}"
      rsync -avz ./ svc_user@${SRV_APP}:~/docker_build_${CI_COMMIT_SHORT_SHA}

      echo "Запускаем деплой..."
      ssh svc_user@${SRV_APP} "cd ~/docker_build_${CI_COMMIT_SHORT_SHA}/ && chmod +x ./build.sh && ./build.sh"

      echo "Удаляем временные файлы..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_COMMIT_SHORT_SHA}"
```

**GitLab CI/CD Variables** (`Settings > CI/CD > Variables`):

| Переменная | Значение | Тип |
|-----------|----------|-----|
| `JUPYTER_TOKEN` | `<secure_token>` | Variable (Masked) |
| `AWS_ACCESS_KEY_ID` | `jupyter` | Variable |
| `AWS_SECRET_ACCESS_KEY` | `<password_from_minio>` | Variable (Masked) |
| `AWS_ENDPOINT_URL` | `https://minio.company.com:9000` | Variable |
| `SPARK_MASTER_URL` | `spark://spark-master.company.com:7077` | Variable |
| `TRINO_HOST` | `trino.company.com` | Variable |

---

## Первоначальная настройка MinIO access

```bash
mc admin user add datalake jupyter <SECURE_PASSWORD>

cat > jupyter-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::datalake/*",
        "arn:aws:s3:::datalake"
      ]
    }
  ]
}
EOF

mc admin policy create datalake jupyter-policy jupyter-policy.json
mc admin policy attach datalake jupyter-policy --user jupyter
```

---

## Примеры Notebooks

### 1. Подключение к Spark Cluster (PySpark)

Создать `01_pyspark_connection.ipynb`:

```python
from pyspark.sql import SparkSession

# Создание сессии - конфигурация загружается из spark-defaults.conf
spark = SparkSession.builder \
    .appName("DataLake-Analysis") \
    .getOrCreate()

# Убрать лишние warnings
spark.sparkContext.setLogLevel("ERROR")

print(f"Spark version: {spark.version}")
print(f"Spark master: {spark.sparkContext.master}")
print(f"Application ID: {spark.sparkContext.applicationId}")
print(f"\nSpark UI: http://localhost:4040")
print(f"Spark Master UI: http://spark-master.company.com:8080")
```

### 2. Чтение Parquet из MinIO через PySpark

```python
# Чтение Parquet файлов
df = spark.read.parquet("s3a://datalake/topics/order-events/")

print(f"Total records: {df.count()}")
df.printSchema()
df.show(5)
```

### 3. SQL запросы через PySpark

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

### 4. Работа с Hive Metastore

```python
# Просмотр баз данных (общие с Trino)
spark.sql("SHOW DATABASES").show()

# Просмотр таблиц
spark.sql("SHOW TABLES IN default").show()

# Создание managed таблицы
spark.sql("""
    CREATE TABLE IF NOT EXISTS default.daily_summary (
        dt DATE,
        order_count BIGINT,
        total_revenue DECIMAL(18,2)
    )
    USING PARQUET
    PARTITIONED BY (dt)
    LOCATION 's3a://datalake/warehouse/daily_summary'
""")
```

### 5. Delta Lake

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

# История изменений
delta_table = DeltaTable.forPath(spark, "s3a://datalake/delta/orders/")
delta_table.history().show()
```

### 6. Подключение к Trino

```python
from trino.dbapi import connect
import pandas as pd

conn = connect(
    host='trino.company.com',
    port=8080,
    user='analyst',
    catalog='iceberg',
    schema='default'
)

query = """
SELECT * FROM iceberg.sales.orders
WHERE dt = '2024-12-25'
LIMIT 100
"""

df = pd.read_sql(query, conn)
print(df.head())
```

### 7. Чтение Parquet через PyArrow (без Spark)

```python
import s3fs
import pandas as pd

s3 = s3fs.S3FileSystem(
    key='jupyter',
    secret='<PASSWORD>',
    client_kwargs={
        'endpoint_url': 'https://minio.company.com:9000',
        'region_name': 'us-east-1'
    }
)

df = pd.read_parquet(
    's3://datalake/topics/order-events/dt=2024-12-25/',
    filesystem=s3
)
print(df.head())
```

---

## Replay данных из MinIO в Kafka

Jupyter поддерживает функционал replay — загрузка Parquet файлов из MinIO обратно в Kafka для повторной обработки.

### Предварительные требования

#### 1. Создать топик для replay

**ВАЖНО**: Используйте отдельный топик для replay данных, чтобы не смешивать с live потоком.

```bash
# На машине с Kafka (или через docker exec)
kafka-topics.sh --bootstrap-server kafka-broker-1:9092 \
  --create \
  --topic order-events-replay \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete
```

Или через AKHQ/Kafka UI если доступен.

#### 2. Создать пользователя в MinIO (если еще не создан)

Пользователь `jupyter` должен иметь права на чтение данных из bucket `datalake`. См. раздел "Первоначальная настройка MinIO access" выше.

### Использование replay notebook

В Jupyter доступен notebook `replay-to-kafka.ipynb` со следующим функционалом:

1. **Просмотр доступных партиций** — показывает все calc_id/dt/hour партиции в MinIO
2. **Выбор партиции для загрузки** — указываете путь к нужной партиции
3. **Чтение Parquet файлов** — загрузка данных через PyArrow
4. **Отправка в Kafka** — Avro сериализация и отправка в топик `order-events-replay`

### Пример использования

```python
# Cell 1: Конфигурация
KAFKA_BOOTSTRAP = "kafka-broker-1.company.com:9092,kafka-broker-2.company.com:9092,kafka-broker-3.company.com:9092"
SCHEMA_REGISTRY = "http://schema-registry.company.com:8081"
TARGET_TOPIC = "order-events-replay"

# Cell 2: Показать доступные партиции
# Выполните ячейку для просмотра списка партиций

# Cell 3: Указать партицию для replay
PARTITION_PATH = "calc_id=20251224-180000/dt=2025-12-24/hour=18"

# Cell 6: Запустить replay
# Выполните ячейку для отправки данных в Kafka
```

### Конфигурация для корпоративного сервера

В notebook необходимо изменить следующие параметры:

| Параметр | Описание | Пример |
|----------|----------|--------|
| `KAFKA_BOOTSTRAP` | Адреса Kafka брокеров | `kafka-broker-1.company.com:9092,...` |
| `SCHEMA_REGISTRY` | URL Schema Registry | `http://schema-registry.company.com:8081` |
| `TARGET_TOPIC` | Топик для replay данных | `order-events-replay` |
| `s3fs endpoint_url` | URL MinIO | `https://minio.company.com:9000` |
| `s3fs key/secret` | Credentials MinIO | Пользователь `jupyter` |

### Проверка результата

1. **AKHQ**: Откройте `http://akhq.company.com:8080`, перейдите в топик `order-events-replay`
2. **kafka-console-consumer**:
   ```bash
   kafka-console-consumer.sh --bootstrap-server kafka-broker-1.company.com:9092 \
     --topic order-events-replay \
     --from-beginning \
     --max-messages 5
   ```

### Troubleshooting replay

#### SerializationError: Schema not found

```bash
# Проверить доступность Schema Registry
curl http://schema-registry.company.com:8081/subjects

# Проверить наличие схемы
curl http://schema-registry.company.com:8081/subjects/order-events-value/versions/latest
```

#### Connection refused to Kafka

```bash
# Проверить доступность брокеров из контейнера Jupyter
docker exec jupyter nc -zv kafka-broker-1.company.com 9092
```

#### SSL Certificate Error (MinIO)

Убедитесь, что корневой сертификат MinIO добавлен в образ Jupyter. См. Dockerfile.

---

## Health Check

```bash
# API health check
curl -f http://jupyter.company.com:8888/api

# Docker healthcheck
docker inspect jupyter | grep -A 5 Health

# Проверка подключения к Spark Cluster
curl -f http://spark-master.company.com:8080/
```

---

## Web UI

| Сервис | URL | Описание |
|--------|-----|----------|
| JupyterLab | http://jupyter.company.com:8888 | Основной интерфейс |
| Spark Application UI | http://jupyter.company.com:4040 | UI текущего Spark приложения |
| Spark Master UI | http://spark-master.company.com:8080 | UI кластера Spark |

### Сетевые порты Jupyter

| Порт | Назначение |
|------|------------|
| 8888 | JupyterLab Web UI |
| 4040 | Spark Application UI |
| 4041 | Spark Driver (для обратной связи executors → driver) |
| 4042 | Spark Block Manager (для передачи данных) |

**Важно**: Порты 4041 и 4042 должны быть доступны с worker nodes для работы Spark в client mode.

---

## Troubleshooting

### Spark запросы выполняются бесконечно долго (зависают)

**Симптомы**: `df.show()`, `df.count()` или любой action зависает без результата.

**Причина**: Executors на worker nodes не могут подключиться обратно к driver (Jupyter) для отправки результатов. Это классическая проблема Spark в client mode.

**Как работает Spark в client mode**:
1. Jupyter (driver) отправляет задачу на Spark Master
2. Master распределяет задачу на Workers
3. Workers запускают executors
4. Executors пытаются подключиться **ОБРАТНО** к driver для отправки результатов
5. Без указания `spark.driver.host` executors не знают, куда подключаться
6. Задача "зависает" бесконечно

**Решение**: Убедитесь, что в `spark-defaults.conf` указаны параметры сети driver:

```properties
# КРИТИЧНО для client mode
spark.driver.host=jupyter.company.com  # hostname, видимый worker nodes
spark.driver.bindAddress=0.0.0.0
spark.driver.port=4041
spark.blockManager.port=4042
```

**Также проверьте**:
1. Порты 4041 и 4042 открыты в firewall
2. Порты проброшены в Docker (`-p 4041:4041 -p 4042:4042`)
3. Hostname `jupyter.company.com` резолвится на worker nodes

**Диагностика**:
```bash
# На worker node проверить доступность driver
nc -zv jupyter.company.com 4041
nc -zv jupyter.company.com 4042
```

---

### Ошибки связанные с Dynamic Allocation

Если на Spark кластере включен Dynamic Allocation (`spark.dynamicAllocation.enabled=true`), убедитесь что:

1. **External Shuffle Service запущен на workers** — требуется для `spark.shuffle.service.enabled=true`
2. В Jupyter конфиге явно отключите, если не используете:
   ```properties
   spark.dynamicAllocation.enabled=false
   ```

**Симптомы проблемы**: Executor'ы запускаются и сразу умирают, задачи перезапускаются.

---

### WARN NativeCodeLoader: Unable to load native-hadoop library

Это некритичное предупреждение. Чтобы скрыть:
```python
spark.sparkContext.setLogLevel("ERROR")
```

### Connection refused to spark-master

```bash
# Проверить доступность Spark Master
nc -zv spark-master.company.com 7077

# Проверить что Master запущен
curl -f http://spark-master.company.com:8080/
```

### ClassNotFoundException: S3AFileSystem

Проверить наличие JAR файлов:
```bash
docker exec jupyter ls -la $SPARK_HOME/jars/ | grep -E "(hadoop-aws|aws-java-sdk)"

# Должны быть:
# hadoop-aws-3.3.4.jar
# aws-java-sdk-bundle-1.12.262.jar
```

### Connection refused to hive-metastore

```bash
nc -zv hive-metastore.company.com 9083
```

### SSL Certificate Error (MinIO)

```python
# Проверить сертификаты
import subprocess
result = subprocess.run(['ls', '/usr/local/share/ca-certificates/'], capture_output=True, text=True)
print("Certificates:", result.stdout)

# Явно указать путь (если нужно)
import os
os.environ['REQUESTS_CA_BUNDLE'] = '/etc/ssl/certs/ca-certificates.crt'
```

### Out of Memory на Driver

```bash
# Увеличить память driver
# В spark-defaults.conf:
spark.driver.memory=4g

# Или при создании сессии:
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

### Jupyter не стартует

```bash
docker logs jupyter

# Проверить порт
netstat -tuln | grep 8888
```

---

## Совместимость версий

**ВАЖНО**: Версии JAR-зависимостей должны совпадать между Jupyter и Spark Cluster!

| Компонент | Jupyter | Spark Cluster | Источник |
|-----------|---------|---------------|----------|
| Spark | 3.5.0 | 3.5.0 | Базовый образ |
| Java | JDK 17 | JDK 11/17 | Базовый образ |
| Hadoop AWS | 3.3.4 | 3.3.4 | JAR в Dockerfile |
| AWS SDK | 1.12.262 | 1.12.262 | JAR в Dockerfile |
| Delta Lake | 3.2.0 | 3.2.0 | JAR в Dockerfile |
| Scala | 2.12 | 2.12 | Базовый образ |

При обновлении JAR-зависимостей — обновляйте синхронно во всех Dockerfile!

---

## Следующий шаг

После развертывания:
1. Проверить подключение к Spark Cluster
2. Проверить доступ к MinIO (S3A)
3. Проверить интеграцию с Hive Metastore
4. Создать тестовые notebooks

👉 [Продвинутые возможности](advanced/next-stage.md) - HA, мониторинг, авторизация
