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

EXPOSE 8888 4040

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
```

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

---

## Troubleshooting

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
