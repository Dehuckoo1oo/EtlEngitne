# Trino - MVP Deployment

## Назначение

Распределенный SQL query engine для аналитических запросов к данным в Data Lake.

**Конфигурация для больших данных**: Оптимизирован для работы с 2+ TB Parquet данных.

---

## Требования к машине

| Параметр | Значение |
|----------|----------|
| CPU | 16 cores |
| RAM | 64 GB |
| Storage | 500 GB SSD (для spill disk) |
| Network | 10 Gbit/s |
| Hostname | trino.company.com |

---

## GitLab структура

```
trino/
├── Dockerfile
├── config/
│   ├── config.properties
│   ├── jvm.config
│   ├── node.properties
│   ├── log.properties
│   └── catalog/
│       ├── iceberg.properties
│       └── hive.properties
├── .env.example
├── .gitlab-ci.yml
└── README.md
```

---

## Dockerfile

`trino/Dockerfile`:

```dockerfile
FROM trinodb/trino:435

USER root

# Создать директории
RUN mkdir -p /data/trino && \
    chown -R trino:trino /data/trino

# Копировать конфигурационные файлы
COPY --chown=trino:trino config/config.properties /etc/trino/config.properties
COPY --chown=trino:trino config/jvm.config /etc/trino/jvm.config
COPY --chown=trino:trino config/node.properties /etc/trino/node.properties
COPY --chown=trino:trino config/log.properties /etc/trino/log.properties
COPY --chown=trino:trino config/catalog/ /etc/trino/catalog/

USER trino

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:8080/v1/info || exit 1

EXPOSE 8080

CMD ["/usr/lib/trino/bin/run-trino"]
```

---

## Конфигурационные файлы

### config.properties

`config/config.properties`:

```properties
# === COORDINATOR (Single-node включает coordinator и worker) ===
coordinator=true
node-scheduler.include-coordinator=true

# === HTTP SERVER ===
http-server.http.port=8080

# === DISCOVERY ===
discovery.uri=http://localhost:8080

# === MEMORY (для больших запросов на 2+ TB) ===
query.max-memory=32GB
query.max-memory-per-node=32GB
query.max-total-memory=48GB

# === SPILL TO DISK (для больших JOIN) ===
spill-enabled=true
spiller-spill-path=/data/trino/spill
spiller-max-used-space-threshold=0.8

# === PERFORMANCE ===
query.max-stage-count=150
query.max-execution-time=2h
task.concurrency=16
```

### jvm.config

`config/jvm.config`:

```
-server
-Xmx48G
-Xms48G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/data/trino/heap_dump.hprof
-XX:+ExitOnOutOfMemoryError
-XX:ReservedCodeCacheSize=512M
-XX:PerformanceDataSamplingInterval=1000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
```

### node.properties

`config/node.properties`:

```properties
node.environment=production
node.id=trino-mvp-1
node.data-dir=/data/trino
```

### log.properties

`config/log.properties`:

```properties
io.trino=INFO
```

### catalog/iceberg.properties

`config/catalog/iceberg.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://hive-metastore.company.com:9083

# === FILE FORMAT ===
iceberg.file-format=PARQUET
iceberg.compression-codec=SNAPPY

# === S3/MinIO CONFIGURATION ===
fs.native-s3.enabled=true
s3.endpoint=https://minio.company.com:9000
s3.region=us-east-1
s3.path-style-access=true

# Credentials будут переданы через environment variables
# s3.aws-access-key=${ENV:S3_ACCESS_KEY}
# s3.aws-secret-key=${ENV:S3_SECRET_KEY}
```

### catalog/hive.properties

`config/catalog/hive.properties`:

```properties
connector.name=hive
hive.metastore.uri=thrift://hive-metastore.company.com:9083

# === PERMISSIONS ===
hive.non-managed-table-writes-enabled=true

# === S3/MinIO ===
fs.native-s3.enabled=true
s3.endpoint=https://minio.company.com:9000
s3.region=us-east-1
s3.path-style-access=true

# Credentials через environment variables
```

**Важно**: S3 credentials нужно передать через environment variables в `.env` файле.

---

## Environment Variables

`.env.example`:

```bash
# === S3/MinIO Credentials ===
# Получить от MinIO admin (см. minio.md)
S3_ACCESS_KEY=trino
S3_SECRET_KEY=<PASSWORD_FROM_MINIO>
```

---

## Build & Deploy

### Вариант 1: Вручную

```bash
# На машине trino.company.com

# 1. Клонировать репозиторий
git clone https://gitlab.company.com/datalake/infrastructure.git
cd infrastructure/trino

# 2. Build образа
docker build -t trino-datalake:latest .

# 3. Создать .env
cp .env.example .env
nano .env  # Установить S3 credentials

# 4. Запуск контейнера
docker run -d \
  --name trino \
  --restart unless-stopped \
  -p 8080:8080 \
  --env-file .env \
  -e "s3.aws-access-key=${S3_ACCESS_KEY}" \
  -e "s3.aws-secret-key=${S3_SECRET_KEY}" \
  -v /mnt/data/trino:/data/trino \
  trino-datalake:latest

# 5. Проверить логи
docker logs -f trino

# Дождаться сообщения:
# "======== SERVER STARTED ========"

# 6. Health check
curl -f http://trino.company.com:8080/v1/info
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
  TARGET_HOST: trino.company.com

build:
  stage: build
  script:
    - docker build -t ${REGISTRY}/trino-datalake:${IMAGE_TAG} .
    - docker tag ${REGISTRY}/trino-datalake:${IMAGE_TAG} ${REGISTRY}/trino-datalake:latest
    - docker push ${REGISTRY}/trino-datalake:${IMAGE_TAG}
    - docker push ${REGISTRY}/trino-datalake:latest
  only:
    - main

deploy:
  stage: deploy
  script:
    - ssh deploy@${TARGET_HOST} "docker pull ${REGISTRY}/trino-datalake:latest"
    - ssh deploy@${TARGET_HOST} "docker stop trino || true && docker rm trino || true"
    - ssh deploy@${TARGET_HOST} "docker run -d --name trino --restart unless-stopped -p 8080:8080 --env-file /opt/trino/.env -v /mnt/data/trino:/data/trino ${REGISTRY}/trino-datalake:latest"
  only:
    - main
  when: manual
```

---

## Проверка работоспособности

### 1. Web UI

Открыть в браузере: http://trino.company.com:8080

Должна отобразиться Trino Web UI с информацией о кластере.

### 2. Trino CLI

```bash
# Установить Trino CLI
wget https://repo1.maven.org/maven2/io/trino/trino-cli/435/trino-cli-435-executable.jar
chmod +x trino-cli-435-executable.jar
sudo mv trino-cli-435-executable.jar /usr/local/bin/trino

# Подключиться
trino --server http://trino.company.com:8080 --catalog iceberg --schema default

# Выполнить тестовый запрос
trino> SHOW CATALOGS;
trino> SHOW SCHEMAS FROM iceberg;
```

### 3. Тестовый SQL запрос

```sql
-- Создать тестовую таблицу
CREATE SCHEMA IF NOT EXISTS iceberg.test;

CREATE TABLE iceberg.test.sample_table (
  id BIGINT,
  name VARCHAR,
  created_date DATE
) WITH (
  format = 'PARQUET',
  location = 's3a://datalake/test/sample_table/'
);

-- Вставить тестовые данные
INSERT INTO iceberg.test.sample_table VALUES
  (1, 'Test 1', DATE '2024-12-25'),
  (2, 'Test 2', DATE '2024-12-25');

-- Выбрать данные
SELECT * FROM iceberg.test.sample_table;
```

---

## Health Check

```bash
# API health check
curl -f http://trino.company.com:8080/v1/info

# Возвращает JSON с информацией о кластере:
# {
#   "nodeVersion": {...},
#   "environment": "production",
#   "coordinator": true,
#   "starting": false,
#   ...
# }

# Docker healthcheck
docker inspect trino | grep -A 5 Health
```

---

## Troubleshooting

### Trino не стартует

```bash
# Проверить логи
docker logs trino

# Частые проблемы:
# 1. Не может подключиться к Hive Metastore
nc -zv hive-metastore.company.com 9083

# 2. Недостаточно памяти
# Убедитесь что машина имеет 64GB RAM
free -h

# 3. Ошибка в конфигурационных файлах
# Проверить синтаксис в config/*.properties
```

### Cannot connect to Hive Metastore

```bash
# Проверить доступность
docker exec trino nc -zv hive-metastore.company.com 9083

# Если недоступен - проверить:
# 1. Hive Metastore запущен
# 2. Firewall правила
# 3. DNS разрешение
```

### Cannot access MinIO/S3

```bash
# Проверить credentials
docker exec trino env | grep S3

# Проверить доступность MinIO
docker exec trino curl -I https://minio.company.com:9000

# Тест S3 через Trino CLI
trino> SELECT * FROM system.runtime.nodes;
```

### Query fails с OOM

```bash
# Увеличить heap в jvm.config
# -Xmx48G -> -Xmx56G (если есть доступная RAM)

# Или включить spill to disk (уже включен в config.properties)
# spill-enabled=true

# Проверить использование памяти
docker stats trino
```

### Slow queries

```bash
# Проверить query план в Web UI
# http://trino.company.com:8080

# Оптимизации:
# 1. Использовать partition pruning
SELECT * FROM iceberg.sales.orders WHERE dt = '2024-12-25'

# 2. Выбирать только нужные колонки
SELECT id, name FROM table  -- вместо SELECT *

# 3. Использовать LIMIT для тестирования
SELECT * FROM large_table LIMIT 1000

# 4. Проверить статистику таблиц в Hive Metastore
```

---

## Следующий шаг

После успешного развертывания Trino переходите к:
👉 [Jupyter](jupyter.md)
