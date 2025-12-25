# Hive Metastore Production Deployment Guide

## Содержание
1. [Обзор](#обзор)
2. [Архитектура](#архитектура)
3. [Требования](#требования)
4. [Production развертывание](#production-развертывание)
5. [Конфигурация](#конфигурация)
6. [Высокая доступность](#высокая-доступность)
7. [Мониторинг](#мониторинг)
8. [Операции](#операции)
9. [Troubleshooting](#troubleshooting)

---

## Обзор

Hive Metastore - это сервис управления метаданными таблиц Data Lake. Он предоставляет Thrift API для хранения и получения информации о таблицах, партициях, схемах и локациях данных.

### Роль в архитектуре
- Централизованный каталог таблиц Data Lake
- Хранение метаданных в PostgreSQL
- API для Trino, Spark, Presto и других query engines
- Управление локациями данных в MinIO/S3

### Зависимости
- **Требуется**: PostgreSQL Metastore, MinIO
- **Используется**: Trino, Kafka Connect (опционально)

---

## Архитектура

```
┌────────────┐  ┌────────────┐  ┌────────────┐
│   Trino    │  │   Spark    │  │   Kafka    │
│            │  │            │  │  Connect   │
└─────┬──────┘  └─────┬──────┘  └─────┬──────┘
      │               │               │
      └───────┬───────┴───────┬───────┘
              │ Thrift API    │
              ↓ (port 9083)   ↓
      ┌───────────────────────────┐
      │   Hive Metastore (HA)     │
      │  ┌──────┐  ┌──────┐       │
      │  │Node 1│  │Node 2│       │
      │  └───┬──┘  └───┬──┘       │
      └──────┼─────────┼──────────┘
             │         │
             ↓         ↓
      ┌─────────────────────┐
      │  PostgreSQL         │
      │  Metastore DB       │
      └─────────────────────┘
             │
             ↓ S3 operations
      ┌─────────────────────┐
      │      MinIO          │
      └─────────────────────┘
```

---

## Требования

### Аппаратные требования (на узел)

#### Minimum (Development)
- **CPU**: 2 cores
- **RAM**: 4GB
- **Storage**: 20GB
- **Network**: 1 Gbit/s

#### Recommended (Production)
- **CPU**: 4 cores
- **RAM**: 8GB
- **Storage**: 50GB
- **Network**: 10 Gbit/s

### ПО требования
- Docker Engine 20.10+
- Java Runtime (включен в Docker образ)

---

## Production развертывание

### 1. Подготовка

```bash
# Создание директорий
mkdir -p /opt/hive-metastore/config
mkdir -p /opt/hive-metastore/logs

# Клонировать конфиги из репозитория
cd /opt/hive-metastore
```

### 2. Dockerfile

Создать `/opt/hive-metastore/Dockerfile`:

```dockerfile
FROM apache/hive:4.0.0

USER root

# Install required packages
RUN apt-get update && \
    apt-get install -y wget netcat-openbsd && \
    # PostgreSQL JDBC driver
    wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
         -O /opt/hive/lib/postgresql-jdbc.jar && \
    # AWS SDK for S3 (MinIO) support
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         -O /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
         -O /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar && \
    # Create symlinks
    ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar /opt/hive/lib/hadoop-aws-3.3.4.jar && \
    ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar && \
    # Cleanup
    rm -rf /var/lib/apt/lists/*

# Copy Hadoop S3 configuration
COPY config/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

USER hive

ENTRYPOINT ["/entrypoint.sh"]
```

### 3. Hadoop S3 Configuration

Создать `/opt/hive-metastore/config/core-site.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- S3A Configuration for MinIO -->
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
        <!-- Для production с SSL: true -->
    </property>

    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>

    <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
    </property>
</configuration>
```

### 4. Build Docker Image

```bash
cd /opt/hive-metastore
docker build -t hive-metastore:4.0.0-prod .
```

### 5. Environment Variables

Создать `/opt/hive-metastore/.env`:

```bash
# === PostgreSQL Connection ===
POSTGRES_HOST=postgres-metastore.company.com
POSTGRES_PORT=5432
POSTGRES_DB=metastore_db
POSTGRES_USER=hive
POSTGRES_PASSWORD=<secure-password>

# === MinIO/S3 Access ===
AWS_ACCESS_KEY_ID=<minio-access-key>
AWS_SECRET_ACCESS_KEY=<minio-secret-key>
S3_ENDPOINT_URL=http://minio.company.com:9000
S3_PATH_STYLE_ACCESS=true

# === JVM Settings ===
HIVE_METASTORE_HEAP_SIZE=4g
```

### 6. Docker Run Script

Создать `/opt/hive-metastore/docker-run.sh`:

```bash
#!/bin/bash

source /opt/hive-metastore/.env

docker run -d \
  --name hive-metastore \
  --restart unless-stopped \
  --hostname hive-metastore-$(hostname) \
  --network host \
  -e SERVICE_NAME=metastore \
  -e DB_DRIVER=postgres \
  -e SERVICE_OPTS="\
-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver \
-Djavax.jdo.option.ConnectionURL=jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB} \
-Djavax.jdo.option.ConnectionUserName=${POSTGRES_USER} \
-Djavax.jdo.option.ConnectionPassword=${POSTGRES_PASSWORD} \
-Djavax.jdo.option.ConnectionPoolingType=HikariCP \
-Dhikaricp.maximumPoolSize=50 \
-Dhikaricp.minimumIdle=10 \
-Dhikaricp.connectionTimeout=30000 \
-Dhikaricp.idleTimeout=600000 \
-Dhikaricp.maxLifetime=1800000 \
-Xmx${HIVE_METASTORE_HEAP_SIZE} \
-Xms${HIVE_METASTORE_HEAP_SIZE} \
-XX:+UseG1GC \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=/tmp/hive-metastore-heap.hprof \
" \
  -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
  -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  -e S3_ENDPOINT_URL=${S3_ENDPOINT_URL} \
  -e S3_PATH_STYLE_ACCESS=${S3_PATH_STYLE_ACCESS} \
  -v /opt/hive-metastore/logs:/opt/hive/logs \
  hive-metastore:4.0.0-prod
```

### 7. Systemd Service

Создать `/etc/systemd/system/hive-metastore.service`:

```ini
[Unit]
Description=Hive Metastore Service
Documentation=https://hive.apache.org/
After=docker.service postgres-metastore.service
Requires=docker.service
Wants=postgres-metastore.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/hive-metastore
EnvironmentFile=/opt/hive-metastore/.env
ExecStartPre=-/usr/bin/docker stop hive-metastore
ExecStartPre=-/usr/bin/docker rm hive-metastore
ExecStart=/opt/hive-metastore/docker-run.sh
ExecStop=/usr/bin/docker stop hive-metastore
Restart=always
RestartSec=10
StartLimitInterval=0

[Install]
WantedBy=multi-user.target
```

Запуск:
```bash
systemctl daemon-reload
systemctl enable hive-metastore
systemctl start hive-metastore
systemctl status hive-metastore
```

### 8. Проверка

```bash
# Проверить логи
docker logs -f hive-metastore

# Проверить Thrift API
nc -zv localhost 9083

# Проверить подключение к PostgreSQL
docker exec hive-metastore \
  /opt/hive/bin/schematool -dbType postgres -info

# Должен вывести версию схемы
```

---

## Конфигурация

### JVM Parameters

| Параметр | Значение (Production) | Описание |
|----------|----------------------|----------|
| `-Xmx` / `-Xms` | 4-8GB | Heap size (зависит от нагрузки) |
| `-XX:+UseG1GC` | Enabled | G1 Garbage Collector |
| `-XX:+HeapDumpOnOutOfMemoryError` | Enabled | Heap dump при OOM |
| `-Dhikaricp.maximumPoolSize` | 50 | Max DB connections |
| `-Dhikaricp.minimumIdle` | 10 | Min DB connections |

### Database Connection Pool

Оптимальные настройки HikariCP:
- `maximumPoolSize`: 50-100 (зависит от нагрузки)
- `minimumIdle`: 10-20
- `connectionTimeout`: 30000ms
- `idleTimeout`: 600000ms (10 min)
- `maxLifetime`: 1800000ms (30 min)

---

## Высокая доступность

### Stateless Service

Hive Metastore - stateless сервис, все состояние хранится в PostgreSQL.

### Multi-Instance Deployment

Для HA развернуть 2-3 инстанса:

```
┌────────────────┐
│  Load Balancer │
│  (HAProxy)     │
└────────┬───────┘
         │
    ┌────┼────┐
    │    │    │
┌───▼┐ ┌─▼──┐ ┌──▼─┐
│HMS1│ │HMS2│ │HMS3│
└────┘ └────┘ └────┘
    │    │    │
    └────┼────┘
         ↓
  ┌──────────────┐
  │  PostgreSQL  │
  └──────────────┘
```

#### HAProxy Configuration

```haproxy
frontend hive_metastore_frontend
    bind *:9083
    mode tcp
    default_backend hive_metastore_backend

backend hive_metastore_backend
    mode tcp
    balance roundrobin
    option tcp-check
    server hms1 hive-metastore-1:9083 check inter 10s
    server hms2 hive-metastore-2:9083 check inter 10s
    server hms3 hive-metastore-3:9083 check inter 10s
```

### Health Check

```bash
# Script для проверки
#!/bin/bash
nc -zv localhost 9083 && exit 0 || exit 1
```

---

## Мониторинг

### JMX Metrics

Hive Metastore экспортирует JMX метрики.

#### Запуск с JMX

Добавить в `SERVICE_OPTS`:
```bash
-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=9999 \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false
```

#### JMX Exporter для Prometheus

```bash
# Запустить JMX exporter sidecar
docker run -d \
  --name hive-metastore-jmx-exporter \
  --network container:hive-metastore \
  -v /opt/hive-metastore/jmx-exporter-config.yml:/config.yml \
  bitnami/jmx-exporter:latest \
  9404 /config.yml
```

`jmx-exporter-config.yml`:
```yaml
lowercaseOutputName: true
rules:
  - pattern: ".*"
```

### Ключевые метрики

| Метрика | Описание | Threshold |
|---------|----------|-----------|
| JVM Heap Usage | Использование памяти | < 80% |
| GC Time | Время GC | < 5% CPU time |
| DB Connection Pool | Активные подключения | < max pool size |
| API Latency | Latency Thrift API | p95 < 100ms |
| Error Rate | Количество ошибок | < 1% |

### Логи

```bash
# Логи находятся в /opt/hive-metastore/logs
tail -f /opt/hive-metastore/logs/hive.log

# Настроить log level
# В SERVICE_OPTS добавить:
-Dhive.log.level=INFO
```

### Alerting

```yaml
groups:
  - name: hive-metastore
    rules:
      - alert: HiveMetastoreDown
        expr: up{job="hive-metastore"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Hive Metastore is down"

      - alert: HiveMetastoreHighMemory
        expr: jvm_memory_bytes_used / jvm_memory_bytes_max > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High memory usage"
```

---

## Операции

### Schema Migration

При обновлении Hive Metastore может потребоваться миграция схемы БД:

```bash
# Проверить текущую версию схемы
docker exec hive-metastore \
  /opt/hive/bin/schematool -dbType postgres -info

# Выполнить миграцию
docker exec hive-metastore \
  /opt/hive/bin/schematool -dbType postgres -upgradeSchema

# Или при первом запуске:
docker exec hive-metastore \
  /opt/hive/bin/schematool -dbType postgres -initSchema
```

### Обновление версии

```bash
# 1. Backup PostgreSQL
# См. postgres-metastore.md

# 2. Обновить Docker образ
docker pull apache/hive:4.1.0

# 3. Rebuild с новой версией
cd /opt/hive-metastore
# В Dockerfile: FROM apache/hive:4.1.0
docker build -t hive-metastore:4.1.0-prod .

# 4. Остановить старый
systemctl stop hive-metastore

# 5. Обновить docker-run.sh (новый образ)
# hive-metastore:4.1.0-prod

# 6. Выполнить schema migration
# 7. Запустить новый
systemctl start hive-metastore
```

### Очистка старых метаданных

```sql
-- Подключиться к PostgreSQL
psql -h postgres-metastore -U hive -d metastore_db

-- Найти старые партиции
SELECT
  p.part_name,
  t.tbl_name,
  d.name AS db_name,
  p.create_time
FROM PARTITIONS p
JOIN TBLS t ON p.tbl_id = t.tbl_id
JOIN DBS d ON t.db_id = d.db_id
WHERE p.create_time < EXTRACT(EPOCH FROM NOW() - INTERVAL '365 days');

-- Удалить партиции старше года (осторожно!)
-- DELETE FROM PARTITIONS WHERE create_time < ...
```

---

## Troubleshooting

### Не может подключиться к PostgreSQL

```bash
# Проверить доступность PostgreSQL
docker exec hive-metastore nc -zv postgres-metastore.company.com 5432

# Проверить credentials
docker exec hive-metastore env | grep POSTGRES

# Проверить pg_hba.conf на PostgreSQL сервере
```

### Не может подключиться к MinIO

```bash
# Проверить доступность MinIO
docker exec hive-metastore curl -I http://minio.company.com:9000

# Проверить credentials
docker exec hive-metastore env | grep AWS

# Тест S3 операций
docker exec hive-metastore aws --endpoint-url http://minio.company.com:9000 \
  s3 ls s3://datalake/
```

### OutOfMemoryError

```bash
# Увеличить heap size в .env
HIVE_METASTORE_HEAP_SIZE=8g

# Перезапустить
systemctl restart hive-metastore

# Проанализировать heap dump
# /tmp/hive-metastore-heap.hprof
```

### Slow queries

```bash
# Включить query logging в PostgreSQL
# См. postgres-metastore.md

# Анализ slow queries
# Обычно проблема в:
# 1. Большое количество партиций
# 2. Отсутствие индексов в metastore DB
# 3. Недостаточный connection pool
```

---

## Дополнительные ресурсы

- [Apache Hive Documentation](https://hive.apache.org/)
- [Hive Metastore Administration Guide](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration)
- [Schema Tool](https://cwiki.apache.org/confluence/display/Hive/Hive+Schema+Tool)
