# Trino Production Deployment Guide

## Содержание
1. [Обзор](#обзор)
2. [Архитектура](#архитектура)
3. [Требования](#требования)
4. [Production развертывание](#production-развертывание)
5. [Конфигурационные файлы](#конфигурационные-файлы)
6. [Catalogs конфигурация](#catalogs-конфигурация)
7. [JVM Tuning](#jvm-tuning)
8. [Высокая доступность](#высокая-доступность)
9. [Мониторинг](#мониторинг)
10. [Query оптимизация](#query-оптимизация)
11. [Операции](#операции)
12. [Troubleshooting](#troubleshooting)

---

## Обзор

Trino (ранее PrestoSQL) - это распределенный SQL query engine для выполнения аналитических запросов к данным в Data Lake. Он работает с несколькими источниками данных (Hive, Iceberg, MinIO) через единый SQL интерфейс.

### Роль в архитектуре

- **Distributed SQL Query Engine**: Выполнение сложных аналитических запросов
- **Multi-Source Queries**: Объединение данных из Hive и Iceberg
- **S3 Integration**: Прямой доступ к Parquet файлам в MinIO
- **Web UI**: Мониторинг и управление запросами
- **JDBC/HTTP API**: Интеграция с BI инструментами и приложениями

### Зависимости

- **Требуется**: Hive Metastore (каталог таблиц)
- **Требуется**: MinIO/S3 (хранилище данных)
- **Требуется**: PostgreSQL Metastore (для Hive Metastore)
- **Используется**: Jupyter, Kafka Connect (опционально)

---

## Архитектура

### Компоненты Trino

```
┌──────────────────────────────────────────────────────────────┐
│                     Client Applications                       │
│              (BI Tools, Jupyter, JDBC/HTTP)                   │
└────────────────────────┬─────────────────────────────────────┘
                         │
         ┌───────────────┴───────────────┐
         │                               │
    ┌────▼──────────────────┐   ┌───────▼──────────────────┐
    │   COORDINATOR NODE     │   │   LOAD BALANCER (Nginx) │
    │   Port: 8080           │   │   Port: 443/8080        │
    │ - Query coordination   │   │ - SSL termination       │
    │ - Resource management  │   │ - Request distribution  │
    │ - Query scheduling     │   └───────┬──────────────────┘
    └────┬──────────────────┘            │
         │                               │
    ┌────┴───────────────────────────────┘
    │
    ├─────────────────┬──────────────────┬──────────────────┐
    │                 │                  │                  │
┌───▼────────┐  ┌────▼────────┐  ┌──────▼──────┐  ┌─────────▼──┐
│  WORKER-1  │  │  WORKER-2   │  │  WORKER-N   │  │  WORKER-N+1│
│  Port: 8081│  │  Port: 8081 │  │ Port: 8081  │  │ Port: 8081 │
└────┬────────┘  └────┬────────┘  └──────┬──────┘  └─────────┬───┘
     │                │                  │                  │
     └────────────────┼──────────────────┼──────────────────┘
                      │ Discovery
                      ↓ (port 8080)
           ┌──────────────────────┐
           │  Hive Metastore      │
           │  (Thrift API:9083)   │
           └──────────┬───────────┘
                      │
           ┌──────────▼───────────┐
           │  MinIO / S3 Storage  │
           │  (Port: 9000)        │
           └──────────────────────┘
```

### Архитектура с HA

```
                    ┌────────────────────┐
                    │  DNS / Load        │
                    │  Balancer          │
                    └─────────┬──────────┘
                              │
                ┌─────────────┴─────────────┐
                │                           │
         ┌──────▼────────┐          ┌───────▼──────┐
         │ COORDINATOR-1 │          │ COORDINATOR-2│
         │ Port: 8080    │          │ Port: 8080   │
         └──────┬────────┘          └───────┬──────┘
                │                           │
                └──────────────┬────────────┘
                               │ Shared Discovery
                               ↓ & Catalog Cache
                    ┌──────────────────────┐
                    │ Hive Metastore (HA)  │
                    └──────────────────────┘

                   ┌────┬────┬────┬────────┐
                   │    │    │    │        │
            ┌──────▼┐ ┌─▼──┐ ┌─▼──┐ ┌────▼──┐
            │WORKER-│ │WORK│ │WORK│ │WORKER-│
            │   1   │ │ 2  │ │ 3  │ │  N+1  │
            └───────┘ └────┘ └────┘ └───────┘
```

---

## Требования

### Аппаратные требования (на узел)

#### Coordinator Node (Production)
- **CPU**: 8 cores minimum, 16+ cores recommended
- **RAM**: 32GB minimum, 64GB recommended
- **Storage**: 100GB SSD (для logs, spill, etc.)
- **Network**: 10 Gbit/s, latency < 1ms к workers
- **Instances**: 1-2 для HA

#### Worker Nodes (Production)
- **CPU**: 16 cores minimum, 32+ cores recommended
- **RAM**: 64GB minimum, 128GB+ for large datasets
- **Storage**: 500GB-2TB SSD (для spill directories)
- **Network**: 10 Gbit/s, latency < 1ms к coordinator
- **Instances**: 5+ для production, horizontal scaling

### Сетевые требования
- **Latency**: < 1ms между coordinator и workers
- **Bandwidth**: 10 Gbit/s minimum (100Gbit/s recommended)
- **DNS**: Корректное разрешение всех узлов
- **Firewall**:
  - Coordinator: 8080 (HTTP/HTTPS)
  - Workers: 8081 (HTTP)
  - Discovery: 8080

### ПО требования
- Docker Engine 20.10+
- Java 11+ (включен в Docker образ)

---

## Production развертывание

### 1. Архитектура развертывания

**Production setup**: Каждый компонент на отдельной машине

```
┌────────────────────────────────────────────────────────────────┐
│ Машина 1: Trino Coordinator                                    │
│ - 8-16 CPU cores                                               │
│ - 32GB RAM                                                      │
│ - Port 8080: HTTP API, Web UI                                  │
│ - Сроль: query coordination, scheduling                        │
└────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│ Машины 2-6+: Trino Workers                                    │
│ - 16 CPU cores каждый                                         │
│ - 64GB RAM каждый                                             │
│ - Port 8081: Worker API                                        │
│ - Роль: query execution, data processing (stateless)          │
└────────────────────────────────────────────────────────────────┘

Все machine соединены через 10 Gbit/s network с latency < 1ms
```

### 2. Подготовка инфраструктуры

На каждом узле (coordinator и workers):

```bash
# Обновление системы
apt-get update && apt-get upgrade -y

# Установка необходимых пакетов
apt-get install -y \
    curl \
    ca-certificates \
    gnupg \
    lsb-release \
    net-tools \
    ntp

# Синхронизация времени (очень важно!)
systemctl restart ntp
timedatectl status

# Установка Docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io

# Создание директорий
mkdir -p /opt/trino/{config,data,logs}

# Настройка hostnames
# На coordinator: trino-coordinator
# На workers: trino-worker-1, trino-worker-2, etc.
hostnamectl set-hostname <hostname>
```

### 3. Firewall правила

```bash
# На Coordinator
ufw allow 8080/tcp comment 'Trino HTTP API'
ufw allow from 10.0.0.0/8 to any port 8080 proto tcp comment 'Trino Discovery'

# На Workers
ufw allow from <coordinator-ip> to any port 8081 proto tcp comment 'Trino Worker API'
ufw allow from 10.0.0.0/8 to any port 8081 proto tcp comment 'Inter-worker communication'
```

### 4. Deployment Coordinator

#### Копирование конфигурационных файлов

Из `data-lake/trino/`:

```bash
# На coordinator (/opt/trino/config/)
cp config.properties /opt/trino/config/
cp jvm.config /opt/trino/config/
cp node.properties /opt/trino/config/
cp log.properties /opt/trino/config/

# Каталоги
mkdir -p /opt/trino/config/catalog
cp catalog/iceberg.properties /opt/trino/config/catalog/
cp catalog/hive.properties /opt/trino/config/catalog/
```

#### Docker запуск Coordinator

Создать `/opt/trino/docker-run-coordinator.sh`:

```bash
#!/bin/bash

docker run -d \
  --name trino-coordinator \
  --restart unless-stopped \
  --hostname trino-coordinator \
  --network host \
  -e NODE_ID=coordinator-1 \
  -e NODE_ENVIRONMENT=production \
  -v /opt/trino/config:/etc/trino \
  -v /opt/trino/data:/data/trino \
  -v /opt/trino/logs:/var/log/trino \
  -p 8080:8080 \
  trinodb/trino:latest
```

Запуск:

```bash
chmod +x /opt/trino/docker-run-coordinator.sh
/opt/trino/docker-run-coordinator.sh

# Проверка
docker logs -f trino-coordinator

# Проверить Web UI
curl http://localhost:8080/ui/
```

#### Systemd Service для Coordinator

Создать `/etc/systemd/system/trino-coordinator.service`:

```ini
[Unit]
Description=Trino Coordinator
Documentation=https://trino.io/docs/
After=docker.service
Requires=docker.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/trino
ExecStartPre=-/usr/bin/docker stop trino-coordinator
ExecStartPre=-/usr/bin/docker rm trino-coordinator
ExecStart=/opt/trino/docker-run-coordinator.sh
ExecStop=/usr/bin/docker stop trino-coordinator
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Активация:

```bash
systemctl daemon-reload
systemctl enable trino-coordinator
systemctl start trino-coordinator
systemctl status trino-coordinator
```

### 5. Deployment Workers

На каждом worker узле скопировать конфиги и создать `/opt/trino/docker-run-worker.sh`:

```bash
#!/bin/bash

# Получить ID worker из hostname (например: trino-worker-1 -> 1)
WORKER_NUM=$(hostname | grep -oP '(?<=-)\d+$')
COORDINATOR_IP=<coordinator-ip>

docker run -d \
  --name trino-worker \
  --restart unless-stopped \
  --hostname trino-worker-${WORKER_NUM} \
  --network host \
  -e NODE_ID=worker-${WORKER_NUM} \
  -e NODE_ENVIRONMENT=production \
  -e DISCOVERY_URI=http://${COORDINATOR_IP}:8080 \
  -v /opt/trino/config:/etc/trino \
  -v /opt/trino/data:/data/trino \
  -v /opt/trino/logs:/var/log/trino \
  -p 8081:8081 \
  trinodb/trino:latest
```

Запуск на всех workers:

```bash
# На каждом worker
chmod +x /opt/trino/docker-run-worker.sh
/opt/trino/docker-run-worker.sh

# Проверка
docker logs -f trino-worker
```

Systemd Service для Workers:

Создать `/etc/systemd/system/trino-worker.service`:

```ini
[Unit]
Description=Trino Worker
Documentation=https://trino.io/docs/
After=docker.service
Requires=docker.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/trino
ExecStartPre=-/usr/bin/docker stop trino-worker
ExecStartPre=-/usr/bin/docker rm trino-worker
ExecStart=/opt/trino/docker-run-worker.sh
ExecStop=/usr/bin/docker stop trino-worker
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 6. Проверка Cluster

```bash
# На coordinator
curl -s http://localhost:8080/v1/cluster | jq

# Вывод должен показать:
# - activeNodes: 5-6 (coordinator + workers)
# - coordinatorActive: true

# Альтернативно через Web UI
# http://coordinator-ip:8080/ui/

# Детальная информация о worker nodes
curl -s http://localhost:8080/v1/node | jq '.[] | {nodeId, state}'
```

### 7. Load Balancer (Production)

#### Nginx Setup

Создать `/opt/nginx/nginx.conf`:

```nginx
upstream trino {
    least_conn;
    server trino-coordinator:8080;
}

# Health check upstream
upstream trino_health {
    server trino-coordinator:8080;
}

server {
    listen 443 ssl http2;
    server_name trino.company.com;

    ssl_certificate /etc/nginx/ssl/trino.crt;
    ssl_certificate_key /etc/nginx/ssl/trino.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    # Increase request limits for large queries
    client_max_body_size 100M;
    client_body_timeout 300s;
    proxy_read_timeout 300s;

    # Disable buffering for streaming
    proxy_buffering off;
    proxy_request_buffering off;

    location / {
        proxy_pass http://trino;
        proxy_set_header Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support для Web UI
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # Timeouts
        proxy_connect_timeout 30s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }

    # Health check endpoint
    location /v1/cluster {
        proxy_pass http://trino;
        access_log off;
    }
}

# Redirect HTTP to HTTPS
server {
    listen 80;
    server_name trino.company.com;
    return 301 https://$server_name$request_uri;
}
```

Docker запуск Nginx:

```bash
docker run -d \
  --name trino-nginx \
  --restart unless-stopped \
  --network host \
  -v /opt/nginx/nginx.conf:/etc/nginx/nginx.conf \
  -v /opt/nginx/ssl:/etc/nginx/ssl \
  -p 80:80 \
  -p 443:443 \
  nginx:latest
```

---

## Конфигурационные файлы

### config.properties (Coordinator)

```properties
# === NODE CONFIGURATION ===
coordinator=true
node-scheduler.include-coordinator=true

# === HTTP SERVER ===
http-server.http.port=8080
http-server.http.enabled=true
http-server.https.enabled=false
# Для production с SSL:
# http-server.https.enabled=true
# http-server.https.port=8443
# http-server.https.keystore.path=/etc/trino/keystore.jks
# http-server.https.keystore.key=<password>

# === DISCOVERY ===
discovery.uri=http://trino-coordinator:8080
discovery.listener-only=true

# === QUERY EXECUTION ===
query.max-memory=4GB
query.max-memory-per-node=2GB
query.max-run-time=100d
query.max-execution-time=100d

# === SCHEDULING ===
node-scheduler.max-split-batch-size=1000
node-scheduler.split-concurrency-adjustment-enabled=true

# === SPILL ===
spill-order-by-enabled=true
spill-window-operator-enabled=true
spill-enabled=true
spill-max-used-space=8GB

# Adjust based on available disk space:
# For 500GB disk: spill-max-used-space=200GB
# For 2TB disk: spill-max-used-space=800GB

spill-compression-enabled=true
spill-compression-codec=SNAPPY

# === MEMORY ===
memory.heap-headroom=2GB

# === QUEUES (для управления ресурсами) ===
query-manager.required-workers=3
query-manager.required-workers-max-wait=5m

# === LOGGING ===
log.output-format=json
log.level=INFO
log.max-size=100MB
log.max-history=10
```

### config.properties (Worker)

```properties
# === NODE CONFIGURATION ===
coordinator=false
node-scheduler.include-coordinator=false

# === HTTP SERVER ===
http-server.http.port=8081

# === DISCOVERY ===
discovery.uri=http://trino-coordinator:8080
discovery.listener-only=false

# === QUERY EXECUTION ===
query.max-memory=4GB
query.max-memory-per-node=2GB

# === SPILL ===
spill-order-by-enabled=true
spill-window-operator-enabled=true
spill-enabled=true
spill-max-used-space=200GB  # Adjust based on disk

spill-compression-enabled=true
spill-compression-codec=SNAPPY

# === MEMORY ===
memory.heap-headroom=2GB

# === LOGGING ===
log.output-format=json
log.level=INFO
```

### jvm.config

```properties
-server
-Xmx32G
-Xms32G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/var/log/trino/heap-dump.hprof
-XX:ReservedCodeCacheSize=512M
-XX:PerBytecodeSize=100
-XX:TieredStopAtLevel=4
-XX:+UnlockDiagnosticVMOptions
-XX:G1SummarizeRSetStatsPeriod=1
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000

# Для large heap (64GB на некоторых workers):
# -Xmx64G
# -Xms64G

# GC logging (опционально)
# -Xlog:gc*:file=/var/log/trino/gc.log:time,uptime:filecount=10,filesize=512m
```

### node.properties

На Coordinator:
```properties
node.environment=production
node.id=coordinator-1
node.data-dir=/data/trino
node.location=<data-center-name>
node.internal-address=<coordinator-ip>
```

На Worker-1:
```properties
node.environment=production
node.id=worker-1
node.data-dir=/data/trino
node.location=<data-center-name>
node.internal-address=<worker-1-ip>
```

На Worker-N:
```properties
node.environment=production
node.id=worker-N
node.data-dir=/data/trino
node.location=<data-center-name>
node.internal-address=<worker-N-ip>
```

### log.properties

```properties
# === ROOT LOGGER ===
io.trino=INFO

# === QUERY LOGGING ===
io.trino.server.QueryMonitor=DEBUG
io.trino.execution.QueryExecution=DEBUG

# === CATALOG LOGGING ===
io.trino.connector=INFO
io.trino.connector.hive=DEBUG
io.trino.connector.iceberg=DEBUG

# === CONNECTOR LOGGING ===
io.trino.plugin.hive=INFO
io.trino.plugin.iceberg=INFO

# === PERFORMANCE TUNING LOGGING ===
io.trino.operator=DEBUG
io.trino.server.scheduling=INFO

# === SPILL LOGGING ===
io.trino.spiller=DEBUG

# === MEMORY LOGGING ===
io.trino.memory=DEBUG
```

---

## Catalogs конфигурация

### catalog/hive.properties

```properties
# === CONNECTOR ===
connector.name=hive

# === METASTORE CONNECTION ===
hive.metastore.uri=thrift://hive-metastore:9083
hive.metastore.client.socket.keep-alive=true
hive.metastore.client.connect-timeout=30s
hive.metastore.client.socket-timeout=30s

# === TABLE PROPERTIES ===
hive.non-managed-table-writes-enabled=true
hive.respect-table-format=true

# === PARTITION HANDLING ===
hive.partition-projection-enabled=true
hive.partition-statistics-table-enabled=true

# === BUCKETING ===
hive.bucketed-execution-enabled=true

# === S3 / MinIO CONFIGURATION ===
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=<minio-access-key>
s3.aws-secret-key=<minio-secret-key>
s3.use-instance-credentials=false

# === OBJECT STORAGE ===
hive.storage-format=PARQUET
hive.compression-codec=SNAPPY

# === CACHING ===
hive.metastore-cache-ttl=1h
hive.metastore-refresh-interval=1h
hive.max-initial-splits=1000

# === STATISTICS ===
hive.statistics.enabled=true
hive.dfs.ignore-immutable-marker=false

# === PERFORMANCE ===
hive.max-partitions-per-scan=100000
hive.allow-insecure-writes=false
hive.insert-existing-partitions-behavior=OVERWRITE
```

### catalog/iceberg.properties

```properties
# === CONNECTOR ===
connector.name=iceberg

# === CATALOG TYPE ===
iceberg.catalog.type=hive_metastore

# === METASTORE CONNECTION ===
hive.metastore.uri=thrift://hive-metastore:9083
hive.metastore.client.socket.keep-alive=true

# === FILE FORMAT ===
iceberg.file-format=PARQUET
iceberg.compression-codec=SNAPPY

# === CACHING ===
iceberg.metadata-cache-ttl=10m
iceberg.metadata-refresh-interval=1h

# === S3 / MinIO CONFIGURATION ===
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=<minio-access-key>
s3.aws-secret-key=<minio-secret-key>
s3.use-instance-credentials=false

# === ICEBERG SPECIFIC ===
iceberg.hide-immutable-projections=true
iceberg.table-statistics-enabled=true

# === SNAPSHOTS & VERSIONING ===
iceberg.max-snapshot-history-per-table=20

# === PERFORMANCE ===
iceberg.max-partitions-per-scan=100000
iceberg.split-size=256MB
```

---

## JVM Tuning

### Для Coordinator (32GB RAM)

```
jvm.config:
-Xmx32G
-Xms32G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
```

**Рекомендации**:
- Heap размер: 75-85% от доступной RAM
- Coordinator обычно требует меньше памяти, чем workers
- Оставлять 5-8GB для OS и system processes

### Для Workers (64GB RAM)

```
jvm.config:
-Xmx64G
-Xms64G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
```

**Для very large workers (128GB RAM)**:
```
-Xmx100G
-Xms100G
-XX:G1HeapRegionSize=64M  # Увеличить region size для большого heap
```

### G1GC Tuning

| Параметр | Значение | Описание |
|----------|----------|----------|
| `-XX:G1HeapRegionSize` | 32M-64M | Размер региона (зависит от heap size) |
| `-XX:MaxGCPauseMillis` | 200 (default) | Max pause time for GC |
| `-XX:InitiatingHeapOccupancyPercent` | 35 | Trigger для concurrent GC |
| `-XX:+UnlockDiagnosticVMOptions` | true | Для advanced tuning |
| `-XX:G1SummarizeRSetStatsPeriod` | 1 | Logging RSet stats (для debug) |

### Memory Configuration in config.properties

```properties
# Query memory allocation
query.max-memory=4GB              # Per-query max memory
query.max-memory-per-node=2GB     # Max per-query per-node

# Memory management
memory.heap-headroom=2GB          # Reserved for internal structures

# Spill configuration (when memory exceeded)
spill-enabled=true
spill-max-used-space=200GB        # Workers: max spill size
spill-compression-enabled=true
spill-order-by-enabled=true
spill-window-operator-enabled=true
```

### Monitoring GC

```bash
# Включить GC logging в jvm.config
-Xlog:gc*:file=/var/log/trino/gc.log:time,uptime:filecount=10,filesize=512m

# Анализ GC логов
tail -f /var/log/trino/gc.log

# Использовать GCViewer для анализа GC patterns
# https://github.com/chewiebug/GCViewer
```

---

## Высокая доступность

### Coordinator HA (2+ instances)

Для production рекомендуется 2 coordinator для redundancy:

```
┌─────────────────────────────────────────────┐
│        DNS / Load Balancer                  │
│     trino.company.com:443                   │
└────────────────┬────────────────────────────┘
                 │
        ┌────────┴────────┐
        │                 │
   ┌────▼────┐       ┌───▼─────┐
   │COORD-1  │       │COORD-2  │
   │:8080    │       │:8080    │
   └────┬────┘       └───┬─────┘
        │                │
        └───────┬────────┘
                ↓
        ┌──────────────────┐
        │ Hive Metastore   │
        │ (Shared catalog) │
        └──────────────────┘
```

**Конфигурация**:
- Оба coordinator `coordinator=true`
- Одинаковое `discovery.uri` (не важно, обращаются друг к другу)
- Shared catalog configuration
- Shared hive.metastore.uri
- Load balancer распределяет трафик

### Worker Stateless Design

Workers - полностью stateless:
- Нет состояния на диске (кроме временных spill files)
- Можно добавлять/удалять без downtime
- Каждый worker独立
- Coordinator отслеживает health и перераспределяет нагрузку

### Health Checks

```bash
# Coordinator health
curl -s http://coordinator:8080/v1/cluster

# Worker discovery
curl -s http://coordinator:8080/v1/node | jq

# Load balancer healthcheck
location /health {
    proxy_pass http://trino;
    access_log off;
}
```

---

## Мониторинг

### Prometheus Metrics

Trino экспортирует метрики на endpoint `/v1/jmx/mbean`

#### Prometheus конфигурация

```yaml
scrape_configs:
  - job_name: 'trino-coordinator'
    metrics_path: /v1/jmx/mbean
    static_configs:
      - targets: ['coordinator:8080']
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance

  - job_name: 'trino-workers'
    metrics_path: /v1/jmx/mbean
    static_configs:
      - targets:
        - 'worker-1:8081'
        - 'worker-2:8081'
        - 'worker-3:8081'
        - 'worker-N:8081'
```

### Ключевые метрики

| Метрика | Описание | Threshold |
|---------|----------|-----------|
| `trino.server.query.input.rows` | Строки обработано | Monitor trends |
| `trino.server.query.execution.time` | Время выполнения | p95 < 30s для OLAP |
| `trino.memory.available_heap_bytes` | Свободная память | > query.max-memory |
| `trino.memory.pool.reserved` | Зарезервированная память | < 30% heap |
| `trino.operator.work_processor_output_rows` | Выходные строки | Monitor throughput |
| `trino.server.active_queries` | Активные запросы | Vary by workload |
| `trino.node.active_workers` | Active worker nodes | >= expected workers |
| `jvm.gc.collection.time` | Время GC | < 5% CPU time |

### Grafana Dashboards

#### Dashboard 1: Cluster Overview
- Active nodes
- Query count (running, queued, completed)
- Average query execution time
- Memory usage (heap, off-heap)
- GC statistics

```sql
-- Grafana panel: Active workers
SELECT
  time,
  value
FROM trino_node_active_workers
```

#### Dashboard 2: Query Performance
- Queries by status (running, queued, failed)
- Query duration distribution
- Input rows vs output rows
- CPU and memory per query

#### Dashboard 3: Resource Utilization
- Memory utilization (coordinator vs workers)
- CPU usage by node
- GC pause times
- Spill statistics

### Web UI Monitoring

Встроенное в Trino Web UI на `http://coordinator:8080/ui/`

**Доступная информация**:
- Active queries (real-time)
- Completed queries с метриками
- Worker nodes status
- Task distribution
- System settings

### Alerting Rules

```yaml
groups:
  - name: trino
    rules:
      - alert: TrinoCoordinatorDown
        expr: up{job="trino-coordinator"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Trino Coordinator is down"

      - alert: TrinoWorkerDown
        expr: count(up{job="trino-workers"}) < 3
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "{{ $value }} Trino workers are down"

      - alert: TrinoHighMemoryUsage
        expr: (trino.memory.pool.reserved) / (trino.memory.available_heap_bytes + trino.memory.pool.reserved) > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Memory usage is {{ $value | humanizePercentage }}"

      - alert: TrinoQueriesPiling
        expr: trino.server.queued_queries > 10
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "{{ $value }} queries queued"

      - alert: TrinoHighGCTime
        expr: rate(jvm_gc_collection_seconds_sum[5m]) > 0.05
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High GC time detected"
```

### Логи

```bash
# Web UI и query logs
/var/log/trino/server.log

# GC logs
/var/log/trino/gc.log

# Просмотр логов
docker logs -f trino-coordinator
docker logs -f trino-worker

# На хосте
tail -f /opt/trino/logs/server.log

# JSON format логов (production)
cat /opt/trino/logs/server.log | jq '.query_id, .message'
```

---

## Query оптимизация

### Connectors конфигурация

#### Hive Connector Optimization

```properties
# Partition projection (для large partitioned tables)
hive.partition-projection-enabled=true

# Statistics
hive.statistics-enabled=true
hive.statistics-collection-enabled=true

# Bucketing optimization
hive.bucketed-execution-enabled=true

# ORC vs Parquet (Parquet recommended for our use case)
hive.storage-format=PARQUET
```

#### Iceberg Connector Optimization

```properties
# Manifest file caching
iceberg.metadata-cache-ttl=10m

# Hidden partitions (для Iceberg partition evolution)
iceberg.hide-immutable-projections=true

# Statistics
iceberg.table-statistics-enabled=true
```

### Query Tuning

#### 1. Partition Pruning

```sql
-- GOOD: partition filtering
SELECT * FROM events
WHERE date = '2024-12-25'  -- partition column
  AND event_type = 'purchase';  -- regular column

-- BAD: no partition filtering
SELECT * FROM events
WHERE CAST(date AS date) = CAST('2024-12-25' AS date)
  AND event_type = 'purchase';
```

#### 2. Column Selection

```sql
-- GOOD: select only needed columns
SELECT user_id, event_id, timestamp
FROM events
WHERE date = '2024-12-25';

-- BAD: select all columns
SELECT *
FROM events
WHERE date = '2024-12-25';
```

#### 3. Predicate Pushdown

```sql
-- GOOD: filter before aggregation
SELECT user_id, COUNT(*) as count
FROM events
WHERE date = '2024-12-25'
  AND event_type = 'purchase'
GROUP BY user_id;

-- BAD: filter after aggregation (slower)
SELECT user_id, COUNT(*) as count
FROM events
WHERE date = '2024-12-25'
GROUP BY user_id
HAVING COUNT(*) > 10
  AND event_type = 'purchase';  -- Should be in WHERE
```

#### 4. Join Optimization

```sql
-- GOOD: broadcast join (small table on right)
SELECT a.id, a.name, b.total
FROM large_table a
JOIN small_dimension b ON a.dim_id = b.id;

-- GOOD: join columns have good selectivity
SELECT a.*, b.*
FROM events a
JOIN users b ON a.user_id = b.id  -- High selectivity join key
WHERE a.date = '2024-12-25';

-- BAD: join on low selectivity column
SELECT * FROM events
JOIN regions ON events.region_id = regions.id;  -- Should pre-filter
```

#### 5. Window Functions

```sql
-- GOOD: window function with partition
SELECT user_id,
       event_id,
       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp)
FROM events
WHERE date = '2024-12-25'
  AND event_type = 'purchase';

-- BAD: window function without partition (slower)
SELECT user_id,
       event_id,
       ROW_NUMBER() OVER (ORDER BY timestamp)  -- No partition
FROM events
WHERE date = '2024-12-25';
```

### Resource Configuration

```properties
# config.properties

# Memory per query
query.max-memory=4GB
query.max-memory-per-node=2GB

# Execution threads (workers)
task.max-worker-threads=1000  # Adjust based on CPU cores
task.min-drivers=2

# Writer configuration
task.writer-count=1  # Number of writer tasks
writer-min-size=64MB  # Min data before writing

# CPU settings
task.cpu-time-accounting-enabled=true
```

### Caching

Trino не имеет встроенного query caching, но можно использовать:

1. **View caching** (materialized tables):
```sql
CREATE TABLE mv_daily_sales AS
SELECT date, sum(amount) as total_sales
FROM events
WHERE event_type = 'purchase'
GROUP BY date;
```

2. **Hive Metastore caching**:
```properties
hive.metastore-cache-ttl=1h
```

3. **Application-level caching** (Redis, Memcached)

---

## Операции

### Добавление новых Worker nodes

Production scenario: добавить 2 новых workers к существующему кластеру

```bash
# 1. На новой машине подготовить инфраструктуру
apt-get update && apt-get upgrade -y
apt-get install -y curl docker.ce ntp
mkdir -p /opt/trino/{config,data,logs}
hostnamectl set-hostname trino-worker-7

# 2. Скопировать конфиги с существующего worker
scp -r existing-worker:/opt/trino/config/* /opt/trino/config/

# 3. Обновить node.properties
cat > /opt/trino/config/node.properties <<EOF
node.environment=production
node.id=worker-7
node.data-dir=/data/trino
node.location=<data-center>
node.internal-address=<new-worker-ip>
EOF

# 4. Запустить docker container
/opt/trino/docker-run-worker.sh

# 5. Проверить в coordinator
curl -s http://coordinator:8080/v1/node | jq '.[] | select(.nodeId | contains("worker-7"))'

# Новый worker должен появиться в течение 30 секунд
```

### Graceful Worker Shutdown

Для обновления или удаления worker без потери data:

```bash
# 1. На coordinator проверить active queries
curl -s http://coordinator:8080/v1/query | jq '.[] | select(.state=="RUNNING")'

# 2. На worker остановить прием новых queries
docker exec trino-worker curl -X PUT \
  http://localhost:8081/v1/service/shutting_down \
  -H "Content-Type: application/json" \
  -d '{"shutting_down": true}'

# 3. Дождаться завершения existing queries (обычно < 5 минут)
watch 'curl -s http://coordinator:8080/v1/query | jq "[.[] | select(.state==\"RUNNING\")] | length"'

# 4. Остановить worker
systemctl stop trino-worker

# Coordinator автоматически переставит running queries на другие workers
```

### Coordin тor Migration (для обновления версии)

```bash
# 1. Убедиться что есть backup конфигурации
cp -r /opt/trino/config /opt/trino/config.backup.$(date +%Y%m%d)

# 2. Обновить образ
docker pull trinodb/trino:latest

# 3. Остановить координатор
systemctl stop trino-coordinator

# 4. Обновить версию в docker-run-coordinator.sh
# trinodb/trino:latest

# 5. Запустить новый координатор
systemctl start trino-coordinator

# 6. Дождаться инициализации
sleep 30

# 7. Проверить здоровье кластера
curl -s http://coordinator:8080/v1/cluster | jq '.activeNodes'

# 8. Если есть проблемы, откатить:
systemctl stop trino-coordinator
docker pull trinodb/trino:<previous-version>
# Обновить docker-run-coordinator.sh
systemctl start trino-coordinator
```

### Скейлинг ресурсов

#### Увеличение heap size

```bash
# 1. Обновить jvm.config
cat > /opt/trino/config/jvm.config <<EOF
-server
-Xmx64G
-Xms64G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
EOF

# 2. Перезапустить
systemctl restart trino-worker

# 3. Проверить
docker exec trino-worker jps -l
# или
docker exec trino-worker ps aux | grep java
```

#### Увеличение query.max-memory

```bash
# 1. Обновить config.properties
cat > /opt/trino/config/config.properties <<EOF
query.max-memory=8GB
query.max-memory-per-node=4GB
EOF

# 2. Reload (некоторые параметры reload без перезапуска)
# curl -X POST http://coordinator:8080/v1/reload-configuration

# 3. Или перезапустить
systemctl restart trino-coordinator
systemctl restart trino-worker
```

---

## Troubleshooting

### Trino Coordinator не стартует

```bash
# 1. Проверить логи
docker logs trino-coordinator

# 2. Частые проблемы:
# - Не может подключиться к Hive Metastore
docker exec trino-coordinator curl -v telnet://hive-metastore:9083

# - Конфигурационный синтаксис ошибок
docker exec trino-coordinator cat /etc/trino/config.properties | grep -v '^#' | grep '='

# - Port уже занят
netstat -tuln | grep 8080
lsof -i :8080

# 3. Проверить конфиги
ls -la /opt/trino/config/
cat /opt/trino/config/config.properties

# 4. Дата/время (must match на всех nodes!)
date
timedatectl status

# 5. Проверить network connectivity
docker exec trino-coordinator nc -zv hive-metastore 9083
docker exec trino-coordinator nc -zv minio 9000
```

### Workers не подключаются к Coordinator

```bash
# На worker:
# 1. Проверить discovery.uri
docker exec trino-worker grep discovery.uri /etc/trino/config.properties

# 2. Проверить network connectivity
docker exec trino-worker curl http://<coordinator-ip>:8080/v1/cluster

# 3. Проверить DNS resolution
docker exec trino-worker nslookup trino-coordinator

# 4. Проверить порты
docker exec trino-worker nc -zv trino-coordinator 8080

# На coordinator:
# Проверить worker nodes статус
curl -s http://localhost:8080/v1/node | jq '.[] | {nodeId, state}'
```

### Out Of Memory (OOM) ошибки

```bash
# Признаки:
# - Docker контейнер crashed/restarted
# - Логи содержат "OutOfMemoryError"
# - Java heap dump в /var/log/trino/heap-dump.hprof

# Решение:
# 1. Увеличить heap size в jvm.config
-Xmx64G  # для workers
-Xmx32G  # для coordinator

# 2. Уменьшить query.max-memory
query.max-memory=2GB
query.max-memory-per-node=1GB

# 3. Включить spill (для large queries)
spill-enabled=true
spill-max-used-space=200GB

# 4. Увеличить worker nodes (horizontal scaling)

# 5. Проанализировать heap dump
jmap -histo:live /opt/trino/logs/heap-dump.hprof
# или использовать Eclipse Memory Analyzer
```

### Slow Queries / High Latency

```bash
# 1. Проверить активные queries
curl -s http://coordinator:8080/v1/query | jq '.[] | {queryId, state, elapsedTime}'

# 2. Проверить отдельный query
curl -s http://coordinator:8080/v1/query/<query-id> | jq '.stages'

# 3. Проверить worker load
curl -s http://coordinator:8080/v1/node | jq '.[] | {nodeId, activeTaskCount}'

# 4. Проверить resource usage
top -p $(docker inspect -f '{{.State.Pid}}' trino-coordinator)

# Решение:
# - Добавить workers (horizontal scaling)
# - Оптимизировать query (partition pruning, column selection)
# - Увеличить heap size
# - Проверить network latency между nodes
```

### Spill директория заполнена

```bash
# Признаки:
# - Disk space usage высокий
# - Logи содержат "No space left on device"

# Решение:
# 1. Проверить spill directory size
du -sh /opt/trino/data/

# 2. Очистить spill files (они временные)
rm -rf /opt/trino/data/spill/*

# 3. Увеличить disk space или
# 4. Уменьшить spill-max-used-space
spill-max-used-space=100GB

# 5. Добавить workers (распределить нагрузку)
```

### Hive Metastore Connection Issues

```bash
# 1. Проверить connectivity
docker exec trino-coordinator \
  nc -zv hive-metastore 9083

# 2. Проверить credentials
docker exec trino-coordinator \
  env | grep -i metastore

# 3. Проверить catalog конфиг
cat /opt/trino/config/catalog/hive.properties | grep metastore

# 4. Проверить Hive Metastore логи
docker logs hive-metastore | grep ERROR

# 5. Проверить network policies
netstat -tuln | grep 9083
ufw status numbered | grep 9083
```

### Query Execution Timeout

```bash
# Если query выполняется > query.max-execution-time

# 1. Увеличить timeout
query.max-execution-time=200h  # вместо 100d

# 2. Или оптимизировать query

# 3. Или добавить resources (workers, memory)

# 4. Проверить скорость обработки
docker logs trino-coordinator | grep "elapsed time"
```

### Coordinator Web UI не доступен

```bash
# 1. Проверить порт
netstat -tuln | grep 8080

# 2. Проверить reverse proxy (если есть Nginx)
docker logs trino-nginx

# 3. Проверить coordinator статус
systemctl status trino-coordinator
docker logs trino-coordinator

# 4. Проверить SSL (если включен HTTPS)
openssl s_client -connect localhost:8443

# 5. Firewall rules
ufw status numbered | grep 8080
```

---

## Дополнительные ресурсы

- [Trino Official Documentation](https://trino.io/docs/)
- [Trino GitHub](https://github.com/trinodb/trino)
- [Trino Performance Tuning](https://trino.io/docs/current/admin/performance-tuning.html)
- [Trino Configuration Reference](https://trino.io/docs/current/admin/properties.html)
- [Trino Connector Documentation](https://trino.io/docs/current/connector.html)
- [Iceberg Connector Guide](https://trino.io/docs/current/connector/iceberg.html)
- [Hive Connector Guide](https://trino.io/docs/current/connector/hive.html)
