# PostgreSQL Metastore Production Deployment Guide

## Содержание
1. [Обзор](#обзор)
2. [Архитектура](#архитектура)
3. [Требования](#требования)
4. [Production развертывание](#production-развертывание)
5. [Высокая доступность](#высокая-доступность)
6. [Мониторинг](#мониторинг)
7. [Backup и восстановление](#backup-и-восстановление)
8. [Операции](#операции)
9. [Troubleshooting](#troubleshooting)

---

## Обзор

PostgreSQL Metastore - это база данных для хранения метаданных Hive Metastore: информации о таблицах, партициях, схемах, локациях данных в MinIO.

### Роль в архитектуре
- Хранение метаданных Hive Metastore
- ACID транзакции для метаданных
- Основа для каталога таблиц Data Lake

### Зависимости
- **Входящие подключения**: Hive Metastore service
- **Исходящие подключения**: Standby реплика (для HA)

---

## Архитектура

### Single Instance (Development)
```
┌────────────────────┐
│  Hive Metastore    │
│    Service         │
└────────┬───────────┘
         │ JDBC
         ↓
┌────────────────────┐
│    PostgreSQL      │
│  Port: 5432        │
└────────┬───────────┘
         │
    ┌────▼────┐
    │ Volume  │
    │  /data  │
    └─────────┘
```

### High Availability (Production)
```
┌─────────────────┐      ┌─────────────────┐
│ Hive Metastore  │      │ Hive Metastore  │
│    Service 1    │      │    Service 2    │
└────────┬────────┘      └────────┬────────┘
         │                        │
         └────────┬───────────────┘
                  │ JDBC Connection Pool
                  ↓
         ┌────────────────┐
         │   PgBouncer    │ (Connection Pooling - опционально)
         │  or HAProxy    │
         └────────┬───────┘
                  │
         ┌────────▼───────────────┐
         │                        │
    ┌────▼──────┐          ┌──────▼─────┐
    │PostgreSQL │          │PostgreSQL  │
    │ PRIMARY   │──WAL────>│  STANDBY   │
    │Port: 5432 │Repl.     │Port: 5432  │
    └───────────┘          └────────────┘
    Streaming Replication
```

---

## Требования

### Аппаратные требования

#### Minimum (Development)
- **CPU**: 2 cores
- **RAM**: 4GB
- **Storage**: 20GB SSD
- **Network**: 1 Gbit/s

#### Recommended (Production)
- **CPU**: 4-8 cores
- **RAM**: 16-32GB
- **Storage**: 100-200GB SSD (NVMe preferred)
- **Network**: 10 Gbit/s

### База данных размеры

Для оценки размера БД:
- **Таблицы**: ~100KB метаданных на таблицу
- **Партиции**: ~50KB на партицию
- **Пример**: 1000 таблиц с 10000 партиций каждая = ~500GB метаданных

---

## Production развертывание

### 1. Подготовка сервера

```bash
# Обновление системы
apt-get update && apt-get upgrade -y

# Установка Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Создание директорий
mkdir -p /opt/postgres/data
mkdir -p /opt/postgres/config
mkdir -p /opt/postgres/backups
mkdir -p /opt/postgres/archive
```

### 2. Создание secrets

```bash
# Генерация паролей
export POSTGRES_PASSWORD=$(openssl rand -hex 32)
export REPLICATION_PASSWORD=$(openssl rand -hex 32)

# Сохранение в secrets файл
cat > /opt/postgres/.env <<EOF
POSTGRES_DB=metastore_db
POSTGRES_USER=hive
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
REPLICATION_USER=replicator
REPLICATION_PASSWORD=${REPLICATION_PASSWORD}
EOF

chmod 600 /opt/postgres/.env
```

### 3. PostgreSQL конфигурация

Создать `/opt/postgres/config/postgresql.conf`:

```ini
# === CONNECTION SETTINGS ===
listen_addresses = '*'
port = 5432
max_connections = 200
superuser_reserved_connections = 3

# === MEMORY ===
shared_buffers = 4GB                    # 25% of RAM
effective_cache_size = 12GB             # 75% of RAM
maintenance_work_mem = 1GB
work_mem = 20MB                         # shared_buffers / max_connections
wal_buffers = 16MB

# === QUERY TUNING ===
random_page_cost = 1.1                  # For SSD
effective_io_concurrency = 200          # For SSD
default_statistics_target = 100

# === WAL (Write-Ahead Logging) ===
wal_level = replica                     # For replication
fsync = on
synchronous_commit = on
wal_sync_method = fdatasync
full_page_writes = on
wal_compression = on
wal_log_hints = on

# WAL File Management
max_wal_size = 4GB
min_wal_size = 1GB
wal_keep_size = 1GB                     # Keep WAL for standby

# === REPLICATION ===
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
hot_standby_feedback = on

# === ARCHIVING (for PITR) ===
archive_mode = on
archive_command = 'cp %p /var/lib/postgresql/archive/%f'
archive_timeout = 300                   # 5 minutes

# === CHECKPOINTS ===
checkpoint_timeout = 10min
checkpoint_completion_target = 0.9
checkpoint_warning = 5min

# === LOGGING ===
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql-%Y-%m-%d_%H%M%S.log'
log_rotation_age = 1d
log_rotation_size = 100MB
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
log_timezone = 'UTC'

# Log slow queries
log_min_duration_statement = 1000       # Log queries > 1 second
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on
log_statement = 'ddl'                   # Log DDL statements
log_temp_files = 0

# === AUTOVACUUM ===
autovacuum = on
autovacuum_max_workers = 4
autovacuum_naptime = 30s
autovacuum_vacuum_threshold = 50
autovacuum_analyze_threshold = 50
autovacuum_vacuum_scale_factor = 0.1
autovacuum_analyze_scale_factor = 0.05

# === LOCALE ===
datestyle = 'iso, mdy'
timezone = 'UTC'
lc_messages = 'en_US.UTF-8'
lc_monetary = 'en_US.UTF-8'
lc_numeric = 'en_US.UTF-8'
lc_time = 'en_US.UTF-8'
default_text_search_config = 'pg_catalog.english'
```

Создать `/opt/postgres/config/pg_hba.conf`:

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# Local connections
local   all             all                                     peer

# IPv4 local connections
host    all             all             127.0.0.1/32            scram-sha-256

# Hive Metastore connections
host    metastore_db    hive            10.0.0.0/8              scram-sha-256

# Replication connections
host    replication     replicator      <standby-ip>/32         scram-sha-256

# Monitoring
host    all             postgres        <monitoring-ip>/32      scram-sha-256
```

### 4. Docker deployment (Primary)

Создать `/opt/postgres/docker-run-primary.sh`:

```bash
#!/bin/bash

source /opt/postgres/.env

docker run -d \
  --name postgres-metastore \
  --restart unless-stopped \
  --network host \
  -e POSTGRES_DB=${POSTGRES_DB} \
  -e POSTGRES_USER=${POSTGRES_USER} \
  -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
  -v /opt/postgres/data:/var/lib/postgresql/data \
  -v /opt/postgres/config/postgresql.conf:/etc/postgresql/postgresql.conf \
  -v /opt/postgres/config/pg_hba.conf:/etc/postgresql/pg_hba.conf \
  -v /opt/postgres/archive:/var/lib/postgresql/archive \
  -v /opt/postgres/backups:/backups \
  postgres:15-alpine \
  -c config_file=/etc/postgresql/postgresql.conf
```

Запуск:
```bash
chmod +x /opt/postgres/docker-run-primary.sh
/opt/postgres/docker-run-primary.sh

# Проверка
docker logs -f postgres-metastore
```

### 5. Создание replication user

```bash
docker exec -it postgres-metastore psql -U postgres -d metastore_db

-- Создать replication user
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '<replication-password>';

-- Проверить
\du
```

### 6. Systemd service

Создать `/etc/systemd/system/postgres-metastore.service`:

```ini
[Unit]
Description=PostgreSQL Metastore Database
Documentation=https://www.postgresql.org/docs/
After=docker.service
Requires=docker.service

[Service]
Type=simple
User=root
EnvironmentFile=/opt/postgres/.env
ExecStartPre=-/usr/bin/docker stop postgres-metastore
ExecStartPre=-/usr/bin/docker rm postgres-metastore
ExecStart=/usr/bin/docker run --rm \
  --name postgres-metastore \
  --network host \
  -e POSTGRES_DB=${POSTGRES_DB} \
  -e POSTGRES_USER=${POSTGRES_USER} \
  -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
  -v /opt/postgres/data:/var/lib/postgresql/data \
  -v /opt/postgres/config/postgresql.conf:/etc/postgresql/postgresql.conf \
  -v /opt/postgres/config/pg_hba.conf:/etc/postgresql/pg_hba.conf \
  -v /opt/postgres/archive:/var/lib/postgresql/archive \
  -v /opt/postgres/backups:/backups \
  postgres:15-alpine \
  -c config_file=/etc/postgresql/postgresql.conf
ExecStop=/usr/bin/docker stop postgres-metastore
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Активация:
```bash
systemctl daemon-reload
systemctl enable postgres-metastore
systemctl start postgres-metastore
systemctl status postgres-metastore
```

---

## Высокая доступность

### Streaming Replication Setup

#### 1. На Primary создать replication slot

```sql
-- Подключиться к primary
docker exec -it postgres-metastore psql -U postgres

-- Создать replication slot
SELECT * FROM pg_create_physical_replication_slot('standby_slot');

-- Проверить
SELECT * FROM pg_replication_slots;
```

#### 2. Настроить Standby сервер

На standby сервере:

```bash
# Остановить PostgreSQL если запущен
systemctl stop postgres-metastore

# Очистить data директорию
rm -rf /opt/postgres/data/*

# Создать базовый backup с Primary
docker run --rm \
  -v /opt/postgres/data:/var/lib/postgresql/data \
  postgres:15-alpine \
  pg_basebackup \
  -h <primary-ip> \
  -D /var/lib/postgresql/data \
  -U replicator \
  -P \
  -v \
  -R \
  -X stream \
  -C -S standby_slot

# -R создаст standby.signal и настроит recovery
# -X stream будет стримить WAL во время backup
# -C -S создаст replication slot
```

#### 3. Standby конфигурация

Создать `/opt/postgres/data/standby.signal` (пустой файл):
```bash
touch /opt/postgres/data/standby.signal
```

Добавить в `/opt/postgres/config/postgresql.conf`:
```ini
# === STANDBY SPECIFIC ===
primary_conninfo = 'host=<primary-ip> port=5432 user=replicator password=<repl-password> application_name=standby1'
primary_slot_name = 'standby_slot'
restore_command = 'cp /var/lib/postgresql/archive/%f %p'
recovery_target_timeline = 'latest'
```

#### 4. Запустить Standby

```bash
systemctl start postgres-metastore

# Проверить логи
journalctl -u postgres-metastore -f

# На Primary проверить репликацию
docker exec -it postgres-metastore psql -U postgres -c "SELECT * FROM pg_stat_replication;"

# Должны увидеть standby в списке
```

### Failover (Переключение на Standby)

#### Automatic Failover с Patroni (Рекомендуется)

Patroni - это HA решение для PostgreSQL с автоматическим failover.

```yaml
# patroni.yml на Primary
scope: postgres-metastore
name: postgres-primary

restapi:
  listen: 0.0.0.0:8008
  connect_address: <primary-ip>:8008

etcd:
  hosts: <etcd-cluster-ips>

bootstrap:
  dcs:
    ttl: 30
    loop_wait: 10
    retry_timeout: 10
    maximum_lag_on_failover: 1048576
    postgresql:
      use_pg_rewind: true
      parameters:
        max_connections: 200
        shared_buffers: 4GB

postgresql:
  listen: 0.0.0.0:5432
  connect_address: <primary-ip>:5432
  data_dir: /opt/postgres/data
  pgpass: /tmp/pgpass
  authentication:
    replication:
      username: replicator
      password: <repl-password>
    superuser:
      username: postgres
      password: <postgres-password>
  parameters:
    # Все параметры из postgresql.conf
```

#### Manual Failover

```bash
# На Standby сервере:

# 1. Остановить репликацию
docker exec -it postgres-metastore psql -U postgres -c "SELECT pg_promote();"

# 2. Standby становится Primary
# Удалить standby.signal
rm /opt/postgres/data/standby.signal

# 3. Перезапустить
systemctl restart postgres-metastore

# 4. Обновить DNS или Load Balancer чтобы указывал на новый Primary

# 5. Старый Primary нужно переконфигурировать как Standby
```

### Connection Pooling с PgBouncer

```ini
# /opt/pgbouncer/pgbouncer.ini
[databases]
metastore_db = host=<postgres-primary-ip> port=5432 dbname=metastore_db

[pgbouncer]
listen_addr = *
listen_port = 6432
auth_type = scram-sha-256
auth_file = /opt/pgbouncer/userlist.txt
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 50
reserve_pool_size = 10
reserve_pool_timeout = 5
server_idle_timeout = 600
log_connections = 1
log_disconnections = 1
```

Docker запуск PgBouncer:
```bash
docker run -d \
  --name pgbouncer \
  --restart unless-stopped \
  -p 6432:6432 \
  -v /opt/pgbouncer/pgbouncer.ini:/etc/pgbouncer/pgbouncer.ini \
  -v /opt/pgbouncer/userlist.txt:/etc/pgbouncer/userlist.txt \
  edoburu/pgbouncer
```

Hive Metastore подключается через PgBouncer:
```
jdbc:postgresql://pgbouncer:6432/metastore_db
```

---

## Мониторинг

### Prometheus postgres_exporter

```bash
# Запустить postgres_exporter
docker run -d \
  --name postgres-exporter \
  --restart unless-stopped \
  -p 9187:9187 \
  -e DATA_SOURCE_NAME="postgresql://postgres:<password>@<postgres-ip>:5432/metastore_db?sslmode=disable" \
  prometheuscommunity/postgres-exporter
```

### Ключевые метрики

| Метрика | Описание | Threshold |
|---------|----------|-----------|
| `pg_up` | Статус PostgreSQL | = 1 |
| `pg_stat_database_numbackends` | Активные подключения | < max_connections * 0.8 |
| `pg_stat_database_tup_inserted` | Вставки в секунду | Monitor trends |
| `pg_stat_database_tup_updated` | Обновления в секунду | Monitor trends |
| `pg_replication_lag` | Replication lag | < 10 seconds |
| `pg_stat_database_blks_hit / (pg_stat_database_blks_hit + pg_stat_database_blks_read)` | Cache hit ratio | > 0.95 |
| `pg_stat_activity_max_tx_duration` | Longest transaction | < 300s |

### Grafana Dashboard

- Dashboard ID: 9628 (PostgreSQL Database)
- URL: https://grafana.com/grafana/dashboards/9628

### Alerting Rules

```yaml
groups:
  - name: postgresql
    rules:
      - alert: PostgreSQLDown
        expr: pg_up == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "PostgreSQL is down"

      - alert: PostgreSQLReplicationLag
        expr: pg_replication_lag_seconds > 30
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Replication lag is {{ $value }}s"

      - alert: PostgreSQLConnectionsHigh
        expr: pg_stat_database_numbackends / pg_settings_max_connections > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High connection usage"

      - alert: PostgreSQLCacheHitRatioLow
        expr: |
          pg_stat_database_blks_hit /
          (pg_stat_database_blks_hit + pg_stat_database_blks_read) < 0.95
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Cache hit ratio is low"
```

---

## Backup и восстановление

### Automated Backups

#### 1. pg_dump (Logical Backup)

```bash
#!/bin/bash
# /opt/postgres/scripts/backup.sh

source /opt/postgres/.env

BACKUP_DIR="/opt/postgres/backups"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/metastore_${DATE}.dump"

# Full backup
docker exec postgres-metastore \
  pg_dump -U ${POSTGRES_USER} -F c -b -v \
  -f /backups/metastore_${DATE}.dump \
  ${POSTGRES_DB}

# Compress
gzip ${BACKUP_FILE}

# Удалить старые backups (старше 30 дней)
find ${BACKUP_DIR} -name "*.dump.gz" -mtime +30 -delete

# Upload to S3 (опционально)
aws s3 cp ${BACKUP_FILE}.gz s3://backups/postgres-metastore/
```

Cron job:
```cron
# Ежедневный backup в 2 AM
0 2 * * * /opt/postgres/scripts/backup.sh >> /var/log/postgres-backup.log 2>&1
```

#### 2. WAL Archiving (Continuous Archiving)

```bash
# В postgresql.conf уже настроено:
archive_mode = on
archive_command = 'cp %p /var/lib/postgresql/archive/%f'

# Синхронизация WAL в S3
#!/bin/bash
# /opt/postgres/scripts/archive-wal-to-s3.sh
aws s3 sync /opt/postgres/archive/ s3://backups/postgres-metastore/wal/ --delete
```

Cron job:
```cron
# Каждые 5 минут
*/5 * * * * /opt/postgres/scripts/archive-wal-to-s3.sh
```

### Восстановление

#### From pg_dump

```bash
# 1. Остановить Hive Metastore
# 2. Удалить существующую БД
docker exec -it postgres-metastore psql -U postgres -c "DROP DATABASE metastore_db;"

# 3. Создать новую БД
docker exec -it postgres-metastore psql -U postgres -c "CREATE DATABASE metastore_db OWNER hive;"

# 4. Восстановить из backup
gunzip /opt/postgres/backups/metastore_<timestamp>.dump.gz
docker exec -i postgres-metastore \
  pg_restore -U hive -d metastore_db -v \
  /backups/metastore_<timestamp>.dump

# 5. Запустить Hive Metastore
```

#### Point-In-Time Recovery (PITR)

```bash
# 1. Остановить PostgreSQL
systemctl stop postgres-metastore

# 2. Очистить data directory
rm -rf /opt/postgres/data/*

# 3. Восстановить базовый backup
tar -xzf /opt/postgres/backups/base_backup.tar.gz -C /opt/postgres/data/

# 4. Скопировать WAL files из archive
cp /opt/postgres/archive/* /opt/postgres/data/pg_wal/

# 5. Создать recovery.signal
touch /opt/postgres/data/recovery.signal

# 6. Настроить recovery target в postgresql.conf
cat >> /opt/postgres/config/postgresql.conf <<EOF
restore_command = 'cp /var/lib/postgresql/archive/%f %p'
recovery_target_time = '2024-12-25 14:30:00'  # Целевое время восстановления
recovery_target_action = 'promote'
EOF

# 7. Запустить PostgreSQL
systemctl start postgres-metastore

# 8. PostgreSQL восстановится до указанного времени и автоматически promote
```

---

## Операции

### Обновление PostgreSQL

```bash
# 1. Создать полный backup
/opt/postgres/scripts/backup.sh

# 2. Проверить совместимость версий
docker run --rm postgres:16-alpine pg_dump --version

# 3. Обновить на Standby первым
# Остановить
systemctl stop postgres-metastore

# Обновить версию в systemd service или docker-run скрипте
# Было: postgres:15-alpine
# Стало: postgres:16-alpine

# Запустить
systemctl start postgres-metastore

# 4. Failover на обновленный Standby
# 5. Обновить бывший Primary (теперь Standby)
```

### Вакуум и анализ

```bash
# Ручной вакуум
docker exec postgres-metastore psql -U postgres -d metastore_db -c "VACUUM VERBOSE ANALYZE;"

# Полный вакуум (требует downtime)
docker exec postgres-metastore psql -U postgres -d metastore_db -c "VACUUM FULL VERBOSE ANALYZE;"

# Проверить статистику автовакуума
docker exec postgres-metastore psql -U postgres -d metastore_db -c "
SELECT
  schemaname,
  relname,
  last_vacuum,
  last_autovacuum,
  last_analyze,
  last_autoanalyze
FROM pg_stat_user_tables
ORDER BY last_autovacuum DESC NULLS LAST;
"
```

### Реиндексация

```bash
# Реиндексировать всю БД
docker exec postgres-metastore psql -U postgres -d metastore_db -c "REINDEX DATABASE metastore_db;"

# Реиндексировать конкретную таблицу
docker exec postgres-metastore psql -U postgres -d metastore_db -c "REINDEX TABLE <table_name>;"
```

---

## Troubleshooting

### Высокое использование CPU

```sql
-- Найти медленные запросы
SELECT
  pid,
  now() - pg_stat_activity.query_start AS duration,
  query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY duration DESC;

-- Убить медленный запрос
SELECT pg_terminate_backend(<pid>);
```

### Connection limit reached

```sql
-- Проверить текущие подключения
SELECT count(*) FROM pg_stat_activity;

-- Увеличить max_connections (требует перезапуска)
-- В postgresql.conf:
max_connections = 300

-- Или использовать PgBouncer для connection pooling
```

### Replication lag

```sql
-- На Primary проверить lag
SELECT
  client_addr,
  state,
  sent_lsn,
  write_lsn,
  flush_lsn,
  replay_lsn,
  sync_state,
  pg_wal_lsn_diff(sent_lsn, replay_lsn) AS lag_bytes
FROM pg_stat_replication;

-- Возможные причины:
-- 1. Сетевая задержка
-- 2. High load на Standby
-- 3. Slow disk на Standby
```

### Disk space full

```bash
# Проверить размер WAL
du -sh /opt/postgres/data/pg_wal

# Очистить старые WAL (если archiving отключен)
docker exec postgres-metastore psql -U postgres -c "SELECT pg_switch_wal();"
docker exec postgres-metastore psql -U postgres -c "CHECKPOINT;"

# Проверить размер таблиц
docker exec postgres-metastore psql -U postgres -d metastore_db -c "
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 20;
"
```

---

## Дополнительные ресурсы

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Replication Tutorial](https://www.postgresql.org/docs/current/warm-standby.html)
- [Performance Tuning](https://wiki.postgresql.org/wiki/Performance_Optimization)
- [Patroni HA](https://github.com/patroni/patroni)
