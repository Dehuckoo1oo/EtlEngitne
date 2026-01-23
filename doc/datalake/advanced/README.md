# Production Data Lake Deployment Guide (Post-MVP)

> Этот раздел описывает продвинутые сценарии (HA, мониторинг, backup/DR, авторизация).
> Для MVP используйте `doc/datalake/README.md`, а список следующих шагов — `advanced/next-stage.md`.

## Содержание

1. [Обзор архитектуры](#обзор-архитектуры)
2. [Компоненты системы](#компоненты-системы)
3. [Требования к инфраструктуре](#требования-к-инфраструктуре)
4. [Порядок развертывания](#порядок-развертывания)
5. [Сетевая архитектура](#сетевая-архитектура)
6. [Мониторинг и операции](#мониторинг-и-операции)
7. [Безопасность](#безопасность)
8. [Backup и восстановление](#backup-и-восстановление)

---

## Обзор архитектуры

Data Lake представляет собой распределенную систему для хранения и аналитической обработки больших объемов данных в формате Parquet с использованием современного table format Apache Iceberg.

### Поток данных

```
┌──────────────┐
│    Kafka     │  Источник событий в реальном времени
└──────┬───────┘
       │
       ↓ Kafka Connect читает топики
┌──────────────┐
│Kafka Connect │  Преобразует в Parquet и сохраняет
└──────┬───────┘
       │
       ↓ Запись через S3 API
┌──────────────┐
│    MinIO     │  S3-совместимое объектное хранилище
└──────┬───────┘
       │
       ↓ Метаданные таблиц
┌──────────────┐
│PostgreSQL    │  Хранилище метаданных
│Metastore DB  │
└──────┬───────┘
       │
       ↓ Hive Metastore Service
┌──────────────┐
│Hive Metastore│  Сервис управления метаданными
└──────┬───────┘
       │
       ├─────────────────────────────────┐
       ↓ SQL запросы                     ↓ Distributed Processing
┌──────────────┐                ┌──────────────┐
│    Trino     │                │    Spark     │  Batch/Stream обработка
└──────┬───────┘                └──────┬───────┘
       │                               │
       └───────────────┬───────────────┘
                       ↓ Анализ данных
               ┌──────────────┐
               │   Jupyter    │  Аналитические ноутбуки (SQL + PySpark)
               └──────────────┘
```

---

## Компоненты системы

### 1. MinIO
- **Назначение**: S3-совместимое объектное хранилище для файлов Parquet
- **Порты**: 9000 (API), 9001 (Console)
- **Документация**: [minio.md](minio.md)

### 2. PostgreSQL Metastore
- **Назначение**: База данных для хранения метаданных Hive Metastore
- **Порты**: 5432
- **Документация**: [postgres-metastore.md](postgres-metastore.md)

### 3. Hive Metastore
- **Назначение**: Сервис управления метаданными таблиц Data Lake
- **Порты**: 9083 (Thrift API)
- **Документация**: [hive-metastore.md](hive-metastore.md)

### 4. Kafka Connect
- **Назначение**: Интеграция Kafka с MinIO (запись Parquet файлов)
- **Порты**: 8083 (REST API)
- **Документация**: [kafka-connect.md](kafka-connect.md)

### 5. Spark Cluster
- **Назначение**: Распределенная обработка больших данных (batch/stream)
- **Порты**: 7077 (Master), 8080 (Master UI), 8081 (Worker UI), 4040 (App UI)
- **Документация**: [spark.md](../spark.md)

### 6. Trino
- **Назначение**: Распределенный SQL движок для аналитических запросов
- **Порты**: 8080 (HTTP API, Web UI)
- **Документация**: [trino.md](trino.md)

### 7. Jupyter
- **Назначение**: Интерактивная среда для анализа данных
- **Порты**: 8888 (Web UI)
- **Документация**: [jupyter.md](jupyter.md)

---

## Требования к инфраструктуре

### Минимальные требования для Production

#### MinIO Cluster (3-4 узла)
- **CPU**: 4 cores per node
- **RAM**: 16GB per node
- **Storage**: SSD RAID 10, 1TB+ per node
- **Network**: 10 Gbit/s

#### PostgreSQL Metastore (1 узел + standby)
- **CPU**: 4 cores
- **RAM**: 8GB
- **Storage**: SSD 100GB
- **Network**: 1 Gbit/s

#### Hive Metastore (2-3 узла для HA)
- **CPU**: 2 cores per node
- **RAM**: 4GB per node
- **Storage**: 50GB per node
- **Network**: 1 Gbit/s

#### Trino Coordinator (1 узел)
- **CPU**: 8 cores
- **RAM**: 32GB
- **Storage**: SSD 200GB
- **Network**: 10 Gbit/s

#### Trino Workers (5+ узлов)
- **CPU**: 16 cores per node
- **RAM**: 64GB per node
- **Storage**: SSD 500GB per node
- **Network**: 10 Gbit/s

#### Kafka Connect (2-3 узла)
- **CPU**: 4 cores per node
- **RAM**: 8GB per node
- **Storage**: 100GB per node
- **Network**: 10 Gbit/s

#### Spark Master (1 узел)
- **CPU**: 4 cores
- **RAM**: 8GB
- **Storage**: 50GB SSD
- **Network**: 10 Gbit/s

#### Spark Workers (3-10 узлов)
- **CPU**: 16 cores per node
- **RAM**: 64GB per node
- **Storage**: 500GB SSD per node
- **Network**: 10 Gbit/s

#### Jupyter (1 узел)
- **CPU**: 4 cores
- **RAM**: 16GB
- **Storage**: 200GB
- **Network**: 1 Gbit/s

### Сетевые требования

- **Внутренняя сеть**: 10 Gbit/s между всеми узлами
- **DNS**: Требуется корректное разрешение имен между сервисами
- **Firewall**: Настроить правила для портов между сервисами

---

## Порядок развертывания

### Этап 1: Подготовка инфраструктуры

1. Подготовить виртуальные машины или физические серверы
2. Установить Docker Engine на все узлы
3. Настроить DNS записи для всех сервисов
4. Настроить сетевые правила firewall
5. Подготовить персистентные хранилища (volumes)

### Этап 2: Развертывание хранилища (Storage Layer)

```bash
# Последовательность развертывания:
1. MinIO cluster
2. PostgreSQL Metastore (primary + standby)
```

**Зависимости**: Нет

**Документация**:
- [MinIO Setup](minio.md#production-deployment)
- [PostgreSQL Setup](postgres-metastore.md#production-deployment)

### Этап 3: Развертывание метаданных (Metadata Layer)

```bash
# После готовности Storage Layer:
3. Hive Metastore cluster
```

**Зависимости**:
- MinIO (готов и доступен)
- PostgreSQL Metastore (готов и доступен)

**Документация**: [Hive Metastore Setup](hive-metastore.md#production-deployment)

### Этап 4: Развертывание интеграции (Integration Layer)

```bash
# После готовности Metadata Layer:
4. Kafka Connect cluster
```

**Зависимости**:
- Kafka brokers (должны быть развернуты отдельно)
- Schema Registry (должен быть развернут отдельно)
- MinIO (готов)
- Hive Metastore (готов - опционально, для записи метаданных)

**Документация**: [Kafka Connect Setup](kafka-connect.md#production-deployment)

### Этап 5: Развертывание query engine (Query Layer)

```bash
# После готовности Metadata Layer:
5. Trino Coordinator
6. Trino Workers (scale из 5+ узлов)
```

**Зависимости**:
- Hive Metastore (готов)
- MinIO (готов)

**Документация**: [Trino Setup](trino.md#production-deployment)

### Этап 6: Развертывание аналитики (Analytics Layer)

```bash
# После готовности всех предыдущих слоев:
7. Jupyter
```

**Зависимости**:
- Trino (готов)
- MinIO (готов)

**Документация**: [Jupyter Setup](jupyter.md#production-deployment)

### Граф зависимостей

```
MinIO ──────────────────┐
                        ├──> Hive Metastore ──┬──> Trino (Coordinator + Workers) ─┬──> Jupyter
PostgreSQL Metastore ───┘                     │                                    │
                                              └──> Spark (Master + Workers) ───────┘
Kafka + Schema Registry ──> Kafka Connect ────────> MinIO
```

---

## Сетевая архитектура

### Сетевая топология

```
┌─────────────────────────────────────────────────────────────┐
│                     Public Network (DMZ)                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Load Balancer│  │ Load Balancer│  │ Load Balancer│      │
│  │  (MinIO UI)  │  │  (Trino UI)  │  │  (Jupyter)   │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
└─────────┼──────────────────┼──────────────────┼─────────────┘
          │                  │                  │
┌─────────┼──────────────────┼──────────────────┼─────────────┐
│         │    Application Network (Private)    │             │
│  ┌──────▼───────┐  ┌──────▼───────┐  ┌────────▼─────┐      │
│  │ MinIO Cluster│  │Trino Cluster │  │   Jupyter    │      │
│  │ (3-4 nodes)  │  │(Coord+Workers)│  │              │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                  │                  │              │
│  ┌──────▼──────────────────▼──────────────────▼────────┐    │
│  │              Hive Metastore (2-3 nodes)             │    │
│  └──────────────────────┬──────────────────────────────┘    │
│                         │                                    │
│  ┌──────────────────────▼──────────────────────────────┐    │
│  │        PostgreSQL Metastore (Primary + Standby)     │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         Kafka Connect Cluster (2-3 nodes)            │   │
│  └──────────────────────┬───────────────────────────────┘   │
└─────────────────────────┼────────────────────────────────────┘
                          │
┌─────────────────────────┼────────────────────────────────────┐
│                         │  Kafka Network                     │
│  ┌──────────────────────▼───────────────────────────────┐   │
│  │         Kafka Cluster + Schema Registry              │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

### Таблица портов и коммуникации

| Сервис | Порт | Протокол | Источник | Назначение |
|--------|------|----------|----------|------------|
| MinIO API | 9000 | HTTP/S3 | Kafka Connect, Trino, Spark, Jupyter, Hive Metastore | Чтение/запись данных |
| MinIO Console | 9001 | HTTP/HTTPS | Администраторы (через LB) | Управление |
| PostgreSQL | 5432 | PostgreSQL | Hive Metastore | Метаданные |
| Hive Metastore | 9083 | Thrift | Trino, Spark, Kafka Connect (опц.) | Метаданные таблиц |
| Kafka Connect | 8083 | HTTP | Администраторы, мониторинг | REST API |
| Spark Master | 7077 | Spark Protocol | Spark Workers, Jupyter | Cluster communication |
| Spark Master UI | 8080 | HTTP | Администраторы | Web UI |
| Spark Worker UI | 8081 | HTTP | Администраторы | Web UI |
| Spark App UI | 4040 | HTTP | Jupyter (локально) | Application UI |
| Trino | 8080 | HTTP | Jupyter, клиенты, админы (через LB) | SQL queries, Web UI |
| Jupyter | 8888 | HTTP | Аналитики (через LB) | Notebooks |

### Сетевые политики (Firewall Rules)

#### MinIO
```bash
# Входящие
ALLOW TCP 9000 FROM kafka-connect-nodes, trino-nodes, jupyter-node, hive-metastore-nodes
ALLOW TCP 9001 FROM load-balancer-public

# Исходящие
ALLOW ALL (для распределенного режима между MinIO узлами)
```

#### PostgreSQL Metastore
```bash
# Входящие
ALLOW TCP 5432 FROM hive-metastore-nodes
DENY TCP 5432 FROM ALL

# Исходящие
ALLOW TCP 5432 TO standby-node (для репликации)
```

#### Hive Metastore
```bash
# Входящие
ALLOW TCP 9083 FROM trino-nodes, kafka-connect-nodes

# Исходящие
ALLOW TCP 5432 TO postgres-metastore-node
ALLOW TCP 9000 TO minio-nodes
```

#### Kafka Connect
```bash
# Входящие
ALLOW TCP 8083 FROM monitoring-systems, admin-networks

# Исходящие
ALLOW TCP 19092 TO kafka-brokers
ALLOW TCP 8081 TO schema-registry
ALLOW TCP 9000 TO minio-nodes
ALLOW TCP 9083 TO hive-metastore-nodes (опционально)
```

#### Trino
```bash
# Входящие (Coordinator)
ALLOW TCP 8080 FROM load-balancer-public, jupyter-node, admin-networks

# Входящие (Workers)
ALLOW TCP 8080 FROM trino-coordinator

# Исходящие (All Trino nodes)
ALLOW TCP 9083 TO hive-metastore-nodes
ALLOW TCP 9000 TO minio-nodes
```

#### Jupyter
```bash
# Входящие
ALLOW TCP 8888 FROM load-balancer-public

# Исходящие
ALLOW TCP 8080 TO trino-coordinator
ALLOW TCP 9000 TO minio-nodes
```

---

## Мониторинг и операции

### Метрики для мониторинга

#### MinIO
- **Storage metrics**: Used space, available space, bucket count
- **Performance**: Requests/sec, bandwidth in/out, latency (p50/p95/p99)
- **Availability**: Node health, cluster health
- **Tool**: MinIO встроенный Prometheus endpoint на `/minio/v2/metrics/cluster`

#### PostgreSQL
- **Connections**: Active connections, max connections
- **Performance**: Query duration, transactions/sec, cache hit ratio
- **Replication**: Replication lag, standby status
- **Tool**: postgres_exporter для Prometheus

#### Hive Metastore
- **JVM**: Heap usage, GC time, thread count
- **API**: Request count, error rate, latency
- **Database**: Connection pool usage
- **Tool**: JMX exporter для Prometheus

#### Kafka Connect
- **Connectors**: Status, task count, offset lag
- **Performance**: Records processed/sec, bytes/sec
- **Errors**: Failed tasks, error count
- **Tool**: Kafka Connect REST API + JMX metrics

#### Spark
- **Cluster**: Active workers, executor count, available memory/cores
- **Applications**: Running apps, completed apps, failed apps
- **Jobs**: Active jobs, completed stages, failed tasks
- **Performance**: Shuffle read/write, GC time, task duration
- **Tool**: Spark Master UI, Spark History Server, JMX/Prometheus metrics

#### Trino
- **Queries**: Active queries, queued queries, completed queries
- **Performance**: Query duration, CPU usage, memory usage
- **Failures**: Failed queries, killed queries
- **Cluster**: Active workers, coordinator health
- **Tool**: Trino Web UI `/v1/cluster`, JMX metrics

#### Jupyter
- **Users**: Active sessions, kernel count
- **Resources**: CPU, memory per session
- **Tool**: Standard container metrics

### Логирование

Централизованное логирование через:
- **ELK Stack** (Elasticsearch, Logstash, Kibana)
- **Grafana Loki**
- **Cloud logging** (CloudWatch, Stackdriver)

Логи каждого сервиса направляются в centralized logging system.

### Alerting

Критические алерты:
- MinIO: Node down, high error rate, storage > 80%
- PostgreSQL: Replication lag > 10s, connections > 80%
- Hive Metastore: Service unavailable, high error rate
- Kafka Connect: Connector failed, high lag
- Trino: Coordinator down, worker down, too many failed queries
- Jupyter: High memory usage, kernel crashes

---

## Безопасность

### 1. Аутентификация и авторизация

#### MinIO
- Использовать IAM policies для fine-grained доступа
- Создать отдельные service accounts для каждого сервиса
- Включить bucket versioning для защиты от случайного удаления

#### PostgreSQL
- Использовать сильные пароли (32+ символов)
- Ограничить доступ по IP (pg_hba.conf)
- Включить SSL/TLS соединения

#### Hive Metastore
- Kerberos authentication (для enterprise)
- LDAP/AD интеграция

#### Trino
- LDAP/AD authentication
- File-based access control или OPA (Open Policy Agent)
- HTTPS для всех коннекций

#### Jupyter
- OAuth2 / LDAP authentication
- JupyterHub для multi-user окружения

### 2. Шифрование

#### В покое (At Rest)
- MinIO: Server-side encryption (SSE-S3, SSE-KMS)
- PostgreSQL: Transparent Data Encryption (TDE) или disk encryption
- Volumes: LUKS encryption для всех персистентных volumes

#### В транзите (In Transit)
- Все сервисы: TLS/SSL для всех коммуникаций
- MinIO: HTTPS
- PostgreSQL: SSL connections
- Hive Metastore: SASL/Kerberos
- Trino: HTTPS
- Kafka Connect: SSL для Kafka connections

### 3. Сетевая изоляция

- Использовать private subnets для всех backend сервисов
- Public доступ только через Load Balancers с WAF
- VPN для административного доступа

### 4. Secrets Management

Использовать один из:
- **HashiCorp Vault**
- **AWS Secrets Manager**
- **Azure Key Vault**
- **Kubernetes Secrets** (с encryption at rest)

Никогда не хранить credentials в:
- Environment variables напрямую
- Configuration files в Git
- Container images

---

## Backup и восстановление

### MinIO

#### Backup стратегия
```bash
# Опция 1: MinIO Mirror (репликация в другой S3)
mc mirror --watch source-minio/datalake backup-minio/datalake-backup

# Опция 2: MinIO to Cloud (AWS S3, GCS, Azure Blob)
mc mirror --watch minio/datalake s3/backup-bucket

# Опция 3: Snapshots (если используется versioning)
# Включить bucket versioning
mc version enable minio/datalake
```

#### Retention policy
- **Hot data**: 7 дней на primary
- **Warm data**: 30 дней на secondary site
- **Cold data**: > 30 дней в cloud archive (Glacier, Coldline)

### PostgreSQL Metastore

#### Backup стратегия
```bash
# Ежедневный full backup
pg_dump -h postgres-metastore -U hive -F c metastore_db > metastore_$(date +%Y%m%d).dump

# Continuous archiving (WAL shipping)
# В postgresql.conf:
archive_mode = on
archive_command = 'cp %p /mnt/wal_archive/%f'

# PITR (Point-In-Time Recovery) готовность
```

#### Retention policy
- **Full backups**: 30 дней
- **WAL archives**: 7 дней
- **Offsite copies**: 90 дней

### Hive Metastore

Метаданные хранятся в PostgreSQL, backup не требуется.

### Kafka Connect

#### Backup стратегия
- Connector configurations: хранить в Git
- Offsets: хранятся в Kafka topics (_connect-offsets)
- Kafka topics backup: зависит от Kafka backup стратегии

### Trino

Stateless сервис, backup не требуется. Конфигурация должна храниться в Git.

### Jupyter Notebooks

#### Backup стратегия
```bash
# Автоматический backup notebooks в Git
# Cron job каждый час
0 * * * * cd /jupyter-notebooks && git add . && git commit -m "Auto backup" && git push
```

---

## Disaster Recovery

### RTO (Recovery Time Objective) и RPO (Recovery Point Objective)

| Компонент | RTO | RPO |
|-----------|-----|-----|
| MinIO | 1 час | 5 минут (репликация) |
| PostgreSQL | 30 минут | 1 минута (WAL) |
| Hive Metastore | 15 минут | depends on PostgreSQL |
| Kafka Connect | 15 минут | 0 (Kafka offsets) |
| Spark | 15 минут | 0 (stateless) |
| Trino | 10 минут | 0 (stateless) |
| Jupyter | 30 минут | 1 час (Git backup) |

### DR План

1. **MinIO Failure**: Переключение на replica site
2. **PostgreSQL Failure**: Promote standby to primary
3. **Hive Metastore Failure**: Запустить новый инстанс (stateless)
4. **Kafka Connect Failure**: Запустить новые workers, connectors восстановятся из Kafka topics
5. **Spark Failure**: Запустить новые Master/Workers (stateless, running jobs будут перезапущены)
6. **Trino Failure**: Запустить новые coordinator/workers
7. **Jupyter Failure**: Восстановить из Git backup

---

## Дополнительные ресурсы

- [MinIO Production Deployment](minio.md#production-deployment)
- [PostgreSQL HA Setup](postgres-metastore.md#high-availability)
- [Hive Metastore Production Guide](hive-metastore.md#production-deployment)
- [Kafka Connect Production Configuration](kafka-connect.md#production-deployment)
- [Spark Cluster Production Guide](../spark.md#порядок-развертывания)
- [Trino Production Deployment](trino.md#production-deployment)
- [Jupyter Multi-User Setup](jupyter.md#jupyterhub-deployment)

---

## Чеклист развертывания

### Pre-deployment
- [ ] Инфраструктура подготовлена (VM/серверы)
- [ ] DNS записи настроены
- [ ] Firewall правила настроены
- [ ] Персистентные хранилища подготовлены
- [ ] Secrets management система развернута
- [ ] Мониторинг система готова
- [ ] Backup система готова

### Deployment
- [ ] MinIO cluster развернут и протестирован
- [ ] PostgreSQL primary + standby развернуты
- [ ] Hive Metastore cluster развернут
- [ ] Kafka Connect cluster развернут
- [ ] Spark Master развернут
- [ ] Spark Workers развернуты и зарегистрированы
- [ ] Trino coordinator развернут
- [ ] Trino workers развернуты и зарегистрированы
- [ ] Jupyter развернут (с PySpark интеграцией)

### Post-deployment
- [ ] Интеграционные тесты выполнены
- [ ] Monitoring dashboards настроены
- [ ] Alerting правила настроены
- [ ] Backup jobs настроены и протестированы
- [ ] DR процедуры документированы и протестированы
- [ ] Runbook создан для операционной команды
- [ ] Обучение команды проведено

---

## Поддержка и контакты

Для вопросов и проблем:
- **Документация**: См. детальные гайды по каждому сервису
- **Runbook**: Операционные процедуры для типовых задач
- **On-call**: Контакты дежурной команды
