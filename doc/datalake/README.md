# Data Lake MVP Deployment Guide

## Цель MVP

Быстро развернуть Data Lake для демонстрации возможности работы с большими объемами данных (2+ TB) с высокой скоростью обработки.

## Архитектура MVP

```
┌──────────────┐
│    Kafka     │  Источник событий (уже развернут)
└──────┬───────┘
       │
       ↓
┌──────────────┐
│Kafka Connect │  Преобразование в Parquet + запись в MinIO
│ (1 instance) │
└──────┬───────┘
       │
       ↓
┌──────────────┐
│    MinIO     │  S3-совместимое хранилище Parquet файлов
│ (1 instance) │
└──────┬───────┘
       │
       ↓
┌──────────────┐
│  PostgreSQL  │  Метаданные о таблицах
│ (1 instance) │
└──────┬───────┘
       │
       ↓
┌──────────────┐
│Hive Metastore│  Каталог таблиц Data Lake
│ (1 instance) │
└──────┬───────┘
       │
       ↓
┌──────────────┐
│    Trino     │  SQL запросы к данным
│ (1 instance) │
└──────┬───────┘
       │
       ↓
┌──────────────┐
│   Jupyter    │  Аналитика и визуализация
│ (1 instance) │
└──────────────┘
```

**Один сервис = Одна машина = Один Docker контейнер**

MVP-ограничения:
- один инстанс каждого сервиса, без балансировщиков, координаторов и кластеров
- мониторинг, backup, DR и пользовательская авторизация — вне MVP (см. [advanced/next-stage.md](advanced/next-stage.md))
- healthcheck остаются в Dockerfile каждого сервиса
- все конфиги хранятся в GitLab рядом с Dockerfile и копируются в образ в Dockerfile
- `data-lake/` и `compose.yaml` в репозитории — только для локального dev, не менять для прод

---

## Компоненты системы

| Сервис | Порт | Машина | Размер данных | Документация |
|--------|------|--------|---------------|--------------|
| MinIO | 9000, 9001 | minio.company.com | 2+ TB | [minio.md](minio.md) |
| PostgreSQL | 5432 | postgres-metastore.company.com | ~10 GB | [postgres-metastore.md](postgres-metastore.md) |
| Hive Metastore | 9083 | hive-metastore.company.com | Stateless | [hive-metastore.md](hive-metastore.md) |
| Kafka Connect | 8083 | kafka-connect.company.com | Stateless | [kafka-connect.md](kafka-connect.md) |
| Trino | 8080 | trino.company.com | Stateless | [trino.md](trino.md) |
| Jupyter | 8888 | jupyter.company.com | ~100 GB | [jupyter.md](jupyter.md) |

---

## Предварительные требования

### На всех машинах уже настроено:
- ✅ ОС Linux (Ubuntu/CentOS)
- ✅ Диски смонтированы
- ✅ Firewall порты открыты
- ✅ DNS настроен
- ✅ SSL сертификаты размещены в `/etc/ssl/certs/`

### Нужно установить:
- Docker Engine

### Нужно прокинуть в контейнеры:
- SSL сертификаты из `/etc/ssl/certs/` (COPY в Dockerfile или volume mount)

---

## Порядок развертывания

### Этап 1: Установка Docker на всех машинах

```bash
# На каждой машине
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
systemctl enable docker
systemctl start docker
```

### Этап 2: Развертывание сервисов (в порядке зависимостей)

**ВАЖНО**: Сервисы должны разворачиваться строго в указанном порядке!

1. **MinIO** (независимый) → [Инструкция](minio.md)
   - Создать bucket `datalake`
   - Создать пользователей: `hive-metastore`, `kafka-connect`, `trino`

2. **PostgreSQL** (независимый) → [Инструкция](postgres-metastore.md)
   - База данных `metastore_db` создается автоматически
   - Пользователь `hive` создается автоматически

3. **Hive Metastore** (зависит от: PostgreSQL, MinIO) → [Инструкция](hive-metastore.md)
   - ⚠️ Убедитесь что PostgreSQL и MinIO запущены и доступны
   - Схема БД создается автоматически при первом запуске

4. **Kafka Connect** (зависит от: Kafka, Schema Registry, MinIO) → [Инструкция](kafka-connect.md)
   - ⚠️ Убедитесь что Kafka и Schema Registry доступны

5. **Trino** (зависит от: Hive Metastore, MinIO) → [Инструкция](trino.md)
   - ⚠️ Убедитесь что Hive Metastore доступен

6. **Jupyter** (зависит от: Trino, MinIO) → [Инструкция](jupyter.md)
   - ⚠️ Убедитесь что Trino доступен

Запуск каждого сервиса выполняется через GitLab CI/CD или вручную (варианты есть в сервисных инструкциях).

---

## GitLab CI/CD структура

```
data-lake/
├── minio/
│   ├── Dockerfile
│   ├── .env.example
│   └── .gitlab-ci.yml
├── postgres-metastore/
│   ├── Dockerfile
│   ├── postgresql.conf
│   ├── pg_hba.conf
│   ├── .env.example
│   └── .gitlab-ci.yml
├── hive-metastore/
│   ├── Dockerfile
│   ├── config/
│   │   └── core-site.xml
│   ├── .env.example
│   └── .gitlab-ci.yml
├── kafka-connect/
│   ├── Dockerfile
│   ├── config/
│   │   └── connect-distributed.properties
│   ├── connectors/
│   │   └── s3-sink-order-events.json
│   ├── .env.example
│   └── .gitlab-ci.yml
├── trino/
│   ├── Dockerfile
│   ├── config/
│   │   ├── config.properties
│   │   ├── jvm.config
│   │   ├── node.properties
│   │   └── catalog/
│   │       ├── iceberg.properties
│   │       └── hive.properties
│   ├── .env.example
│   └── .gitlab-ci.yml
└── jupyter/
    ├── Dockerfile
    ├── requirements.txt
    ├── .env.example
    └── .gitlab-ci.yml
```

### Пример GitLab CI/CD pipeline

```yaml
# .gitlab-ci.yml (в корне каждого сервиса)
stages:
  - build
  - deploy

variables:
  IMAGE_TAG: ${CI_COMMIT_REF_NAME}-${CI_COMMIT_SHORT_SHA}
  REGISTRY: registry.company.com

build:
  stage: build
  script:
    - docker build -t ${REGISTRY}/${CI_PROJECT_NAME}:${IMAGE_TAG} .
    - docker push ${REGISTRY}/${CI_PROJECT_NAME}:${IMAGE_TAG}
  only:
    - main
    - develop

deploy:
  stage: deploy
  script:
    - ssh deploy@${TARGET_HOST} "docker pull ${REGISTRY}/${CI_PROJECT_NAME}:${IMAGE_TAG}"
    - ssh deploy@${TARGET_HOST} "docker stop ${CI_PROJECT_NAME} || true"
    - ssh deploy@${TARGET_HOST} "docker rm ${CI_PROJECT_NAME} || true"
    - ssh deploy@${TARGET_HOST} "docker run -d --name ${CI_PROJECT_NAME} --env-file /opt/${CI_PROJECT_NAME}/.env ${REGISTRY}/${CI_PROJECT_NAME}:${IMAGE_TAG}"
  only:
    - main
  when: manual
```

---

## Конфигурация для больших данных (2+ TB)

### MinIO
- **Storage**: 5TB+ диск (с учетом роста)
- **Memory**: 16GB RAM minimum
- **CPU**: 8 cores

### Kafka Connect
- **Heap**: 8GB (`KAFKA_HEAP_OPTS="-Xms8g -Xmx8g"`)
- **Tasks**: 12+ параллельных задач
- **Flush size**: 50000 records или 5 минут

### Kafka topics (2+ TB)
- **Partitions**: 48-96 (зависит от throughput)
- **Replication factor**: 3
- **retention.bytes**: 3-5 TB (или `-1` при использовании `retention.ms`)
- **segment.bytes**: 1073741824 (1 GB)
- **min.insync.replicas**: 2
- **max.message.bytes**: 10485760 (10 MB)

Пример настройки топика:
```bash
kafka-configs.sh --bootstrap-server kafka-broker-1:9092 \
  --alter --entity-type topics --entity-name order-events \
  --add-config retention.bytes=5497558138880,segment.bytes=1073741824,min.insync.replicas=2,max.message.bytes=10485760
```

### Trino
- **Heap**: 32GB (`-Xmx32G -Xms32G`)
- **Query memory**: 16GB per query
- **Disk spill**: Включен для больших JOIN операций

### PostgreSQL
- **shared_buffers**: 8GB
- **effective_cache_size**: 24GB
- **work_mem**: 32MB

---

## Health Checks

Каждый сервис имеет health check endpoint:

```bash
# MinIO
curl -f http://minio.company.com:9000/minio/health/live

# PostgreSQL
pg_isready -h postgres-metastore.company.com -U hive

# Hive Metastore
nc -zv hive-metastore.company.com 9083

# Kafka Connect
curl -f http://kafka-connect.company.com:8083/

# Trino
curl -f http://trino.company.com:8080/v1/info

# Jupyter
curl -f http://jupyter.company.com:8888/api
```

---

## Быстрый старт

### 1. Клонировать репозиторий
```bash
git clone https://gitlab.company.com/datalake/infrastructure.git
cd infrastructure
```

### 2. Развернуть по очереди
```bash
# На каждой машине:
cd <service-name>
cp .env.example .env
# Отредактировать .env
docker build -t <service-name>:latest .
docker run -d --name <service-name> --env-file .env <service-name>:latest
```

### 3. Инициализация сервисов

После развертывания каждого сервиса необходимо выполнить инициализацию:

#### MinIO
```bash
# Создать bucket и пользователей
mc alias set datalake https://minio.company.com:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
mc mb datalake/datalake
mc admin user add datalake hive-metastore <PASSWORD>
mc admin user add datalake kafka-connect <PASSWORD>
mc admin user add datalake trino <PASSWORD>
# См. подробности в minio.md
```

#### PostgreSQL
```bash
# Проверить что база данных создана
PGPASSWORD='<PASSWORD>' psql -h postgres-metastore.company.com -U hive -d metastore_db -c "SELECT 1;"
# База данных и пользователь создаются автоматически
```

#### Hive Metastore
```bash
# Проверить зависимости перед запуском
pg_isready -h postgres-metastore.company.com -U hive
curl -f https://minio.company.com:9000/minio/health/live
# Схема БД создается автоматически при первом запуске Hive Metastore
```

### 4. Проверить работоспособность
```bash
# Выполнить health checks из списка ниже
```

---

## Следующие шаги (после MVP)

Расширенные возможности вынесены в отдельный файл:
📘 [Продвинутые возможности](advanced/next-stage.md)

---

## Troubleshooting

### Сервис не запускается
1. Проверить логи: `docker logs <container-name>`
2. Проверить переменные окружения: `docker exec <container> env`
3. Проверить доступность зависимостей

### Проблемы с подключением
1. Проверить DNS: `ping <hostname>`
2. Проверить порты: `nc -zv <hostname> <port>`
3. Проверить firewall: `iptables -L -n`

### Performance проблемы
1. Проверить CPU/Memory: `docker stats`
2. Проверить disk I/O: `iostat -x 5`
3. Увеличить heap для Java сервисов

---

## Поддержка

- **Документация по сервисам**: См. соответствующие .md файлы
- **GitLab Issues**: https://gitlab.company.com/datalake/infrastructure/issues
- **Internal Wiki**: https://wiki.company.com/datalake
