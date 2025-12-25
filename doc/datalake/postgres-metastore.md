# PostgreSQL Metastore - MVP Deployment

## Назначение

База данных для хранения метаданных Hive Metastore (таблицы, партиции, схемы).
MVP: один инстанс, без репликации и backup.

---

## Требования к машине

| Параметр | Значение |
|----------|----------|
| CPU | 4 cores |
| RAM | 16 GB |
| Storage | 100 GB SSD |
| Network | 1 Gbit/s |
| Hostname | postgres-metastore.company.com |

---

## GitLab структура

```
postgres-metastore/
├── Dockerfile
├── postgresql.conf
├── pg_hba.conf
├── .env.example
├── .gitlab-ci.yml
└── README.md
```

---

## Dockerfile

`postgres-metastore/Dockerfile`:

```dockerfile
FROM postgres:15-alpine

COPY postgresql.conf /etc/postgresql/postgresql.conf
COPY pg_hba.conf /etc/postgresql/pg_hba.conf

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB -h localhost || exit 1

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf", "-c", "hba_file=/etc/postgresql/pg_hba.conf"]
```

---

## Конфигурационные файлы

### postgresql.conf

`postgresql.conf`:

```ini
listen_addresses = '*'
port = 5432
max_connections = 200

# Memory (для 2+ TB)
shared_buffers = 8GB
effective_cache_size = 24GB
work_mem = 32MB
maintenance_work_mem = 1GB
wal_buffers = 16MB

# WAL
wal_level = replica
max_wal_size = 4GB
min_wal_size = 1GB

# Checkpoints
checkpoint_timeout = 10min
checkpoint_completion_target = 0.9

# Logging
log_min_duration_statement = 1000
log_timezone = 'UTC'
```

### pg_hba.conf

`pg_hba.conf`:

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     peer
host    all             all             127.0.0.1/32            scram-sha-256
host    metastore_db    hive            10.0.0.0/8              scram-sha-256
```

---

## Environment Variables

`.env.example`:

```bash
POSTGRES_DB=metastore_db
POSTGRES_USER=hive
POSTGRES_PASSWORD=<SECURE_PASSWORD>
```

---

## Build & Deploy

### Вариант 1: Вручную

```bash
# На машине postgres-metastore.company.com

# 1. Клонировать репозиторий
git clone https://gitlab.company.com/datalake/infrastructure.git
cd infrastructure/postgres-metastore

# 2. Build образа
docker build -t postgres-metastore:latest .

# 3. Создать .env
cp .env.example .env
nano .env  # Установить пароль

# 4. Запуск контейнера
docker run -d \
  --name postgres-metastore \
  --restart unless-stopped \
  -p 5432:5432 \
  --env-file .env \
  -v /mnt/data/postgres:/var/lib/postgresql/data \
  postgres-metastore:latest

# 5. Health check
pg_isready -h postgres-metastore.company.com -U hive
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
  TARGET_HOST: postgres-metastore.company.com

build:
  stage: build
  script:
    - docker build -t ${REGISTRY}/postgres-metastore:${IMAGE_TAG} .
    - docker tag ${REGISTRY}/postgres-metastore:${IMAGE_TAG} ${REGISTRY}/postgres-metastore:latest
    - docker push ${REGISTRY}/postgres-metastore:${IMAGE_TAG}
    - docker push ${REGISTRY}/postgres-metastore:latest
  only:
    - main

deploy:
  stage: deploy
  script:
    - ssh deploy@${TARGET_HOST} "docker pull ${REGISTRY}/postgres-metastore:latest"
    - ssh deploy@${TARGET_HOST} "docker stop postgres-metastore || true && docker rm postgres-metastore || true"
    - ssh deploy@${TARGET_HOST} "docker run -d --name postgres-metastore --restart unless-stopped -p 5432:5432 --env-file /opt/postgres-metastore/.env -v /mnt/data/postgres:/var/lib/postgresql/data ${REGISTRY}/postgres-metastore:latest"
  only:
    - main
  when: manual
```

---

## Health Check

```bash
pg_isready -h postgres-metastore.company.com -U hive
docker inspect postgres-metastore | grep -A 5 Health
```

---

## Следующий шаг

После запуска PostgreSQL переходите к:
👉 [Hive Metastore](hive-metastore.md)
