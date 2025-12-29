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
├── postgresql.conf          # Конфигурация PostgreSQL
├── pg_hba.conf             # Правила аутентификации
├── .env.example            # Шаблон переменных окружения
├── .gitlab-ci.yml          # CI/CD pipeline
├── Dockerfile (optional)   # Опционально, для кастомизации
└── README.md               # Документация
```

**Примечание**: Dockerfile опционален. Для MVP используется стандартный образ `postgres:15-alpine` с монтированием конфигов.

---

## Dockerfile (Опционально)

**ВАЖНО**: Для MVP рекомендуется использовать образ `registry.company.com/postgres:15-alpine` без кастомного Dockerfile. Конфигурационные файлы монтируются через volumes при запуске контейнера.

Если требуется кастомный образ, используйте следующий `Dockerfile`:

```dockerfile
FROM registry.company.com/postgres:15-alpine

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB -h localhost || exit 1

# НЕ переопределяем CMD - используем стандартный entrypoint
# Конфигурация передается через параметры запуска
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
local   all             postgres                                peer
local   all             all                                     md5
host    all             all             127.0.0.1/32            scram-sha-256

# Подключение для Hive Metastore
host    metastore_db    hive            0.0.0.0/0               scram-sha-256

# Подключение для администратора с любой машины
host    all             postgres        0.0.0.0/0               scram-sha-256

# ВАЖНО: Для продакшена замените 0.0.0.0/0 на конкретные подсети!
# Примеры:
# host    metastore_db    hive            10.0.0.0/8              scram-sha-256
# host    all             postgres        192.168.1.0/24          scram-sha-256
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

### Вариант 1: Вручную (рекомендуется для MVP)

```bash
# На машине postgres-metastore.company.com

# 1. Создать директории для конфигурации
mkdir -p /opt/postgres-metastore/config
mkdir -p /mnt/data/postgres

# 2. Создать .env файл
cat > /opt/postgres-metastore/.env <<EOF
POSTGRES_DB=metastore_db
POSTGRES_USER=hive
POSTGRES_PASSWORD=<SECURE_PASSWORD>
EOF

# 3. Создать postgresql.conf
cat > /opt/postgres-metastore/config/postgresql.conf <<'EOF'
listen_addresses = '*'
port = 5432
max_connections = 200

# Memory (для 16 GB RAM)
shared_buffers = 4GB
effective_cache_size = 12GB
work_mem = 20MB
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
EOF

# 4. Создать pg_hba.conf
cat > /opt/postgres-metastore/config/pg_hba.conf <<'EOF'
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             postgres                                peer
local   all             all                                     md5
host    all             all             127.0.0.1/32            scram-sha-256

# Подключение для Hive Metastore
host    metastore_db    hive            0.0.0.0/0               scram-sha-256

# Подключение для администратора с любой машины
host    all             postgres        0.0.0.0/0               scram-sha-256
EOF

# 5. Запуск контейнера (без кастомного Dockerfile)
docker run -d \
  --name postgres-metastore \
  --restart unless-stopped \
  -p 5432:5432 \
  --env-file /opt/postgres-metastore/.env \
  -v /mnt/data/postgres:/var/lib/postgresql/data \
  -v /opt/postgres-metastore/config/postgresql.conf:/etc/postgresql/postgresql.conf:ro \
  -v /opt/postgres-metastore/config/pg_hba.conf:/etc/postgresql/pg_hba.conf:ro \
  postgres:15-alpine \
  -c config_file=/etc/postgresql/postgresql.conf \
  -c hba_file=/etc/postgresql/pg_hba.conf

# 6. Проверить логи
docker logs -f postgres-metastore

# 7. Health check
docker exec postgres-metastore pg_isready -U hive -d metastore_db
```

### Вариант 2: GitLab CI/CD

**Для MVP**: используйте стандартный образ postgres:15-alpine. CI/CD будет разворачивать конфигурационные файлы.

`.gitlab-ci.yml`:

```yaml
variables:
  GIT_STRATEGY: clone

stages:
  - deploy

Deploy PostgreSQL Metastore to TEST:
  stage: deploy
  tags: [your_runner_tag]  # Укажите тег вашего GitLab Runner
  needs: []
  when: manual
  allow_failure: false
  before_script:
    - SRV_APP="postgres-metastore.company.com"  # Целевой сервер
  script:
    - |
      # Создаем переменную с названием образа
      ImageName=postgres:15-alpine

      # Создаем переменную с названием контейнера
      ContainerName=postgres-metastore

      # Создаем скрипт деплоя
      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo 'Останавливаем и удаляем старый контейнер...'
      docker stop ${ContainerName} && docker rm ${ContainerName} && echo 'Старый контейнер остановлен и удален.' || echo 'Старого контейнера нет, останавливать нечего.'

      echo 'Создаем директории для данных и конфигурации...'
      mkdir -p /mnt/data/postgres
      mkdir -p /opt/postgres-metastore/config

      echo 'Создаем новый контейнер...'
      docker run \
        -d \
        --name ${ContainerName} \
        --restart=always \
        -e POSTGRES_DB=${POSTGRES_DB} \
        -e POSTGRES_USER=${POSTGRES_USER} \
        -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
        -p 5432:5432 \
        -v /mnt/data/postgres:/var/lib/postgresql/data \
        -v /opt/postgres-metastore/config/postgresql.conf:/etc/postgresql/postgresql.conf:ro \
        -v /opt/postgres-metastore/config/pg_hba.conf:/etc/postgresql/pg_hba.conf:ro \
        -h ${SRV_APP} \
        ${ImageName} \
        -c config_file=/etc/postgresql/postgresql.conf \
        -c hba_file=/etc/postgresql/pg_hba.conf

      echo "=========================================================================================="
      echo 'ГОТОВО!'
      echo "=========================================================================================="
      echo 'Проверяем состояние контейнера:'
      sleep 10
      docker ps -a --filter name=${ContainerName}
      echo '------------------------------------------------------------------------------------------'
      echo 'Логи контейнера:'
      docker logs ${ContainerName}
      echo '------------------------------------------------------------------------------------------'
      echo 'Проверяем готовность PostgreSQL:'
      docker exec ${ContainerName} pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}
      echo '------------------------------------------------------------------------------------------'

      DEPLOY_SCRIPT

      echo "Копируем конфигурационные файлы и скрипт на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "mkdir -p ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"

      # Копируем конфигурационные файлы
      rsync -avz ./postgresql.conf svc_user@${SRV_APP}:/opt/postgres-metastore/config/
      rsync -avz ./pg_hba.conf svc_user@${SRV_APP}:/opt/postgres-metastore/config/
      rsync -avz ./build.sh svc_user@${SRV_APP}:~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/

      echo "Запускаем скрипт деплоя на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "cd ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/ && \
        chmod u+x ./build.sh && ./build.sh"

      echo "Удаляем временные файлы с ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"
```

**Настройка переменных окружения в GitLab**:

В настройках CI/CD вашего проекта GitLab (`Settings > CI/CD > Variables`) добавьте:

| Переменная | Значение | Тип |
|-----------|----------|-----|
| `POSTGRES_DB` | `metastore_db` | Variable |
| `POSTGRES_USER` | `hive` | Variable |
| `POSTGRES_PASSWORD` | `<secure_password>` | Variable (Masked) |

**Примечание**: Замените `your_runner_tag` на тег вашего GitLab Runner и `svc_user` на пользователя для SSH подключения.

---

## Инициализация базы данных для Hive Metastore

После запуска PostgreSQL необходимо убедиться, что база данных и пользователь созданы правильно.

### Автоматическая инициализация

База данных `metastore_db` и пользователь `hive` создаются автоматически при первом запуске контейнера через переменные окружения из `.env` файла.

### Проверка инициализации

```bash
# Проверить что база данных создана
PGPASSWORD='<SECURE_PASSWORD>' psql -h postgres-metastore.company.com -U hive -d metastore_db -c "\l"

# Проверить что пользователь имеет права
PGPASSWORD='<SECURE_PASSWORD>' psql -h postgres-metastore.company.com -U hive -d metastore_db -c "SELECT current_user, current_database();"
```

### Ручная инициализация (если требуется)

Если база данных не была создана автоматически:

```bash
# Подключиться как postgres пользователь
docker exec -it postgres-metastore psql -U postgres

# Создать базу данных и пользователя
CREATE DATABASE metastore_db;
CREATE USER hive WITH PASSWORD '<SECURE_PASSWORD>';
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;

# Выйти
\q

# Проверить подключение
PGPASSWORD='<SECURE_PASSWORD>' psql -h postgres-metastore.company.com -U hive -d metastore_db -c "SELECT 1;"
```

### Схема базы данных

Схема Hive Metastore будет создана автоматически при первом запуске Hive Metastore сервиса. PostgreSQL должен быть запущен и доступен ДО запуска Hive Metastore.

**Важно**: Не создавайте схему вручную - Hive Metastore сделает это автоматически при первом подключении.

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
