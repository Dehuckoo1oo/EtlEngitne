# Hive Metastore - MVP Deployment

## Назначение

Сервис метаданных для Data Lake (Thrift API). Использует PostgreSQL и MinIO.
MVP: один инстанс, без HA и балансировщиков.

---

## Требования к машине

| Параметр | Значение |
|----------|----------|
| CPU | 4 cores |
| RAM | 8 GB |
| Storage | 50 GB SSD |
| Network | 1 Gbit/s |
| Hostname | hive-metastore.company.com |

---

## GitLab структура

```
hive-metastore/
├── Dockerfile
├── config/
│   └── core-site.xml
├── .env.example
├── .gitlab-ci.yml
└── README.md
```

---

## Dockerfile

`hive-metastore/Dockerfile`:

```dockerfile
FROM apache/hive:4.0.0

USER root

# Установить JDBC и S3 библиотеки
RUN apt-get update && \
    apt-get install -y wget netcat-openbsd && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -O /opt/hive/lib/postgresql-jdbc.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar && \
    ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar /opt/hive/lib/hadoop-aws-3.3.4.jar && \
    ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar && \
    rm -rf /var/lib/apt/lists/*

# Конфиг S3
COPY config/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

USER hive

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD nc -z localhost 9083 || exit 1

EXPOSE 9083

ENTRYPOINT ["/entrypoint.sh"]
```

---

## Конфигурационные файлы

### core-site.xml

`config/core-site.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>https://minio.company.com:9000</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.connection.maximum</name>
    <value>200</value>
  </property>
  <property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>com.amazonaws.auth.EnvironmentVariableCredentialsProvider</value>
  </property>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
</configuration>
```

---

## Environment Variables

`.env.example`:

```bash
# === MinIO/S3 ===
AWS_ACCESS_KEY_ID=hive-metastore
AWS_SECRET_ACCESS_KEY=<PASSWORD_FROM_MINIO>

# === Hive Metastore opts ===
SERVICE_OPTS=-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres-metastore.company.com:5432/metastore_db -Djavax.jdo.option.ConnectionUserName=hive -Djavax.jdo.option.ConnectionPassword=<SECURE_PASSWORD> -Xms4g -Xmx4g
```

---

## Зависимости

**ВАЖНО**: Hive Metastore зависит от следующих сервисов и должен быть запущен ПОСЛЕ их успешного развертывания:

1. **PostgreSQL Metastore** - база данных для хранения метаданных
   - Должен быть доступен по адресу `postgres-metastore.company.com:5432`
   - База данных `metastore_db` должна быть создана
   - Пользователь `hive` должен иметь права на запись
   - Health check: `pg_isready -h postgres-metastore.company.com -U hive`

2. **MinIO** - объектное хранилище для Data Lake
   - Должен быть доступен по адресу `minio.company.com:9000`
   - Bucket `datalake` должен быть создан
   - Пользователь `hive-metastore` должен быть создан с правами на чтение/запись
   - Health check: `curl -f https://minio.company.com:9000/minio/health/live`

### Проверка готовности зависимостей

Перед запуском Hive Metastore выполните:

```bash
# Проверить PostgreSQL
pg_isready -h postgres-metastore.company.com -U hive
# Ожидаемый результат: postgres-metastore.company.com:5432 - accepting connections

# Проверить MinIO
curl -f https://minio.company.com:9000/minio/health/live
# Ожидаемый результат: HTTP 200 OK

# Проверить доступность базы данных
PGPASSWORD='<SECURE_PASSWORD>' psql -h postgres-metastore.company.com -U hive -d metastore_db -c "SELECT 1;"
# Ожидаемый результат: 1 строка
```

---

## Build & Deploy

### Вариант 1: Вручную

```bash
# На машине hive-metastore.company.com

# 1. Клонировать репозиторий
git clone https://gitlab.company.com/datalake/infrastructure.git
cd infrastructure/hive-metastore

# 2. Build образа
docker build -t hive-metastore:latest .

# 3. Создать .env
cp .env.example .env
nano .env  # Установить пароли и SERVICE_OPTS

# 4. Запуск контейнера
docker run -d \
  --name hive-metastore \
  --restart unless-stopped \
  -p 9083:9083 \
  --env-file .env \
  hive-metastore:latest

# 5. Health check
nc -zv hive-metastore.company.com 9083
```

### Вариант 2: GitLab CI/CD

**ВАЖНО**: Перед деплоем Hive Metastore убедитесь, что PostgreSQL и MinIO уже запущены и доступны.

`.gitlab-ci.yml`:

```yaml
variables:
  GIT_STRATEGY: clone

stages:
  - build
  - deploy

Build Hive Metastore Image:
  stage: build
  tags: [your_runner_tag]
  script:
    - docker build -t hive-metastore:${CI_COMMIT_SHORT_SHA} .
    - docker tag hive-metastore:${CI_COMMIT_SHORT_SHA} hive-metastore:latest
    # Опционально: push в registry
    # - docker tag hive-metastore:latest registry.company.com/hive-metastore:latest
    # - docker push registry.company.com/hive-metastore:latest
  only:
    - main

Deploy Hive Metastore to TEST:
  stage: deploy
  tags: [your_runner_tag]
  needs: [Build Hive Metastore Image]
  when: manual
  allow_failure: false
  before_script:
    - SRV_APP="hive-metastore.company.com"
    - POSTGRES_HOST="postgres-metastore.company.com"
    - MINIO_HOST="minio.company.com"
  script:
    - |
      # Создаем переменную с названием образа
      ImageName=hive-metastore:latest

      # Создаем переменную с названием контейнера
      ContainerName=hive-metastore

      # Создаем скрипт деплоя
      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo '==========================================================================================='
      echo 'Проверка зависимостей перед деплоем...'
      echo '==========================================================================================='

      # Проверить PostgreSQL
      echo 'Проверяем доступность PostgreSQL...'
      docker run --rm postgres:15-alpine pg_isready -h ${POSTGRES_HOST} -U hive || \
        (echo 'ОШИБКА: PostgreSQL недоступен!' && exit 1)

      # Проверить MinIO
      echo 'Проверяем доступность MinIO...'
      curl -f https://${MINIO_HOST}:9000/minio/health/live || \
        (echo 'ОШИБКА: MinIO недоступен!' && exit 1)

      echo 'Все зависимости доступны. Продолжаем деплой...'
      echo '==========================================================================================='

      echo 'Останавливаем и удаляем старый контейнер...'
      docker stop ${ContainerName} && docker rm ${ContainerName} && echo 'Старый контейнер остановлен и удален.' || echo 'Старого контейнера нет, останавливать нечего.'

      echo 'Создаем новый контейнер...'
      docker run \
        -d \
        --name ${ContainerName} \
        --restart=always \
        -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
        -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
        -e SERVICE_OPTS="${SERVICE_OPTS}" \
        -p 9083:9083 \
        -h ${SRV_APP} \
        ${ImageName}

      echo "=========================================================================================="
      echo 'ГОТОВО!'
      echo "=========================================================================================="
      echo 'Проверяем состояние контейнера:'
      sleep 15
      docker ps -a --filter name=${ContainerName}
      echo '------------------------------------------------------------------------------------------'
      echo 'Логи контейнера:'
      docker logs ${ContainerName}
      echo '------------------------------------------------------------------------------------------'
      echo 'Проверяем доступность Thrift порта:'
      nc -zv localhost 9083 || echo 'ВНИМАНИЕ: Порт 9083 еще не доступен. Дождитесь полной инициализации.'
      echo '------------------------------------------------------------------------------------------'

      DEPLOY_SCRIPT

      echo "Копируем конфигурационные файлы и образ на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"
      rsync -avz ./ svc_user@${SRV_APP}:~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}

      # Копируем Docker образ на целевой сервер
      echo "Экспортируем Docker образ..."
      docker save hive-metastore:latest | gzip > hive-metastore-latest.tar.gz

      echo "Копируем образ на ${SRV_APP}..."
      rsync -avz ./hive-metastore-latest.tar.gz svc_user@${SRV_APP}:~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/

      echo "Загружаем образ на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "cd ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}/ && \
        docker load < hive-metastore-latest.tar.gz"

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
| `AWS_ACCESS_KEY_ID` | `hive-metastore` | Variable |
| `AWS_SECRET_ACCESS_KEY` | `<password_from_minio>` | Variable (Masked) |
| `SERVICE_OPTS` | `-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres-metastore.company.com:5432/metastore_db -Djavax.jdo.option.ConnectionUserName=hive -Djavax.jdo.option.ConnectionPassword=<SECURE_PASSWORD> -Xms4g -Xmx4g` | Variable (Masked) |

**Примечание**:
- Замените `your_runner_tag` на тег вашего GitLab Runner
- Замените `svc_user` на пользователя для SSH подключения
- Скрипт автоматически проверяет доступность PostgreSQL и MinIO перед деплоем
- Docker образ копируется на целевой сервер для изоляции от registry

---

## Health Check

```bash
nc -zv hive-metastore.company.com 9083
docker inspect hive-metastore | grep -A 5 Health
```

---

## Следующий шаг

После запуска Hive Metastore переходите к:
👉 [Kafka Connect](kafka-connect.md)
