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
│   ├── core-site.xml
│   ├── hive-log4j2.properties  # Конфигурация логирования
│   ├── maven-settings.xml
│   └── minio-root-ca.crt       # Корневой сертификат MinIO
├── .env.example
├── .gitlab-ci.yml
└── README.md
```

**Важно**: Файл `minio-root-ca.crt` должен содержать корневой сертификат вашего MinIO сервера в формате PEM.

---

## Dockerfile

`hive-metastore/Dockerfile`:

```dockerfile
# Stage 1: Builder для скачивания зависимостей через Maven
FROM registry.company.com/maven:3.9-eclipse-temurin-11 AS builder

# Копировать maven-settings.xml с настройкой Nexus
COPY config/maven-settings.xml /root/.m2/settings.xml

# Отключить SSL проверку на уровне Maven и Java (ОБЯЗАТЕЛЬНО для корпоративных Nexus с самоподписанными сертификатами)
ENV MAVEN_OPTS="-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true"
ENV JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/etc/ssl/certs/java/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true"

# Скачать JDBC и S3 библиотеки через Nexus proxy
RUN mvn -U \
        -Dmaven.resolver.transport=wagon \
        -Dmaven.wagon.http.ssl.insecure=true \
        -Dmaven.wagon.http.ssl.allowall=true \
        -Dmaven.wagon.http.ssl.ignore.validity.dates=true \
        dependency:copy -Dartifact=org.postgresql:postgresql:42.7.1:jar -DoutputDirectory=/jars && \
    mvn -U \
        -Dmaven.resolver.transport=wagon \
        -Dmaven.wagon.http.ssl.insecure=true \
        -Dmaven.wagon.http.ssl.allowall=true \
        -Dmaven.wagon.http.ssl.ignore.validity.dates=true \
        dependency:copy -Dartifact=org.apache.hadoop:hadoop-aws:3.3.4:jar -DoutputDirectory=/jars && \
    mvn -U \
        -Dmaven.resolver.transport=wagon \
        -Dmaven.wagon.http.ssl.insecure=true \
        -Dmaven.wagon.http.ssl.allowall=true \
        -Dmaven.wagon.http.ssl.ignore.validity.dates=true \
        dependency:copy -Dartifact=com.amazonaws:aws-java-sdk-bundle:1.12.262:jar -DoutputDirectory=/jars

# Stage 2: Final образ
FROM registry.company.com/apache/hive:4.0.0

USER root

# === УСТАНОВКА КОРНЕВОГО СЕРТИФИКАТА MINIO ===
# Копируем корневой сертификат MinIO
COPY config/minio-root-ca.crt /tmp/minio-root-ca.crt

# Добавляем сертификат в Java truststore
RUN JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    keytool -import -trustcacerts -noprompt \
      -alias minio-root-ca \
      -file /tmp/minio-root-ca.crt \
      -keystore $JAVA_HOME/lib/security/cacerts \
      -storepass changeit && \
    rm /tmp/minio-root-ca.crt

# Установить только netcat для healthcheck
RUN apt-get update && \
    apt-get install -y netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Копировать JAR библиотеки из builder stage
COPY --from=builder /jars/postgresql-42.7.1.jar /opt/hive/lib/postgresql-jdbc.jar
COPY --from=builder /jars/hadoop-aws-3.3.4.jar /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar
COPY --from=builder /jars/aws-java-sdk-bundle-1.12.262.jar /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar

# Создать symlinks для hive
RUN ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.4.jar /opt/hive/lib/hadoop-aws-3.3.4.jar && \
    ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.262.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar

# Конфиг S3
COPY config/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

# Конфиг логирования
COPY config/hive-log4j2.properties /opt/hive/conf/hive-log4j2.properties

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
  <!-- После добавления корневого сертификата в truststore можно оставить SSL включенным -->
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

### maven-settings.xml

`config/maven-settings.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings>
  <mirrors>
    <mirror>
      <id>nexus</id>
      <mirrorOf>*</mirrorOf>
      <url>https://nexus.company.com/repository/maven-public</url>
    </mirror>
  </mirrors>
</settings>
```

**Примечание**: Замените `nexus.company.com` на реальный адрес вашего Nexus сервера.

### hive-log4j2.properties

`config/hive-log4j2.properties`:

```properties
# Root logger
status = INFO
name = HiveLog4j2
packages = org.apache.hadoop.hive.ql.log

# Console appender
appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{ISO8601} %5p [%t] %c{2}: %m%n

# Root logger configuration
rootLogger.level = INFO
rootLogger.appenderRefs = console
rootLogger.appenderRef.console.ref = console

# Hive Metastore
logger.metastore.name = org.apache.hadoop.hive.metastore
logger.metastore.level = INFO

# S3A FileSystem - ВАЖНО для диагностики проблем с S3/MinIO
logger.s3a.name = org.apache.hadoop.fs.s3a
logger.s3a.level = DEBUG

# AWS SDK - для детальной диагностики SSL и credentials
logger.aws.name = com.amazonaws
logger.aws.level = INFO

# Hadoop FS
logger.hadoop.name = org.apache.hadoop
logger.hadoop.level = INFO

# DataNucleus
logger.datanucleus.name = DataNucleus
logger.datanucleus.level = ERROR

# Thrift
logger.thrift.name = org.apache.thrift
logger.thrift.level = WARN
```

**Важно**:
- `logger.s3a.level = DEBUG` включает детальное логирование всех операций с S3/MinIO
- Это поможет увидеть SSL ошибки, проблемы с credentials, timeouts и другие проблемы подключения
- Для production можно вернуть на `INFO` после отладки

**Для временной отладки** можно установить еще более подробное логирование:
```properties
logger.s3a.level = TRACE
logger.aws.level = DEBUG
```

---

## Environment Variables

`.env.example`:

```bash
# === Service Type ===
SERVICE_NAME=metastore

# === MinIO/S3 ===
AWS_ACCESS_KEY_ID=hive-metastore
AWS_SECRET_ACCESS_KEY=<PASSWORD_FROM_MINIO>

# === Database ===
DB_DRIVER=postgres

# === Hive Metastore opts ===
# ВАЖНО: Значение должно быть в кавычках!
SERVICE_OPTS="-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres-metastore.company.com:5432/metastore_db -Djavax.jdo.option.ConnectionUserName=hive -Djavax.jdo.option.ConnectionPassword=<SECURE_PASSWORD> -Xms4g -Xmx4g"
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
  - deploy

Deploy Hive Metastore to TEST:
  stage: deploy
  tags: [your_runner_tag]
  needs: []
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

      echo 'Собираем Docker образ...'
      docker build -t ${ImageName} .

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
        -e SERVICE_NAME=metastore \
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

      echo "Копируем папку на ${SRV_APP}..."
      ssh svc_user@${SRV_APP} "rm -Rf ~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}"
      rsync -avz ./ svc_user@${SRV_APP}:~/docker_build_${CI_PROJECT_NAME}_${CI_COMMIT_SHORT_SHA}_${CI_JOB_ID}

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

**ВАЖНО для SERVICE_OPTS**: В GitLab переменных НЕ нужны внешние кавычки, но в `.env` файле они обязательны!

**Примечание**:
- Замените `your_runner_tag` на тег вашего GitLab Runner
- Замените `svc_user` на пользователя для SSH подключения
- Скрипт автоматически проверяет доступность PostgreSQL и MinIO перед деплоем
- Docker образ собирается локально на целевом сервере из скопированного проекта

---

## Health Check

```bash
nc -zv hive-metastore.company.com 9083
docker inspect hive-metastore | grep -A 5 Health
```

---

## Troubleshooting

### Контейнер постоянно перезапускается с "Initialized schema successfully.."

**Симптомы**:
```
Initialized schema successfully..
+ '[' '' == hiveserver2 ']'
+ '[' '' == metastore ']'
+ : postgres
+ SKIP_SCHEMA_INIT=false
...
Initialized schema successfully..
```

**Причина**: Не установлена переменная окружения `SERVICE_NAME=metastore`. Entrypoint скрипт инициализирует схему БД, но не запускает сам Metastore сервис. Контейнер завершается, Docker перезапускает его из-за `--restart=always`, и цикл повторяется.

**Решение**:
1. Добавьте `SERVICE_NAME=metastore` в ваш `.env` файл:
   ```bash
   SERVICE_NAME=metastore
   AWS_ACCESS_KEY_ID=hive-metastore
   AWS_SECRET_ACCESS_KEY=<PASSWORD>
   ...
   ```

2. Пересоздайте контейнер:
   ```bash
   docker stop hive-metastore && docker rm hive-metastore
   docker run -d \
     --name hive-metastore \
     --restart unless-stopped \
     -p 9083:9083 \
     --env-file .env \
     hive-metastore:latest
   ```

3. Проверьте логи - теперь должно быть:
   ```bash
   docker logs -f hive-metastore
   # Ожидается: Starting Hive Metastore Server
   ```

---

### Ошибка: "Could not find or load main class Djavax.jdo.option..."

**Симптомы**:
```
Error: Could not find or load main class Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
Schema initialization failed!
```

**Причина**: В `SERVICE_OPTS` отсутствует дефис `-` перед `D`. Это происходит, если значение не обернуто в кавычки в `.env` файле.

**Решение**:
1. Если используете `.env` файл - оберните значение в кавычки:
   ```bash
   SERVICE_OPTS="-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver ..."
   ```

2. Если используете GitLab CI/CD переменные - кавычки НЕ нужны (GitLab сам обрабатывает значения корректно)

3. Пересоздайте контейнер:
   ```bash
   docker stop hive-metastore && docker rm hive-metastore
   # Запустите снова с исправленным .env
   ```

### Ошибка: "Connection refused" к PostgreSQL

**Симптомы**: Hive не может подключиться к PostgreSQL

**Причина**: Неправильная конфигурация `pg_hba.conf` или PostgreSQL недоступен

**Решение**:
1. Проверьте, что PostgreSQL запущен:
   ```bash
   docker ps | grep postgres-metastore
   ```

2. Проверьте доступность с машины Hive:
   ```bash
   pg_isready -h postgres-metastore.company.com -U hive
   ```

3. Убедитесь, что в `pg_hba.conf` разрешен доступ с IP адреса Hive машины:
   ```
   host    metastore_db    hive            0.0.0.0/0               scram-sha-256
   ```

4. Перезапустите PostgreSQL после изменения `pg_hba.conf`:
   ```bash
   docker restart postgres-metastore
   ```

### База данных не создается автоматически

**Причина**: Переменные окружения PostgreSQL не были переданы при первом запуске контейнера

**Решение**:
1. Удалите данные PostgreSQL:
   ```bash
   docker stop postgres-metastore && docker rm postgres-metastore
   rm -rf /mnt/data/postgres/*
   ```

2. Запустите заново с правильными переменными:
   ```bash
   docker run -d \
     --name postgres-metastore \
     -e POSTGRES_DB=metastore_db \
     -e POSTGRES_USER=hive \
     -e POSTGRES_PASSWORD=<SECURE_PASSWORD> \
     ...
   ```

### Timeout при создании таблиц с location в S3

**Симптомы**:
```
SocketTimeoutException: Read timed out
Failed to create external path s3a://...
```

**Причина**: Hive Metastore не может подключиться к MinIO из-за проблем с SSL сертификатом или credentials

**Диагностика через логи**:

1. Убедитесь что `hive-log4j2.properties` скопирован в образ (см. раздел Dockerfile выше)

2. Пересоберите образ и перезапустите контейнер:
   ```bash
   docker build -t hive-metastore:latest .
   docker stop hive-metastore && docker rm hive-metastore
   docker run -d --name hive-metastore ... hive-metastore:latest
   ```

3. Запустите просмотр логов в реальном времени:
   ```bash
   docker logs hive-metastore -f
   ```

4. В другом терминале попробуйте создать таблицу через Trino

5. В логах должны появиться детальные сообщения от S3A:
   ```
   DEBUG o.a.hadoop.fs.s3a.S3AFileSystem: Opening 's3a://datalake/...'
   DEBUG o.a.hadoop.fs.s3a.S3AFileSystem: Endpoint: https://minio.company.com:9000
   DEBUG o.a.hadoop.fs.s3a.auth: Using credentials provider: EnvironmentVariableCredentialsProvider
   DEBUG com.amazonaws.request: Sending Request: ...
   ```

**Типичные ошибки в логах и решения**:

- **SSL certificate error**:
  ```
  javax.net.ssl.SSLHandshakeException: PKIX path building failed
  ```
  → Корневой сертификат MinIO не установлен. Проверьте что `minio-root-ca.crt` копируется в Dockerfile и добавляется в truststore.

- **Access Denied (403)**:
  ```
  Status Code: 403, AWS Service: Amazon S3
  ```
  → Проблема с credentials или правами. Проверьте `AWS_ACCESS_KEY_ID` и `AWS_SECRET_ACCESS_KEY`, убедитесь что у пользователя есть права на запись в bucket.

- **Connection timeout**:
  ```
  java.net.ConnectException: Connection timed out
  ```
  → MinIO недоступен или firewall блокирует. Проверьте доступность MinIO с сервера Hive Metastore.

**Решение SSL проблемы** (если сертификат установлен, но не работает):

Временно переключитесь на HTTP для тестирования:

```bash
# На сервере hive-metastore
docker exec -it hive-metastore bash
vi /opt/hadoop/etc/hadoop/core-site.xml

# Измените:
# https://minio.company.com:9000 → http://minio.company.com:9000
# fs.s3a.connection.ssl.enabled true → false

exit
docker restart hive-metastore
```

Если после этого заработает - проблема точно в SSL сертификате.

---

## Следующий шаг

После запуска Hive Metastore переходите к:
👉 [Kafka Connect](kafka-connect.md)
