# Trino - MVP Deployment

## Назначение

Распределенный SQL query engine для аналитических запросов к данным в Data Lake.

**Конфигурация для больших данных**: Оптимизирован для работы с 2+ TB Parquet данных.

---

## Требования к машине

### Целевая конфигурация (для работы с 2+ TB данных)

| Параметр | Значение |
|----------|----------|
| CPU | 16 cores |
| RAM | 64 GB |
| Storage | 500 GB SSD (для spill disk) |
| Network | 10 Gbit/s |
| Hostname | trino.company.com |

### Минимальная конфигурация (ограниченные ресурсы)

| Параметр | Значение |
|----------|----------|
| CPU | 16 cores |
| RAM | 64 GB |
| Storage | **64 GB** (минимум) |
| Network | 10 Gbit/s |
| Hostname | trino.company.com |

⚠️ **Ограничения минимальной конфигурации**:
- Нельзя обрабатывать очень большие JOIN (нет места для spill)
- Запросы должны быть оптимизированы (использовать партиционирование, фильтры)
- Рекомендуется работа с датасетами до 500GB-1TB

📋 **См. раздел**: [Конфигурация для ограниченных ресурсов](#конфигурация-для-ограниченных-ресурсов-64gb-ram--64gb-disk)

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
│   ├── minio-root-ca.crt          # Корневой сертификат MinIO
│   └── catalog/
│       ├── iceberg.properties
│       └── hive.properties
├── .env.example
├── .gitlab-ci.yml
└── README.md
```

**Важно**: Файл `minio-root-ca.crt` должен содержать корневой сертификат вашего MinIO сервера в формате PEM.

---

## Dockerfile

`trino/Dockerfile`:

```dockerfile
FROM registry.company.com/trinodb/trino:435

USER root

# === УСТАНОВКА КОРНЕВОГО СЕРТИФИКАТА MINIO ===
# Копируем корневой сертификат MinIO
COPY config/minio-root-ca.crt /tmp/minio-root-ca.crt

# Добавляем сертификат в Java truststore
RUN JAVA_HOME=$(dirname $(dirname $(readlink -f $(command -v java)))) && \
    keytool -import -trustcacerts -noprompt \
      -alias minio-root-ca \
      -file /tmp/minio-root-ca.crt \
      -keystore $JAVA_HOME/lib/security/cacerts \
      -storepass changeit && \
    rm /tmp/minio-root-ca.crt

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

## Конфигурационные файлы (целевая конфигурация)

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

# === SSL ===
# SSL работает автоматически через корневой сертификат, добавленный в Java truststore (см. Dockerfile)
# Параметр s3.ssl.enabled не поддерживается при fs.native-s3.enabled=true

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

# === SSL ===
# SSL работает автоматически через корневой сертификат, добавленный в Java truststore (см. Dockerfile)
# Параметр s3.ssl.enabled не поддерживается при fs.native-s3.enabled=true

# Credentials через environment variables
```

**Важно**: S3 credentials нужно передать через environment variables в `.env` файле.

---

## Конфигурация для ограниченных ресурсов (64GB RAM / 64GB Disk)

Если у вас доступно только 64GB диска вместо 500GB, используйте следующие адаптированные конфигурации.

### config.properties (ограниченные ресурсы)

`config/config.properties`:

```properties
# === COORDINATOR (Single-node включает coordinator и worker) ===
coordinator=true
node-scheduler.include-coordinator=true

# === HTTP SERVER ===
http-server.http.port=8080

# === DISCOVERY ===
discovery.uri=http://localhost:8080

# === MEMORY (адаптировано под ограниченные ресурсы) ===
query.max-memory=24GB
query.max-memory-per-node=24GB
query.max-total-memory=32GB

# === SPILL TO DISK (ОТКЛЮЧЕН - недостаточно места) ===
# Для 64GB диска spill отключаем, чтобы избежать нехватки места
spill-enabled=false
# spiller-spill-path=/data/trino/spill
# spiller-max-used-space-threshold=0.8

# === PERFORMANCE (снижено для стабильности) ===
query.max-stage-count=100
query.max-execution-time=1h
task.concurrency=12

# === FAILSAFE (автоматическая остановка больших запросов) ===
query.max-scan-physical-bytes=500GB
```

⚠️ **Важно**:
- Spill to disk **отключен** из-за нехватки места
- Большие JOIN запросы могут падать с OOM
- Используйте фильтры по партициям для ограничения объема данных

### jvm.config (ограниченные ресурсы)

`config/jvm.config`:

```
-server
-Xmx40G
-Xms40G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/data/trino/heap_dump.hprof
-XX:+ExitOnOutOfMemoryError
-XX:ReservedCodeCacheSize=512M
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
```

**Изменения**:
- Heap снижен с 48GB до **40GB** (оставляем больше места для ОС и кэшей)

### node.properties (без изменений)

`config/node.properties`:

```properties
node.environment=production
node.id=trino-mvp-1
node.data-dir=/data/trino
```

### log.properties (без изменений)

`config/log.properties`:

```properties
io.trino=INFO
```

### Catalogs (без изменений)

Используйте те же `catalog/iceberg.properties` и `catalog/hive.properties` из основной конфигурации.

### Рекомендации по работе с ограниченными ресурсами

1. **Всегда используйте партиционирование**:
   ```sql
   -- ХОРОШО: фильтр по партиции
   SELECT * FROM iceberg.sales.orders
   WHERE dt BETWEEN '2024-12-01' AND '2024-12-31'

   -- ПЛОХО: полное сканирование таблицы
   SELECT * FROM iceberg.sales.orders
   ```

2. **Ограничивайте JOIN размеры**:
   ```sql
   -- Используйте WHERE перед JOIN
   SELECT a.*, b.name
   FROM large_table a
   JOIN small_table b ON a.id = b.id
   WHERE a.dt = '2024-12-25'  -- фильтр ПЕРЕД JOIN
   ```

3. **Используйте LIMIT для тестирования**:
   ```sql
   SELECT * FROM large_table LIMIT 1000
   ```

4. **Мониторинг памяти**:
   ```bash
   # Проверять использование RAM
   docker stats trino

   # Проверять использование диска
   docker exec trino df -h /data/trino
   ```

5. **Если нужен spill** - подключите внешний volume:
   ```bash
   # Пример: подключить внешний диск
   docker run -d \
     --name trino \
     -v /mnt/large-disk:/data/trino/spill \
     ...
   ```

   И включите в config.properties:
   ```properties
   spill-enabled=true
   spiller-spill-path=/data/trino/spill
   spiller-max-used-space-threshold=0.8
   ```

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

# 4. Создать директорию для данных с правильными правами
sudo mkdir -p /mnt/data/trino
sudo chown -R 1000:1000 /mnt/data/trino
sudo chmod -R 755 /mnt/data/trino

# 5. Запуск контейнера
docker run -d \
  --name trino \
  --restart unless-stopped \
  -p 8080:8080 \
  --env-file .env \
  -e "s3.aws-access-key=${S3_ACCESS_KEY}" \
  -e "s3.aws-secret-key=${S3_SECRET_KEY}" \
  -v /mnt/data/trino:/data/trino \
  trino-datalake:latest

# 6. Проверить логи
docker logs -f trino

# Дождаться сообщения:
# "======== SERVER STARTED ========"

# 7. Health check
curl -f http://trino.company.com:8080/v1/info
```

### Вариант 2: GitLab CI/CD

**ВАЖНО**: Перед деплоем Trino убедитесь, что Hive Metastore уже запущен и доступен.

`.gitlab-ci.yml`:

```yaml
variables:
  GIT_STRATEGY: clone

stages:
  - deploy

Deploy Trino to TEST:
  stage: deploy
  tags: [your_runner_tag]
  needs: []
  when: manual
  allow_failure: false
  before_script:
    - SRV_APP="trino.company.com"
    - HIVE_METASTORE="hive-metastore.company.com"
  script:
    - |
      # Создаем переменную с названием образа
      ImageName=trino-datalake:latest

      # Создаем переменную с названием контейнера
      ContainerName=trino

      # Создаем скрипт деплоя
      echo "set -e" > build.sh
      cat >> build.sh << DEPLOY_SCRIPT

      echo '==========================================================================================='
      echo 'Проверка зависимостей перед деплоем...'
      echo '==========================================================================================='

      # Проверить Hive Metastore
      echo 'Проверяем доступность Hive Metastore...'
      nc -zv ${HIVE_METASTORE} 9083 || \
        (echo 'ОШИБКА: Hive Metastore недоступен!' && exit 1)

      echo 'Все зависимости доступны. Продолжаем деплой...'
      echo '==========================================================================================='

      echo 'Собираем Docker образ...'
      docker build -t ${ImageName} .

      echo 'Останавливаем и удаляем старый контейнер...'
      docker stop ${ContainerName} && docker rm ${ContainerName} && echo 'Старый контейнер остановлен и удален.' || echo 'Старого контейнера нет, останавливать нечего.'

      echo 'Создаем директории для данных с правильными правами...'
      sudo mkdir -p /mnt/data/trino
      sudo chown -R 1000:1000 /mnt/data/trino
      sudo chmod -R 755 /mnt/data/trino

      echo 'Создаем новый контейнер...'
      docker run \
        -d \
        --name ${ContainerName} \
        --restart=always \
        -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
        -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
        -p 8080:8080 \
        -v /mnt/data/trino:/data/trino \
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
      echo 'Проверяем доступность HTTP API:'
      sleep 5
      curl -f http://localhost:8080/v1/info || echo 'ВНИМАНИЕ: HTTP API еще не доступен. Дождитесь полной инициализации.'
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
| `AWS_ACCESS_KEY_ID` | `trino` | Variable |
| `AWS_SECRET_ACCESS_KEY` | `<password_from_minio>` | Variable (Masked) |

**Примечание**:
- Замените `your_runner_tag` на тег вашего GitLab Runner
- Замените `svc_user` на пользователя для SSH подключения
- Скрипт автоматически проверяет доступность Hive Metastore перед деплоем
- Docker образ собирается локально на целевом сервере из скопированного проекта

**Настройка sudo для svc_user** (на сервере trino.company.com):

Пользователь `svc_user` должен иметь права на выполнение команд без пароля. Добавьте в `/etc/sudoers.d/svc_user`:

```bash
# На сервере trino.company.com (под root или sudo)
sudo visudo -f /etc/sudoers.d/svc_user

# Добавить:
svc_user ALL=(ALL) NOPASSWD: /bin/mkdir, /bin/chown, /bin/chmod, /usr/bin/docker
```

Или альтернативный вариант - создайте директорию `/mnt/data/trino` вручную один раз с правильными правами:

```bash
# На сервере trino.company.com (выполнить один раз)
sudo mkdir -p /mnt/data/trino
sudo chown -R 1000:1000 /mnt/data/trino
sudo chmod -R 755 /mnt/data/trino
```

После этого CI/CD скрипт будет использовать уже существующую директорию с правильными правами

---

## Проверка работоспособности

### 1. Web UI

Открыть в браузере: http://trino.company.com:8080

Должна отобразиться Trino Web UI с информацией о кластере.

### 2. Trino CLI

```bash
# Установить Trino CLI

# Вариант 1: Скачать через Nexus напрямую
wget https://nexus.company.com/repository/maven-public/io/trino/trino-cli/435/trino-cli-435-executable.jar
chmod +x trino-cli-435-executable.jar
sudo mv trino-cli-435-executable.jar /usr/local/bin/trino

# Вариант 2: Использовать Maven (если maven-settings.xml настроен на Nexus)
# mvn dependency:copy \
#   -Dartifact=io.trino:trino-cli:435:jar:executable \
#   -DoutputDirectory=. \
#   -s /path/to/maven-settings.xml
# chmod +x trino-cli-435-executable.jar
# sudo mv trino-cli-435-executable.jar /usr/local/bin/trino

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
  location = 's3://datalake/test/sample_table/'
);

-- Вставить тестовые данные
INSERT INTO iceberg.test.sample_table VALUES
  (1, 'Test 1', DATE '2024-12-25'),
  (2, 'Test 2', DATE '2024-12-25');

-- Выбрать данные
SELECT * FROM iceberg.test.sample_table;
```

---

## Работа с данными в S3/MinIO

Этот раздел показывает, как создавать таблицы из существующих Parquet файлов в S3/MinIO и работать с партиционированными данными.

**⚠️ ВАЖНО: Выбор протокола S3**

В зависимости от конфигурации `fs.native-s3.enabled` используйте соответствующий протокол:

| Конфигурация | Протокол | Пример |
|-------------|----------|--------|
| `fs.native-s3.enabled=true` | **`s3://`** | `s3://datalake/topics/data/` |
| `fs.native-s3.enabled=false` | **`s3a://`** | `s3a://datalake/topics/data/` |

В этой инструкции используется **`s3://`** (нативный S3 клиент), так как `fs.native-s3.enabled=true`.

### Основные концепции

1. **External таблицы** - указывают на существующие данные в S3, не копируют их
2. **Партиционирование** - данные организованы в поддиректории типа `srcId=2025122201`
3. **Синхронизация партиций** - Trino нужно явно сообщить о новых партициях
4. **Фильтрация по партициям** - критично для производительности

### Создание схемы

```sql
-- Создать схему с указанием базового пути в S3
CREATE SCHEMA IF NOT EXISTS hive.datalake
WITH (location = 's3://datalake/');

-- Или без явного location (будет использован дефолтный путь)
CREATE SCHEMA IF NOT EXISTS hive.aigt;
```

---

### Пример 1: Таблица с тремя уровнями партиционирования

**Структура данных в S3:**
```
s3://datalake/topics/order-events/
├── calc_id=2025122201/
│   ├── dt=2025-12-22/
│   │   ├── hour=00/
│   │   │   └── data.parquet
│   │   ├── hour=01/
│   │   │   └── data.parquet
│   │   └── ...
│   └── dt=2025-12-23/
│       └── ...
└── calc_id=2025122301/
    └── ...
```

**SQL скрипт для создания таблицы:**

```sql
-- 1. Создать схему
CREATE SCHEMA IF NOT EXISTS hive.datalake
WITH (location = 's3://datalake/');

-- 2. Удалить таблицу (если нужно пересоздать)
DROP TABLE IF EXISTS hive.datalake.order_events;

-- 3. Создать external таблицу
CREATE TABLE hive.datalake.order_events (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_date VARCHAR,
    delivery_date DATE,
    status VARCHAR,
    total_amount DOUBLE,
    currency VARCHAR,
    item_count INTEGER,
    shipping_address VARCHAR,
    billing_address VARCHAR,
    shipping_zip VARCHAR,
    billing_zip VARCHAR,
    shipping_city VARCHAR,
    billing_city VARCHAR,
    shipping_country VARCHAR,
    billing_country VARCHAR,
    payment_method VARCHAR,
    card_last_digits VARCHAR,
    card_expiry VARCHAR,
    ip_address VARCHAR,
    user_agent VARCHAR,
    campaign_id VARCHAR,
    referrer_url VARCHAR,
    device_type VARCHAR,
    browser VARCHAR,
    os VARCHAR,
    coupon_code VARCHAR,
    discount_amount DOUBLE,
    loyalty_points_used INTEGER,
    gift_wrap BOOLEAN,
    special_instructions VARCHAR,
    calc_id VARCHAR,      -- ПАРТИЦИЯ 1
    dt VARCHAR,           -- ПАРТИЦИЯ 2
    hour VARCHAR          -- ПАРТИЦИЯ 3
) WITH (
    external_location = 's3://datalake/topics/order-events',
    format = 'PARQUET',
    partitioned_by = ARRAY['calc_id', 'dt', 'hour']
);

-- 4. Синхронизировать все партиции (FULL = первый раз, найти все)
CALL hive.system.sync_partition_metadata('datalake', 'order_events', 'FULL');

-- 5. Проверить количество записей
SELECT COUNT(*) FROM hive.datalake.order_events;

-- 6. Проверить распределение по партициям
SELECT calc_id, dt, hour, COUNT(*) as row_count
FROM hive.datalake.order_events
GROUP BY calc_id, dt, hour
ORDER BY calc_id, dt, hour;

-- 7. Посмотреть метаданные партиций
SELECT * FROM hive.datalake."order_events$partitions";

-- 8. Посмотреть пути к файлам
SELECT
    "$path" as file_path,
    COUNT(*) as row_count
FROM hive.datalake.order_events
GROUP BY "$path"
ORDER BY "$path"
LIMIT 20;

-- 9. Запрос с фильтрацией (ВСЕГДА используйте фильтр по партициям!)
SELECT *
FROM hive.datalake.order_events
WHERE calc_id = '2025122201'  -- Обязательный фильтр
  AND dt = '2025-12-22'       -- Обязательный фильтр
  AND order_id = 'ORD-7460';
```

---

### Пример 2: Таблица с одной партицией (srcId)

**Структура данных в S3:**
```
s3://datalake/topics/orderPlacementResult/
├── srcId=2025122201/
│   └── *.parquet
├── srcId=2025122301/
│   └── *.parquet
└── srcId=2025122401/
    └── *.parquet
```

**SQL скрипт для создания таблицы:**

```sql
-- 1. Создать схему
CREATE SCHEMA IF NOT EXISTS hive.aigt;

-- 2. Удалить таблицу (если существует)
DROP TABLE IF EXISTS hive.aigt.tOrderForecastCalcResultTemp;

-- 3. Создать external таблицу
CREATE TABLE hive.aigt.tOrderForecastCalcResultTemp (
    rsltId SMALLINT,
    skuId INT,
    spId INT,
    cntrId INT,
    orderDt DATE,
    incomeDt DATE,
    calcAttribute SMALLINT,
    totalOrderPcs DECIMAL(10, 3),
    orderPcs DECIMAL(10, 3),
    baseSaleForecastPcs DECIMAL(10, 5),
    mcSaleForecastMpoPcs DECIMAL(10, 5),
    mcSaleForecastCoeffPcs DECIMAL(10, 5),
    spPerc DECIMAL(10, 3),
    avgSalePcs DECIMAL(10, 3),
    writeoutOrderPcs DECIMAL(10, 3),
    frcstRestPcs DECIMAL(10, 3),
    expectedArrivalPcs DECIMAL(10, 3),
    tslWriteoffPcs DECIMAL(10, 3),
    salePlanPcs DECIMAL(10, 3),
    frcstMcStockPcs DECIMAL(10, 3),
    mcStockPcs DECIMAL(10, 3),
    isMcStockRecalc BOOLEAN,          -- bit -> boolean
    positiveHolidayGrowthPcs DECIMAL(10, 3),
    negativeHolidayGrowthPcs DECIMAL(10, 3),
    positiveMcGrowthPcs DECIMAL(10, 3),
    positiveMcGrpGrowthPcs DECIMAL(10, 3),
    stockPcs DECIMAL(10, 3),
    stockDays DECIMAL(10, 3),
    minStockPcs DECIMAL(10, 3),
    defStockPcs DECIMAL(10, 3),
    calcStockPcs DECIMAL(10, 3),
    totalStockPcs DECIMAL(10, 3),
    orderCoef DECIMAL(10, 3),
    holidayStockPcs DECIMAL(10, 3),
    seasonStockPcs DECIMAL(10, 3),
    addStockPcs DECIMAL(10, 3),
    frcstRestDayPcs DECIMAL(10, 3),
    overallSalePcs DECIMAL(10, 3),
    arithmeticSalePcs DECIMAL(10, 3),
    deviationPcs DECIMAL(10, 3),
    discretDays INT,
    resShelfLifeDays INT,
    firstOrderPcs DECIMAL(10, 3),
    currentRestPcs DECIMAL(10, 3),
    minPartPcs DECIMAL(10, 3),
    isStockDayVolatility BOOLEAN,     -- bit -> boolean
    isStockPcsPss BOOLEAN,            -- bit -> boolean
    analogSkuIdList VARCHAR,          -- varchar(8000) -> varchar
    mainNeedPcs DECIMAL(10, 3),
    totalOrderOperationPcs DECIMAL(10, 3),
    limTotalStockCoef DECIMAL(10, 3),
    roundOrderCoef DECIMAL(10, 3),
    isRestoredSale BOOLEAN,           -- bit -> boolean
    correctWdcPcs DECIMAL(10, 3),
    sumWdcForecastRestPcs DECIMAL(10, 3),
    isRecalcOrderSale BOOLEAN,        -- bit -> boolean
    trustPercList INT,
    conceptPromoLogId BIGINT,
    prtNum TINYINT,
    srcId INT                         -- ПАРТИЦИЯ
) WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['srcId'],
    external_location = 's3://datalake/topics/orderPlacementResult/'
);

-- 4. Синхронизировать партиции
CALL hive.system.sync_partition_metadata('aigt', 'tOrderForecastCalcResultTemp', 'FULL');

-- 5. Проверить партиции
SELECT * FROM hive.aigt."tOrderForecastCalcResultTemp$partitions";

-- 6. Посмотреть доступные srcId
SELECT DISTINCT srcId
FROM hive.aigt."tOrderForecastCalcResultTemp$partitions"
ORDER BY srcId DESC;

-- 7. Запрос за конкретный день (ОБЯЗАТЕЛЬНО указывайте srcId!)
SELECT
    skuId,
    spId,
    cntrId,
    SUM(totalOrderPcs) as total_order,
    AVG(stockDays) as avg_stock_days
FROM hive.aigt.tOrderForecastCalcResultTemp
WHERE srcId = 2025122201  -- ⚠️ ОБЯЗАТЕЛЬНО! Иначе сканируются все дни
  AND orderDt >= DATE '2025-12-01'
GROUP BY skuId, spId, cntrId
ORDER BY total_order DESC
LIMIT 100;

-- 8. Запрос за последние 7 дней
SELECT
    srcId,
    COUNT(*) as record_count,
    SUM(totalOrderPcs) as total_orders
FROM hive.aigt.tOrderForecastCalcResultTemp
WHERE srcId >= 2025121601 AND srcId <= 2025122201  -- Фильтр по партициям
GROUP BY srcId
ORDER BY srcId DESC;
```

---

### Работа с партициями: Best Practices

#### 1. Ежедневная синхронизация новых партиций

После загрузки новых данных в S3 каждый день:

```sql
-- Вариант 1: Автоматически добавить новые партиции (рекомендуется)
CALL hive.system.sync_partition_metadata('aigt', 'tOrderForecastCalcResultTemp', 'ADD');

-- Вариант 2: Добавить конкретную партицию вручную
ALTER TABLE hive.aigt.tOrderForecastCalcResultTemp
ADD IF NOT EXISTS PARTITION (srcId = 2025122301);

-- Вариант 3: Полная пересинхронизация (медленно, для исправления проблем)
CALL hive.system.sync_partition_metadata('aigt', 'tOrderForecastCalcResultTemp', 'FULL');
```

**Автоматизация через cron:**

```bash
# /etc/cron.d/trino-sync-partitions
# Запускать каждый день в 06:00 (после загрузки данных в 05:00)
0 6 * * * trino_user /usr/local/bin/trino --server http://trino.company.com:8080 --execute "CALL hive.system.sync_partition_metadata('aigt', 'tOrderForecastCalcResultTemp', 'ADD');"
```

#### 2. ВСЕГДА используйте фильтр по партициям

```sql
-- ✅ ПРАВИЛЬНО: фильтр по партиции
SELECT * FROM hive.aigt.tOrderForecastCalcResultTemp
WHERE srcId = 2025122201;

-- ✅ ПРАВИЛЬНО: диапазон партиций
SELECT * FROM hive.aigt.tOrderForecastCalcResultTemp
WHERE srcId >= 2025122001 AND srcId <= 2025122201;

-- ❌ ПЛОХО: без фильтра = сканирует ВСЕ партиции (очень медленно!)
SELECT * FROM hive.aigt.tOrderForecastCalcResultTemp;

-- ❌ ПЛОХО: фильтр по не-партиционной колонке
SELECT * FROM hive.aigt.tOrderForecastCalcResultTemp
WHERE orderDt = DATE '2025-12-22';  -- srcId тоже нужен!
```

#### 3. Проверка партиций

```sql
-- Посмотреть все доступные партиции
SELECT * FROM hive.aigt."tOrderForecastCalcResultTemp$partitions"
ORDER BY srcId DESC;

-- Подсчитать записи в каждой партиции
SELECT srcId, COUNT(*) as row_count
FROM hive.aigt.tOrderForecastCalcResultTemp
GROUP BY srcId
ORDER BY srcId DESC;

-- Проверить размер партиций на диске
SELECT
    srcId,
    COUNT(DISTINCT "$path") as file_count,
    COUNT(*) as row_count
FROM hive.aigt.tOrderForecastCalcResultTemp
GROUP BY srcId
ORDER BY srcId DESC;
```

#### 4. Удаление старых партиций

```sql
-- Удалить партицию из метаданных Trino
ALTER TABLE hive.aigt.tOrderForecastCalcResultTemp
DROP IF EXISTS PARTITION (srcId = 2025112201);

-- ⚠️ ВНИМАНИЕ: это НЕ удаляет файлы из S3!
-- Файлы нужно удалить через MinIO Console или AWS CLI:
-- aws s3 rm s3://datalake/topics/orderPlacementResult/srcId=2025112201/ --recursive
```

#### 5. Исправление проблем с партициями

```sql
-- Если партиции не видны - полная пересинхронизация
CALL hive.system.sync_partition_metadata('aigt', 'tOrderForecastCalcResultTemp', 'FULL');

-- Проверить Hive Metastore напрямую
SHOW PARTITIONS hive.aigt.tOrderForecastCalcResultTemp;

-- Проверить количество партиций в метаданных vs фактически
SELECT COUNT(*) as metadata_partition_count
FROM hive.aigt."tOrderForecastCalcResultTemp$partitions";
```

---

### Полезные запросы для анализа

```sql
-- 1. Посмотреть структуру таблицы
DESCRIBE hive.aigt.tOrderForecastCalcResultTemp;

-- 2. Посмотреть пути к Parquet файлам
SELECT DISTINCT "$path"
FROM hive.aigt.tOrderForecastCalcResultTemp
WHERE srcId = 2025122201
LIMIT 10;

-- 3. Проверить, что данные читаются корректно
SELECT
    srcId,
    COUNT(*) as total_rows,
    COUNT(DISTINCT skuId) as unique_skus,
    MIN(orderDt) as min_date,
    MAX(orderDt) as max_date,
    SUM(totalOrderPcs) as total_orders
FROM hive.aigt.tOrderForecastCalcResultTemp
WHERE srcId = 2025122201
GROUP BY srcId;

-- 4. Найти конкретную запись
SELECT *
FROM hive.aigt.tOrderForecastCalcResultTemp
WHERE srcId = 2025122201
  AND skuId = 123456
  AND spId = 789
LIMIT 10;

-- 5. Сравнить данные за разные дни
SELECT
    srcId,
    skuId,
    SUM(totalOrderPcs) as total_orders
FROM hive.aigt.tOrderForecastCalcResultTemp
WHERE srcId IN (2025122101, 2025122201)
GROUP BY srcId, skuId
ORDER BY skuId, srcId;

-- 6. Посмотреть статистику по файлам
SELECT
    "$path" as file_path,
    COUNT(*) as row_count,
    SUM(totalOrderPcs) as total_orders
FROM hive.aigt.tOrderForecastCalcResultTemp
WHERE srcId = 2025122201
GROUP BY "$path"
ORDER BY row_count DESC
LIMIT 20;
```

---

### Важные замечания

1. **Типы данных:**
   - SQL Server `bit` → Trino `BOOLEAN`
   - SQL Server `varchar(N)` → Trino `VARCHAR` (без указания длины)
   - SQL Server `decimal(p,s)` → Trino `DECIMAL(p,s)` (одинаково)

2. **Партиции добавляются ВРУЧНУЮ:**
   - Trino не обнаруживает новые партиции автоматически
   - После загрузки данных в S3 нужно вызвать `sync_partition_metadata`

3. **Производительность:**
   - Без фильтра по партициям запрос сканирует ВСЕ данные
   - Для таблиц с сотнями партиций это = десятки TB данных
   - Всегда указывайте `WHERE srcId = ...`

4. **Структура партиций в S3:**
   - Формат: `key=value` (например, `srcId=2025122201`)
   - Это называется "Hive partitioning"
   - Trino автоматически распознает такую структуру

5. **Ежедневная работа:**
   - Данные загружаются в S3: `srcId=2025122301`
   - Добавляем партицию: `CALL sync_partition_metadata(..., 'ADD')`
   - Запрашиваем данные: `WHERE srcId = 2025122301`

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

### Permission denied: '/data/trino/var'

**Проблема**: При запуске контейнера в логах появляется:
```
ERROR: [Errno 13] Permission denied: '/data/trino/var'
```

**Причина**: Директория `/mnt/data/trino` на хосте принадлежит root, а Trino внутри контейнера работает от пользователя `trino` (UID 1000).

**Решение**:
```bash
# Остановить контейнер
docker stop trino && docker rm trino

# Установить правильные права
sudo chown -R 1000:1000 /mnt/data/trino
sudo chmod -R 755 /mnt/data/trino

# Запустить контейнер снова
docker run -d \
  --name trino \
  --restart unless-stopped \
  -p 8080:8080 \
  --env-file .env \
  -e "s3.aws-access-key=${S3_ACCESS_KEY}" \
  -e "s3.aws-secret-key=${S3_SECRET_KEY}" \
  -v /mnt/data/trino:/data/trino \
  trino-datalake:latest

# Проверить логи
docker logs -f trino
```

**Проверка UID пользователя trino** (если 1000 не работает):
```bash
docker run --rm trino-datalake:latest id trino
# Использовать полученные UID:GID в команде chown
```

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

### Configuration property was not used

**Проблема**: При запуске Trino появляется ошибка:
```
ERROR: Configuration property 's3.ssl.enabled' was not used
```

**Причина**: Параметр `s3.ssl.enabled` не поддерживается в Trino 435 при использовании нативного S3 клиента (`fs.native-s3.enabled=true`).

**Решение**: Удалите строку `s3.ssl.enabled=true` из файлов `config/catalog/iceberg.properties` и `config/catalog/hive.properties`. SSL будет работать автоматически на основе схемы endpoint (https://) и сертификатов в Java truststore.

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

### SSL Certificate Error при создании таблиц

**Проблема**: При создании external таблицы с `external_location = 's3://...'` появляется ошибка:
```
External location is not a valid file system URI: s3://datalake/topics/...
Caused by: PKIX path building failed: unable to find valid certification path to requested target
```

**Причина**: MinIO использует самоподписанный SSL сертификат, который не доверен JVM внутри контейнера Trino.

**Решение (РЕКОМЕНДУЕТСЯ): Добавить корневой сертификат MinIO в truststore**

1. Получите корневой сертификат MinIO в формате .crt или .pem
2. Скопируйте его в директорию `trino/config/minio-root-ca.crt`
3. Dockerfile уже содержит секцию для установки сертификата (см. раздел Dockerfile выше)
4. Пересоберите образ:

```bash
# Убедитесь что сертификат на месте
ls -la config/minio-root-ca.crt

# Пересобрать образ
docker build -t trino-datalake:latest .

# Перезапустить контейнер
docker stop trino && docker rm trino
docker run -d --name trino ... trino-datalake:latest

# Проверить логи
docker logs -f trino
```

**Проверка установки сертификата**:

```bash
# Войти в контейнер
docker exec -it trino bash

# Проверить что сертификат добавлен
keytool -list -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit | grep minio-root-ca

# Должно вывести:
# minio-root-ca, <date>, trustedCertEntry,

# Проверить подключение к MinIO
curl -v https://minio.company.com:9000/minio/health/live
# Не должно быть ошибок SSL
```

**Альтернативное решение (НЕ РЕКОМЕНДУЕТСЯ для production)**:

Использовать HTTP вместо HTTPS - измените endpoint в `config/catalog/*.properties`:
```properties
s3.endpoint=http://minio.company.com:9000
```

**Примечание**: Параметр `s3.ssl.enabled` не поддерживается в Trino 435 при использовании нативного S3 клиента (`fs.native-s3.enabled=true`). SSL работает автоматически на основе схемы endpoint (https://) и доверенных сертификатов в Java truststore.

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
