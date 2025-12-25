# Полное руководство по развертыванию Data Lake на Production

## Содержание
1. [Обзор архитектуры](#обзор-архитектуры)
2. [Описание сервисов](#описание-сервисов)
3. [Детальная конфигурация каждого сервиса](#детальная-конфигурация-каждого-сервиса)
4. [Взаимодействие компонентов](#взаимодействие-компонентов)
5. [API и интерфейсы](#api-и-интерфейсы)
6. [Production конфигурация](#production-конфигурация)
7. [Мониторинг и безопасность](#мониторинг-и-безопасность)

---

## Обзор архитектуры

### Что мы строим?
Data Lake - это централизованное хранилище данных, которое позволяет хранить большие объемы данных в их исходном формате и выполнять аналитические запросы.

### Поток данных:
```
┌──────────────┐
│   MSSQL      │ Источник данных (ваша база данных)
└──────┬───────┘
       │
       ↓ CDC (Change Data Capture) - отслеживание изменений
┌──────────────┐
│   Kafka      │ Шина данных (транспортировка в реальном времени)
└──────┬───────┘
       │
       ↓ Kafka Connect читает сообщения
┌──────────────┐
│Kafka Connect │ Преобразует данные в Parquet и сохраняет
└──────┬───────┘
       │
       ↓ Запись в S3
┌──────────────┐
│    MinIO     │ Объектное хранилище (как Dropbox для файлов данных)
└──────┬───────┘
       │
       ↓ Метаданные о таблицах
┌──────────────┐
│Hive Metastore│ Каталог таблиц (знает что и где лежит)
└──────┬───────┘
       │
       ↓ Запросы SQL
┌──────────────┐
│    Trino     │ SQL движок (выполняет аналитические запросы)
└──────┬───────┘
       │
       ↓ Визуализация и анализ
┌──────────────┐
│   Jupyter    │ Ноутбуки для анализа данных
└──────────────┘
```

---

## Описание сервисов

### 1. MinIO - Объектное хранилище

**Что это?**
MinIO - это S3-совместимое объектное хранилище. Представьте его как файловое хранилище, где:
- Файлы хранятся в "бакетах" (папках)
- Каждый файл имеет уникальный путь
- Доступ через HTTP API

**Зачем нужен?**
- Хранит файлы данных в формате Parquet (оптимизированный колоночный формат)
- Дешевое хранение больших объемов данных
- Масштабируется горизонтально

**Аналогия:**
MinIO = Google Drive, но для аналитических данных

---

### 2. PostgreSQL Metastore - База данных для метаданных

**Что это?**
Обычная PostgreSQL база данных, которая хранит служебную информацию.

**Зачем нужна?**
Hive Metastore сохраняет в нее информацию о:
- Какие таблицы существуют
- Где лежат файлы для каждой таблицы
- Какая структура у каждой таблицы (колонки, типы)
- Какие партиции существуют

**Аналогия:**
PostgreSQL = каталожная картотека в библиотеке

---

### 3. Hive Metastore - Каталог таблиц

**Что это?**
Сервис, который отслеживает метаданные о таблицах в Data Lake.

**Зачем нужен?**
Без него Trino не знает:
- Какие таблицы существуют
- Где искать данные
- Как читать файлы

**Как работает?**
```
Пользователь: CREATE TABLE users (id INT, name VARCHAR)
     ↓
Hive Metastore:
  1. Сохраняет в PostgreSQL: "Таблица users с колонками id, name"
  2. Создает директорию в MinIO: s3://datalake/users/
  3. Запоминает связь: "users → s3://datalake/users/"
```

**API:**
- Thrift API на порту 9083 (бинарный протокол)
- Используется Trino для получения метаданных

---

### 4. Trino - SQL Query Engine

**Что это?**
Распределенный SQL движок для аналитики больших данных.

**Зачем нужен?**
Позволяет выполнять SQL запросы к данным в MinIO, как будто это обычная база данных.

**Как работает?**
```sql
SELECT * FROM iceberg.sales.orders WHERE date = '2025-12-23'
```

1. Trino обращается к Hive Metastore: "Где данные таблицы orders?"
2. Metastore отвечает: "s3://datalake/sales/orders/"
3. Trino читает Parquet файлы из MinIO
4. Фильтрует данные по дате
5. Возвращает результат

**API:**
- HTTP API на порту 8080
- Web UI для выполнения запросов
- JDBC драйвер для подключения из приложений

---

### 5. Kafka Connect - Коннектор данных

**Что это?**
Сервис для интеграции Kafka с внешними системами.

**Зачем нужен?**
Читает данные из Kafka топиков и:
1. Преобразует в формат Parquet
2. Группирует по времени (например, каждые 5 минут)
3. Сохраняет в MinIO

**Плагины:**
- **S3 Sink Connector** - записывает данные в S3/MinIO
- **JDBC Source Connector** - читает из баз данных

**API:**
- REST API на порту 8083
- Управление коннекторами через HTTP

---

### 6. Jupyter - Аналитические ноутбуки

**Что это?**
Интерактивная среда для анализа данных на Python.

**Зачем нужен?**
- Исследование данных
- Создание отчетов
- Прототипирование ML моделей

**Возможности:**
- Подключение к Trino через SQL
- Работа с данными через pandas
- Визуализация графиков

---

## Детальная конфигурация каждого сервиса

### 1. MinIO

#### Dockerfile (не требуется, используем готовый образ)

#### Docker Compose конфигурация:

```yaml
minio:
  image: minio/minio:latest          # Официальный образ MinIO
  hostname: minio                     # DNS имя в Docker сети
  ports:
    - "9000:9000"                     # API порт (S3 совместимый)
    - "9001:9001"                     # Web Console UI
  networks:
    - kafka                           # Подключаем к общей сети
  environment:
    # === УЧЕТНЫЕ ДАННЫЕ ===
    MINIO_ROOT_USER: minioadmin       # PRODUCTION: Замените на сложный логин!
    MINIO_ROOT_PASSWORD: minioadmin   # PRODUCTION: Замените на сложный пароль!

    # === ДОМЕН ===
    MINIO_DOMAIN: minio               # Виртуальный хост для bucket (bucket.minio)

    # === РЕГИОН ===
    MINIO_REGION_NAME: us-east-1      # AWS-совместимое название региона
                                       # Используется для совместимости с AWS SDK
                                       # Можно указать любой: eu-west-1, ru-central-1
  volumes:
    - minio-data:/data                # Персистентное хранилище данных
                                       # PRODUCTION: Используйте отдельный диск!
  command: server /data --console-address ":9001"
    # server /data              - запуск MinIO сервера с данными в /data
    # --console-address ":9001" - Web UI на порту 9001

  healthcheck:
    # Проверка здоровья сервиса
    test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
    interval: 30s    # Проверять каждые 30 секунд
    timeout: 10s     # Таймаут проверки
    retries: 3       # Количество неудачных попыток перед "unhealthy"
```

#### Production настройки для MinIO:

```yaml
# PRODUCTION конфигурация MinIO
minio:
  image: minio/minio:RELEASE.2024-01-01T00-00-00Z  # Используйте конкретную версию!
  hostname: minio.company.com
  ports:
    - "9000:9000"
    - "9001:9001"
  environment:
    # === БЕЗОПАСНОСТЬ ===
    MINIO_ROOT_USER: ${MINIO_ROOT_USER}              # Из .env файла
    MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}      # Из .env файла (минимум 16 символов)

    # === TLS/SSL ===
    MINIO_DOMAIN: minio.company.com
    MINIO_SERVER_URL: https://minio.company.com

    # === ПРОИЗВОДИТЕЛЬНОСТЬ ===
    MINIO_CACHE_DRIVES: "/mnt/cache"                 # SSD для кеширования
    MINIO_CACHE_QUOTA: 80                            # Использовать 80% кеш диска

    # === МОНИТОРИНГ ===
    MINIO_PROMETHEUS_AUTH_TYPE: public               # Метрики для Prometheus

  volumes:
    - /mnt/data1:/data1   # RAID массив для данных
    - /mnt/data2:/data2   # Для distributed режима
    - /mnt/cache:/mnt/cache

  # Для высокой доступности запускайте несколько экземпляров
  command: server http://minio{1...4}/data{1...4} --console-address ":9001"

  deploy:
    resources:
      limits:
        memory: 16G
        cpus: '4.0'
      reservations:
        memory: 8G
```

---

### 2. PostgreSQL Metastore

#### Dockerfile (не требуется, используем готовый образ)

#### Docker Compose конфигурация:

```yaml
postgres-metastore:
  image: postgres:15-alpine           # Alpine = минимальный размер образа
  hostname: postgres-metastore
  networks:
    - kafka
  environment:
    # === БАЗА ДАННЫХ ===
    POSTGRES_DB: metastore_db         # Имя базы данных
                                       # Создается автоматически при первом запуске

    # === ПОЛЬЗОВАТЕЛЬ ===
    POSTGRES_USER: hive               # Пользователь БД
                                       # PRODUCTION: Используйте другое имя!

    # === ПАРОЛЬ ===
    POSTGRES_PASSWORD: hivepassword   # Пароль для подключения
                                       # PRODUCTION: Сложный пароль из .env!
  volumes:
    - metastore-db:/var/lib/postgresql/data
      # /var/lib/postgresql/data - стандартный путь к данным PostgreSQL
      # PRODUCTION: Регулярное резервное копирование!

  healthcheck:
    # Проверка готовности БД
    test: ["CMD-SHELL", "pg_isready -U hive -d metastore_db"]
      # pg_isready - утилита PostgreSQL для проверки доступности
      # -U hive    - от имени пользователя hive
      # -d metastore_db - к базе metastore_db
    interval: 10s
    timeout: 5s
    retries: 5
```

#### Production настройки:

```yaml
# PRODUCTION конфигурация PostgreSQL
postgres-metastore:
  image: postgres:15-alpine
  environment:
    POSTGRES_DB: metastore_db
    POSTGRES_USER: ${POSTGRES_USER}
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}

    # === ПРОИЗВОДИТЕЛЬНОСТЬ ===
    POSTGRES_SHARED_BUFFERS: 256MB           # Кеш данных в памяти
    POSTGRES_EFFECTIVE_CACHE_SIZE: 1GB       # Доступная память для кеша
    POSTGRES_WORK_MEM: 16MB                  # Память для сортировок
    POSTGRES_MAINTENANCE_WORK_MEM: 128MB     # Память для VACUUM

    # === ЛОГИРОВАНИЕ ===
    POSTGRES_LOG_STATEMENT: mod              # Логировать DDL/DML
    POSTGRES_LOG_DURATION: on                # Время выполнения запросов

  volumes:
    - /mnt/postgres-data:/var/lib/postgresql/data
    - ./postgres-init:/docker-entrypoint-initdb.d  # SQL скрипты инициализации

  deploy:
    resources:
      limits:
        memory: 2G
        cpus: '2.0'
```

---

### 3. Hive Metastore

#### Dockerfile:

```dockerfile
# === БАЗОВЫЙ ОБРАЗ ===
FROM apache/hive:4.0.0
# apache/hive:4.0.0 - официальный образ Apache Hive
# Включает:
#   - Java Runtime (для запуска Hive)
#   - Hadoop libraries (для работы с HDFS/S3)
#   - Hive Metastore Service

# === ПЕРЕКЛЮЧЕНИЕ НА ROOT ===
USER root
# По умолчанию образ запускается от пользователя "hive"
# Нам нужен root для установки пакетов

# === УСТАНОВКА ЗАВИСИМОСТЕЙ ===
RUN apt-get update && \
    apt-get install -y wget && \
    # wget - утилита для скачивания файлов

    wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
         -O /opt/hive/lib/postgresql-jdbc.jar && \
    # Скачиваем PostgreSQL JDBC драйвер
    # ЗАЧЕМ? Hive Metastore подключается к PostgreSQL для хранения метаданных
    # JDBC = Java Database Connectivity - стандарт для подключения к БД
    # postgresql-42.7.1.jar - актуальная версия драйвера
    # -O /opt/hive/lib/ - сохраняем в библиотеку Hive

    rm -rf /var/lib/apt/lists/*
    # Удаляем apt кеш для уменьшения размера образа

# === ВОЗВРАТ К ПОЛЬЗОВАТЕЛЮ HIVE ===
USER hive
# Best practice: не запускать сервисы от root

# === ТОЧКА ВХОДА ===
ENTRYPOINT ["/entrypoint.sh"]
# /entrypoint.sh - скрипт из базового образа apache/hive
# Он:
#   1. Инициализирует схему БД (если нужно)
#   2. Запускает Hive Metastore Service
```

#### Docker Compose конфигурация:

```yaml
hive-metastore:
  build:
    context: ./data-lake/hive-metastore
    dockerfile: Dockerfile
  hostname: hive-metastore
  ports:
    - "9083:9083"                     # Thrift API порт
  networks:
    - kafka
  environment:
    # === ТИП СЕРВИСА ===
    SERVICE_NAME: metastore           # Какой компонент Hive запускать
                                       # Варианты: metastore, hiveserver2

    # === ДРАЙВЕР БД ===
    DB_DRIVER: postgres               # Тип базы данных (postgres, mysql, derby)

    # === ПОДКЛЮЧЕНИЕ К POSTGRESQL ===
    SERVICE_OPTS: >-
      -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
      # JDO (Java Data Objects) - стандарт для ORM
      # ConnectionDriverName - класс JDBC драйвера

      -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres-metastore:5432/metastore_db
      # JDBC URL формат: jdbc:postgresql://HOST:PORT/DATABASE
      # postgres-metastore:5432 - Docker DNS имя и порт PostgreSQL
      # metastore_db - имя базы данных

      -Djavax.jdo.option.ConnectionUserName=hive
      # Пользователь PostgreSQL

      -Djavax.jdo.option.ConnectionPassword=hivepassword
      # Пароль PostgreSQL
      # PRODUCTION: Из переменной окружения!

    # === ДОСТУП К S3 (MinIO) ===
    AWS_ACCESS_KEY_ID: minioadmin     # Ключ доступа к MinIO
    AWS_SECRET_ACCESS_KEY: minioadmin # Секретный ключ MinIO
    S3_ENDPOINT_URL: http://minio:9000 # URL MinIO API
    S3_PATH_STYLE_ACCESS: "true"      # Использовать path-style URLs
                                       # Format: http://minio:9000/bucket/key
                                       # Вместо: http://bucket.minio:9000/key

  depends_on:
    postgres-metastore:
      condition: service_healthy      # Ждем пока PostgreSQL запустится
    minio:
      condition: service_healthy      # Ждем пока MinIO запустится

  healthcheck:
    test: ["CMD", "nc", "-z", "localhost", "9083"]
      # nc (netcat) - проверка открытости порта
      # -z - zero I/O mode (только проверка)
      # localhost:9083 - Thrift API
    interval: 30s
    timeout: 10s
    retries: 5
    start_period: 60s                 # Даем 60 секунд на инициализацию
```

#### Production конфигурация:

```yaml
# PRODUCTION Hive Metastore
hive-metastore:
  build:
    context: ./data-lake/hive-metastore
  environment:
    SERVICE_NAME: metastore
    DB_DRIVER: postgres

    SERVICE_OPTS: >-
      -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
      -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://${POSTGRES_HOST}:5432/${POSTGRES_DB}
      -Djavax.jdo.option.ConnectionUserName=${POSTGRES_USER}
      -Djavax.jdo.option.ConnectionPassword=${POSTGRES_PASSWORD}

      # === ПУЛИНГ СОЕДИНЕНИЙ ===
      -Djavax.jdo.option.ConnectionPoolingType=HikariCP
      -Dhikaricp.maximumPoolSize=20
      -Dhikaricp.minimumIdle=5

      # === ПРОИЗВОДИТЕЛЬНОСТЬ ===
      -Xmx2g -Xms1g                    # Java heap: min 1GB, max 2GB
      -XX:+UseG1GC                     # Сборщик мусора G1

    # === AWS/S3 ===
    AWS_ACCESS_KEY_ID: ${S3_ACCESS_KEY}
    AWS_SECRET_ACCESS_KEY: ${S3_SECRET_KEY}
    S3_ENDPOINT_URL: ${S3_ENDPOINT}

  # === ВЫСОКАЯ ДОСТУПНОСТЬ ===
  deploy:
    replicas: 2                        # Два экземпляра для отказоустойчивости
    resources:
      limits:
        memory: 3G
        cpus: '2.0'
```

---

### 4. Trino

#### Dockerfile (не требуется, используем готовый образ)

#### Конфигурационные файлы:

##### config.properties:
```properties
# === РЕЖИМ РАБОТЫ ===
coordinator=true
# Этот узел является координатором (главным)
# Координатор:
#   - Принимает SQL запросы
#   - Планирует выполнение
#   - Координирует воркеров

node-scheduler.include-coordinator=true
# Координатор также выполняет задачи (для single-node режима)
# PRODUCTION: false (координатор только управляет, не выполняет)

# === HTTP СЕРВЕР ===
http-server.http.port=8080
# Порт для HTTP API и Web UI

# === DISCOVERY ===
discovery.uri=http://localhost:8080
# URI для service discovery
# Воркеры подключаются к координатору через этот URI
# PRODUCTION: http://trino-coordinator.company.com:8080

# === ПАМЯТЬ ===
query.max-memory=4GB
# Максимальная память для одного запроса на кластере
# Если запрос требует больше - будет ошибка

query.max-memory-per-node=2GB
# Максимальная память для запроса на одном узле
# Должно быть меньше или равно query.max-memory
```

##### jvm.config:
```
# === JVM ПАРАМЕТРЫ ===
-server
# Режим сервера (оптимизация для long-running процессов)

-Xmx8G
# Максимальный heap (основная память Java)
# PRODUCTION: 70-80% от RAM сервера

-Xms8G
# Начальный heap (равен максимальному для стабильности)

-XX:+UseG1GC
# Использовать G1 Garbage Collector
# Оптимален для больших heap и low latency

-XX:G1HeapRegionSize=32M
# Размер региона G1GC
# Для heap > 8GB рекомендуется 32M

-XX:+ExplicitGCInvokesConcurrent
# Явные вызовы GC не блокируют приложение

-XX:+HeapDumpOnOutOfMemoryError
# Создавать heap dump при OutOfMemoryError
# Для отладки проблем с памятью

-XX:+ExitOnOutOfMemoryError
# Завершить процесс при OOM
# Позволяет orchestrator (Docker) перезапустить контейнер

-XX:ReservedCodeCacheSize=512M
# Кеш для JIT-скомпилированного кода

-Djdk.attach.allowAttachSelf=true
# Разрешить JMX подключения

-Djdk.nio.maxCachedBufferSize=2000000
# Максимальный размер буфера NIO в кеше
```

##### Каталоги (catalogs):

**iceberg.properties:**
```properties
# === ТИП КОННЕКТОРА ===
connector.name=iceberg
# Тип: Apache Iceberg (современный table format)
# Поддерживает:
#   - ACID транзакции
#   - Time travel (запросы к историческим данным)
#   - Schema evolution
#   - Партиционирование

# === МЕТАСТОР ===
iceberg.catalog.type=hive_metastore
# Использовать Hive Metastore для хранения метаданных
# Альтернативы: rest, glue, jdbc

hive.metastore.uri=thrift://hive-metastore:9083
# Thrift API адрес Hive Metastore
# Format: thrift://HOST:PORT

# === ФОРМАТ ФАЙЛОВ ===
iceberg.file-format=PARQUET
# Формат данных на диске
# PARQUET - колоночный формат, оптимальный для аналитики
# Альтернативы: ORC, AVRO

iceberg.compression-codec=SNAPPY
# Алгоритм сжатия
# SNAPPY - быстрое сжатие/распаковка, средняя степень сжатия
# Альтернативы: GZIP (медленнее, лучше сжатие), ZSTD, LZ4

# === S3 КОНФИГУРАЦИЯ ===
fs.native-s3.enabled=true
# Использовать нативный S3 файловую систему (быстрее Hadoop S3A)

s3.endpoint=http://minio:9000
# URL MinIO API

s3.region=us-east-1
# AWS регион (для MinIO - заглушка)
# ОБЯЗАТЕЛЬНО для AWS SDK

s3.path-style-access=true
# Path-style URLs вместо virtual-hosted
# MinIO требует path-style: http://minio:9000/bucket/key

s3.aws-access-key=minioadmin
# Access Key для S3/MinIO

s3.aws-secret-key=minioadmin
# Secret Key для S3/MinIO
```

**hive.properties:**
```properties
# === ТИП КОННЕКТОРА ===
connector.name=hive
# Классический Hive connector
# Поддерживает:
#   - Hive таблицы
#   - Партиционирование
#   - Различные форматы: Parquet, ORC, Avro, Text

# === МЕТАСТОР ===
hive.metastore.uri=thrift://hive-metastore:9083

# === ПРАВА ЗАПИСИ ===
hive.non-managed-table-writes-enabled=true
# Разрешить запись в non-managed (external) таблицы
# Managed таблицы: Hive управляет данными (удаление таблицы = удаление данных)
# External таблицы: Данные независимы от таблицы

# === S3 ===
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
```

#### Docker Compose:

```yaml
trino:
  image: trinodb/trino:latest         # PRODUCTION: Конкретная версия!
  hostname: trino
  ports:
    - "8084:8080"                     # Web UI и API
  networks:
    - kafka
  volumes:
    # === КОНФИГУРАЦИЯ (read-only) ===
    - ./data-lake/trino/config.properties:/etc/trino/config.properties:ro
    - ./data-lake/trino/jvm.config:/etc/trino/jvm.config:ro
    - ./data-lake/trino/node.properties:/etc/trino/node.properties:ro
    - ./data-lake/trino/log.properties:/etc/trino/log.properties:ro
    - ./data-lake/trino/catalog:/etc/trino/catalog:ro
      # :ro = read-only (контейнер не может изменить файлы)

  depends_on:
    minio:
      condition: service_healthy
    hive-metastore:
      condition: service_healthy

  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8080/v1/info"]
      # /v1/info - REST endpoint с информацией о кластере
    interval: 30s
    timeout: 10s
    retries: 3
    start_period: 30s
```

---

### 5. Kafka Connect

#### Dockerfile:

```dockerfile
# === БАЗОВЫЙ ОБРАЗ ===
FROM confluentinc/cp-kafka-connect:7.5.0
# Confluent Kafka Connect с предустановленными:
#   - Java Runtime
#   - Kafka Connect framework
#   - Basic connectors

# === УСТАНОВКА S3 CONNECTOR ===
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.0
# confluent-hub - пакетный менеджер для Kafka Connect плагинов
# install - установить коннектор
# --no-prompt - без интерактивного подтверждения
# confluentinc/kafka-connect-s3:10.5.0 - S3 Sink Connector
#   ЗАЧЕМ? Записывает данные из Kafka в S3/MinIO
#   Функции:
#     - Группировка сообщений
#     - Конвертация в Parquet/Avro/JSON
#     - Партиционирование по времени/полям

# === ПЕРЕКЛЮЧЕНИЕ НА ROOT ===
USER root
RUN mkdir -p /data && chown appuser:appuser /data
# Создаем директорию для данных
# chown - меняем владельца на appuser (пользователь Kafka Connect)

# === ВОЗВРАТ К APPUSER ===
USER appuser
```

#### Docker Compose:

```yaml
kafka-connect:
  build:
    context: ./data-lake/kafka-connect
    dockerfile: Dockerfile
  hostname: kafka-connect
  ports:
    - "8083:8083"                     # REST API
  networks:
    - kafka
  environment:
    # === KAFKA ПОДКЛЮЧЕНИЕ ===
    CONNECT_BOOTSTRAP_SERVERS: broker-i1:19092,broker-i2:19092,broker-i3:19092
    # Список Kafka брокеров
    # Format: host1:port1,host2:port2

    # === REST API ===
    CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
    # Имя хоста для REST API
    # Клиенты будут подключаться к этому адресу

    CONNECT_REST_PORT: 8083
    # Порт REST API

    # === ГРУППА CONNECT ===
    CONNECT_GROUP_ID: kafka-connect-cluster
    # ID группы для распределения задач
    # Воркеры с одинаковым group.id образуют кластер

    # === ТОПИКИ ДЛЯ КОНФИГУРАЦИИ ===
    CONNECT_CONFIG_STORAGE_TOPIC: _connect-configs
    # Топик для хранения конфигураций коннекторов
    # Компактный топик (хранит последнее состояние)

    CONNECT_OFFSET_STORAGE_TOPIC: _connect-offsets
    # Топик для offset'ов (прогресс чтения)
    # Позволяет продолжить с места остановки

    CONNECT_STATUS_STORAGE_TOPIC: _connect-status
    # Топик для статусов задач

    # === РЕПЛИКАЦИЯ ТОПИКОВ ===
    CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 3
    CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 3
    CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 3
    # Replication factor = количество копий
    # 3 = данные хранятся на 3 брокерах
    # PRODUCTION: минимум 3 для отказоустойчивости

    # === КОНВЕРТОРЫ ДАННЫХ ===
    CONNECT_KEY_CONVERTER: io.confluent.connect.avro.AvroConverter
    # Формат для ключей сообщений
    # Avro - бинарный формат со схемой

    CONNECT_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
    # Формат для значений сообщений

    CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
    CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
    # Schema Registry - хранилище Avro схем
    # Позволяет эволюционировать схемы без breaking changes

    # === ВНУТРЕННИЕ КОНВЕРТОРЫ ===
    CONNECT_INTERNAL_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
    CONNECT_INTERNAL_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
    # Для служебных топиков (_connect-*) используется JSON

    CONNECT_INTERNAL_KEY_CONVERTER_SCHEMAS_ENABLE: "false"
    CONNECT_INTERNAL_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
    # Без JSON Schema для простоты

    # === ПЛАГИНЫ ===
    CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components
    # Директории где искать JAR файлы коннекторов

    # === ЛОГИРОВАНИЕ ===
    CONNECT_LOG4J_ROOT_LOGLEVEL: INFO
    CONNECT_LOG4J_LOGGERS: org.apache.kafka.connect.runtime.rest=WARN,org.reflections=ERROR
    # Уровни логов для разных компонентов

    # === ПРОИЗВОДИТЕЛЬНОСТЬ ===
    CONNECT_PRODUCER_COMPRESSION_TYPE: snappy
    # Сжатие при отправке в Kafka (для sink коннекторов)

    CONNECT_PRODUCER_MAX_REQUEST_SIZE: 10485760
    # Максимальный размер batch (10MB)

    CONNECT_CONSUMER_MAX_POLL_RECORDS: 5000
    # Количество записей за один poll

    CONNECT_CONSUMER_MAX_PARTITION_FETCH_BYTES: 10485760
    # Максимальный размер данных от одной партиции (10MB)

    # === JVM ПАМЯТЬ ===
    KAFKA_HEAP_OPTS: "-Xms4g -Xmx4g"
    # Heap: 4GB
    # PRODUCTION: 8-16GB для больших нагрузок

  volumes:
    - kafka-connect-data:/data        # Персистентные данные

  depends_on:
    - broker-i1
    - broker-i2
    - broker-i3
    - schema-registry
    - minio

  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8083/"]
    interval: 30s
    timeout: 10s
    retries: 5
    start_period: 60s                 # Connect медленно стартует
```

---

### 6. Jupyter

#### Dockerfile:

```dockerfile
# === БАЗОВЫЙ ОБРАЗ ===
FROM jupyter/scipy-notebook:latest
# jupyter/scipy-notebook включает:
#   - JupyterLab
#   - Python научные библиотеки: NumPy, Pandas, Matplotlib
#   - Scipy, scikit-learn
#   - Пользователь jovyan

# === УСТАНОВКА СИСТЕМНЫХ ПАКЕТОВ ===
USER root
RUN apt-get update && apt-get install -y \
    curl \
    # curl - для HTTP запросов (проверка healthcheck)
    && rm -rf /var/lib/apt/lists/*
    # Очистка apt cache

# === ВОЗВРАТ К JOVYAN ===
USER jovyan

# === УСТАНОВКА PYTHON ПАКЕТОВ ===
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# --no-cache-dir - не сохранять pip cache (меньше образ)

# === РАБОЧАЯ ДИРЕКТОРИЯ ===
RUN mkdir -p /home/jovyan/work
WORKDIR /home/jovyan/work
```

##### requirements.txt:
```txt
# === РАБОТА С S3 ===
boto3==1.34.0
# AWS SDK для Python
# Используется для прямого доступа к MinIO

s3fs==2024.1.0
# Файловая система S3 для pandas
# Позволяет: pd.read_parquet('s3://bucket/file.parquet')

# === TRINO ПОДКЛЮЧЕНИЕ ===
trino==0.328.0
# Python клиент для Trino
# Использование: from trino.dbapi import connect

sqlalchemy==2.0.25
# SQL toolkit и ORM
# Для Trino через SQLAlchemy

sqlalchemy-trino==0.5.0
# SQLAlchemy dialect для Trino

# === РАБОТА С ДАННЫМИ ===
pyarrow==14.0.2
# Библиотека для Parquet и Arrow форматов
# Критично для чтения Parquet

fastparquet==2024.2.0
# Альтернативная библиотека для Parquet

# === ВИЗУАЛИЗАЦИЯ ===
plotly==5.18.0
# Интерактивные графики

seaborn==0.13.1
# Статистические визуализации

# === УТИЛИТЫ ===
python-dotenv==1.0.0
# Загрузка переменных из .env файлов
```

#### Docker Compose:

```yaml
jupyter:
  build:
    context: ./data-lake/jupyter
    dockerfile: Dockerfile
  hostname: jupyter
  ports:
    - "8888:8888"                     # JupyterLab UI
  networks:
    - kafka
  environment:
    # === JUPYTER НАСТРОЙКИ ===
    JUPYTER_ENABLE_LAB: "yes"         # Использовать JupyterLab (не Notebook)

    JUPYTER_TOKEN: "datalake"         # Токен для входа
    # PRODUCTION: Сложный токен или пароль!

    # === AWS/S3 CREDENTIALS ===
    AWS_ACCESS_KEY_ID: minioadmin
    AWS_SECRET_ACCESS_KEY: minioadmin
    AWS_ENDPOINT_URL: http://minio:9000
    AWS_REGION: us-east-1
    # Для boto3 и s3fs

  volumes:
    - ./data-lake/jupyter/notebooks:/home/jovyan/work
      # Синхронизация ноутбуков с хостом
      # Позволяет редактировать локально

  depends_on:
    - minio
```

---

## Взаимодействие компонентов

### Сценарий 1: Запись данных из Kafka в Data Lake

```
┌─────────────────────────────────────────────────────────────┐
│ 1. ИЗМЕНЕНИЕ В MSSQL                                        │
│    INSERT INTO users VALUES (1, 'John', '2025-12-23')      │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ↓ Debezium CDC отслеживает изменения
┌────────────────────────────────────────────────────────────┐
│ 2. СООБЩЕНИЕ В KAFKA                                        │
│    Topic: mssql.dbo.users                                   │
│    {                                                         │
│      "id": 1,                                               │
│      "name": "John",                                        │
│      "created_date": "2025-12-23",                          │
│      "op": "c" (create)                                     │
│    }                                                         │
└────────────────┬───────────────────────────────────────────┘
                 │
                 ↓ Kafka Connect S3 Sink читает топик
┌────────────────────────────────────────────────────────────┐
│ 3. KAFKA CONNECT ОБРАБОТКА                                  │
│    - Группирует сообщения (batch)                          │
│    - Конвертирует в Parquet                                │
│    - Партиционирует по дате                                │
│    - Сжимает (SNAPPY)                                       │
└────────────────┬───────────────────────────────────────────┘
                 │
                 ↓ Запись через S3 API
┌────────────────────────────────────────────────────────────┐
│ 4. MINIO ХРАНЕНИЕ                                           │
│    s3://datalake/users/                                     │
│      year=2025/                                             │
│        month=12/                                            │
│          day=23/                                            │
│            part-00001.parquet                               │
│            part-00002.parquet                               │
└────────────────┬───────────────────────────────────────────┘
                 │
                 ↓ Обновление метаданных
┌────────────────────────────────────────────────────────────┐
│ 5. HIVE METASTORE                                           │
│    Сохраняет в PostgreSQL:                                  │
│    - Таблица: users                                         │
│    - Локация: s3://datalake/users/                         │
│    - Партиции: year=2025/month=12/day=23                   │
│    - Формат: PARQUET                                        │
│    - Схема: id BIGINT, name VARCHAR, created_date DATE     │
└─────────────────────────────────────────────────────────────┘
```

### Сценарий 2: Чтение данных через Trino

```
┌─────────────────────────────────────────────────────────────┐
│ 1. ПОЛЬЗОВАТЕЛЬ ВЫПОЛНЯЕТ SQL                               │
│    SELECT * FROM iceberg.sales.users                        │
│    WHERE created_date = DATE '2025-12-23'                   │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ↓ Запрос к метастору
┌────────────────────────────────────────────────────────────┐
│ 2. TRINO → HIVE METASTORE                                   │
│    "Где данные для таблицы sales.users?"                   │
│                                                              │
│    METASTORE ОТВЕЧАЕТ:                                      │
│    {                                                         │
│      "location": "s3://datalake/users/",                   │
│      "format": "PARQUET",                                   │
│      "partitions": [                                        │
│        "year=2025/month=12/day=23",                         │
│        "year=2025/month=12/day=22"                          │
│      ],                                                      │
│      "schema": {...}                                        │
│    }                                                         │
└────────────────┬───────────────────────────────────────────┘
                 │
                 ↓ Планирование запроса
┌────────────────────────────────────────────────────────────┐
│ 3. TRINO QUERY PLANNER                                      │
│    - Partition pruning (только year=2025/month=12/day=23)  │
│    - Predicate pushdown (фильтр уйдет в Parquet reader)    │
│    - Column pruning (читаем только нужные колонки)         │
└────────────────┬───────────────────────────────────────────┘
                 │
                 ↓ Чтение файлов
┌────────────────────────────────────────────────────────────┐
│ 4. TRINO → MINIO                                            │
│    GET s3://datalake/users/year=2025/month=12/day=23/      │
│                                                              │
│    MINIO ОТДАЕТ:                                            │
│    - part-00001.parquet (10 MB)                             │
│    - part-00002.parquet (8 MB)                              │
└────────────────┬───────────────────────────────────────────┘
                 │
                 ↓ Обработка данных
┌────────────────────────────────────────────────────────────┐
│ 5. TRINO ОБРАБОТКА                                          │
│    - Распаковка SNAPPY                                      │
│    - Чтение Parquet (только нужные колонки)                │
│    - Применение фильтров                                    │
│    - Агрегация (если нужно)                                │
│    - Возврат результата                                     │
└────────────────┬───────────────────────────────────────────┘
                 │
                 ↓
┌────────────────────────────────────────────────────────────┐
│ 6. РЕЗУЛЬТАТ                                                │
│    id │ name  │ created_date                                │
│    ───┼───────┼─────────────                                │
│    1  │ John  │ 2025-12-23                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## API и интерфейсы

### 1. MinIO

#### REST API (S3-совместимый)

**Базовый URL:** `http://minio:9000`

**Создание bucket:**
```bash
# Через AWS CLI
aws --endpoint-url http://minio:9000 \
    s3 mb s3://datalake

# Через MinIO Client (mc)
mc alias set myminio http://minio:9000 minioadmin minioadmin
mc mb myminio/datalake
```

**Загрузка файла:**
```bash
aws --endpoint-url http://minio:9000 \
    s3 cp myfile.parquet s3://datalake/data/
```

**Листинг файлов:**
```bash
aws --endpoint-url http://minio:9000 \
    s3 ls s3://datalake/
```

**Python (boto3):**
```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

# Создать bucket
s3.create_bucket(Bucket='datalake')

# Загрузить файл
s3.upload_file('local.parquet', 'datalake', 'data/file.parquet')

# Скачать файл
s3.download_file('datalake', 'data/file.parquet', 'local.parquet')

# Листинг
response = s3.list_objects_v2(Bucket='datalake', Prefix='data/')
for obj in response['Contents']:
    print(obj['Key'])
```

#### Web Console

**URL:** `http://localhost:9001`

**Функции:**
- Управление buckets
- Загрузка/скачивание файлов
- Управление пользователями
- Мониторинг (метрики, логи)
- Настройка политик доступа

---

### 2. Hive Metastore

#### Thrift API (порт 9083)

Используется через клиентские библиотеки (не HTTP!).

**Python (через PyHive):**
```python
from pyhive import hive

# Подключение
conn = hive.Connection(host='hive-metastore', port=9083)
cursor = conn.cursor()

# Получить список баз данных
cursor.execute('SHOW DATABASES')
for db in cursor.fetchall():
    print(db)

# Получить таблицы
cursor.execute('SHOW TABLES IN mydb')
for table in cursor.fetchall():
    print(table)

# Получить схему таблицы
cursor.execute('DESCRIBE mydb.mytable')
for col in cursor.fetchall():
    print(col)
```

**Прямой доступ через Hive CLI:**
```bash
docker exec -it etl-engine-hive-metastore-1 \
    /opt/hive/bin/beeline \
    -u jdbc:hive2://localhost:9083
```

---

### 3. Trino

#### REST API (порт 8080)

**Базовый URL:** `http://trino:8080`

**Выполнение запроса через curl:**
```bash
curl -X POST http://localhost:8084/v1/statement \
  -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: iceberg" \
  -H "X-Trino-Schema: default" \
  -d "SELECT * FROM users LIMIT 10"
```

**Python (trino-python-client):**
```python
from trino.dbapi import connect

conn = connect(
    host='localhost',
    port=8084,
    user='admin',
    catalog='iceberg',
    schema='default'
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM users LIMIT 10")
rows = cursor.fetchall()

for row in rows:
    print(row)
```

**SQLAlchemy:**
```python
from sqlalchemy import create_engine

engine = create_engine('trino://admin@localhost:8084/iceberg/default')

import pandas as pd
df = pd.read_sql("SELECT * FROM users", engine)
print(df)
```

#### Web UI

**URL:** `http://localhost:8084`

**Функции:**
- Выполнение SQL запросов
- Мониторинг выполнения (query plan, статистика)
- История запросов
- Просмотр каталогов и таблиц

**Использование:**
1. Откройте `http://localhost:8084`
2. Введите SQL в редакторе
3. Нажмите "Run"
4. Результаты отобразятся в таблице

---

### 4. Kafka Connect

#### REST API (порт 8083)

**Базовый URL:** `http://kafka-connect:8083`

**Список коннекторов:**
```bash
curl http://localhost:8083/connectors
```

**Создание S3 Sink коннектора:**
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "s3-sink-users",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max": "3",
      "topics": "mssql.dbo.users",

      "s3.bucket.name": "datalake",
      "s3.region": "us-east-1",
      "s3.part.size": "5242880",

      "store.url": "http://minio:9000",
      "s3.path.style.access.enabled": "true",

      "aws.access.key.id": "minioadmin",
      "aws.secret.access.key": "minioadmin",

      "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
      "parquet.codec": "snappy",

      "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
      "partition.duration.ms": "3600000",
      "path.format": "'\''year'\''=YYYY/'\'month'\''=MM/'\'day'\''=dd",
      "locale": "en-US",
      "timezone": "UTC",

      "flush.size": "10000",
      "rotate.interval.ms": "600000",

      "schema.registry.url": "http://schema-registry:8081",

      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "http://schema-registry:8081"
    }
  }'
```

**Статус коннектора:**
```bash
curl http://localhost:8083/connectors/s3-sink-users/status
```

**Удаление коннектора:**
```bash
curl -X DELETE http://localhost:8083/connectors/s3-sink-users
```

**Обновление конфигурации:**
```bash
curl -X PUT http://localhost:8083/connectors/s3-sink-users/config \
  -H "Content-Type: application/json" \
  -d '{...новая конфигурация...}'
```

---

### 5. Jupyter

#### Web UI

**URL:** `http://localhost:8888`
**Token:** `datalake`

**Примеры использования:**

**Подключение к Trino:**
```python
from trino.dbapi import connect

conn = connect(
    host='trino',
    port=8080,
    user='admin',
    catalog='iceberg',
    schema='default'
)

import pandas as pd
df = pd.read_sql("SELECT * FROM users", conn)
df.head()
```

**Чтение из MinIO:**
```python
import s3fs
import pandas as pd

# Инициализация S3 filesystem
s3 = s3fs.S3FileSystem(
    key='minioadmin',
    secret='minioadmin',
    client_kwargs={
        'endpoint_url': 'http://minio:9000'
    }
)

# Чтение Parquet
df = pd.read_parquet(
    's3://datalake/users/year=2025/month=12/day=23/part-00001.parquet',
    filesystem=s3
)
df.head()
```

**Визуализация:**
```python
import plotly.express as px

fig = px.line(df, x='created_date', y='count', title='Daily Users')
fig.show()
```

---

## Production конфигурация

### Переменные окружения (.env файл)

Создайте `.env` файл в корне проекта:

```bash
# === MINIO ===
MINIO_ROOT_USER=admin_production_user_2025
MINIO_ROOT_PASSWORD=SuperSecurePassword123!@#MinIO
MINIO_DOMAIN=minio.company.com
MINIO_SERVER_URL=https://minio.company.com

# === POSTGRESQL ===
POSTGRES_HOST=postgres-metastore.company.com
POSTGRES_DB=metastore_production
POSTGRES_USER=hive_prod
POSTGRES_PASSWORD=HiveSecurePass987!@#

# === S3/MINIO ===
S3_ENDPOINT=https://minio.company.com
S3_ACCESS_KEY=${MINIO_ROOT_USER}
S3_SECRET_KEY=${MINIO_ROOT_PASSWORD}

# === KAFKA ===
KAFKA_BOOTSTRAP_SERVERS=kafka1.company.com:9092,kafka2.company.com:9092,kafka3.company.com:9092

# === SCHEMA REGISTRY ===
SCHEMA_REGISTRY_URL=http://schema-registry.company.com:8081

# === TRINO ===
TRINO_COORDINATOR=trino-coordinator.company.com:8080

# === JUPYTER ===
JUPYTER_TOKEN=$(openssl rand -hex 32)  # Сгенерировать случайный токен
```

### Docker Compose для Production

```yaml
version: '3.8'

# === СЕКРЕТЫ ===
secrets:
  minio_root_password:
    external: true
  postgres_password:
    external: true

# === NETWORKS ===
networks:
  datalake:
    driver: overlay
    attachable: true

# === VOLUMES ===
volumes:
  minio-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /mnt/storage/minio

  metastore-db:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /mnt/storage/postgres

# === SERVICES ===
services:

  # === MINIO ===
  minio:
    image: minio/minio:RELEASE.2024-01-01T00-00-00Z
    hostname: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    networks:
      - datalake
    environment:
      MINIO_ROOT_USER_FILE: /run/secrets/minio_root_user
      MINIO_ROOT_PASSWORD_FILE: /run/secrets/minio_root_password
      MINIO_DOMAIN: ${MINIO_DOMAIN}
      MINIO_SERVER_URL: ${MINIO_SERVER_URL}
      MINIO_PROMETHEUS_AUTH_TYPE: public
      MINIO_CACHE_DRIVES: /mnt/cache
      MINIO_CACHE_QUOTA: 80
    secrets:
      - minio_root_user
      - minio_root_password
    volumes:
      - /mnt/data1:/data1
      - /mnt/data2:/data2
      - /mnt/cache:/mnt/cache
    command: server http://minio{1...4}/data{1...4} --console-address ":9001"
    deploy:
      mode: replicated
      replicas: 4
      placement:
        constraints:
          - node.role == worker
          - node.labels.storage == ssd
      resources:
        limits:
          memory: 16G
          cpus: '4.0'
        reservations:
          memory: 8G
          cpus: '2.0'
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s

  # === POSTGRESQL ===
  postgres-metastore:
    image: postgres:15-alpine
    hostname: postgres-metastore
    networks:
      - datalake
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD_FILE: /run/secrets/postgres_password
      POSTGRES_SHARED_BUFFERS: 512MB
      POSTGRES_EFFECTIVE_CACHE_SIZE: 2GB
      POSTGRES_WORK_MEM: 32MB
      POSTGRES_MAINTENANCE_WORK_MEM: 256MB
      POSTGRES_MAX_CONNECTIONS: 200
      POSTGRES_LOG_STATEMENT: ddl
      POSTGRES_LOG_DURATION: on
    secrets:
      - postgres_password
    volumes:
      - metastore-db:/var/lib/postgresql/data
      - ./postgres-init:/docker-entrypoint-initdb.d:ro
      - /mnt/backups/postgres:/backups
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.role == worker
          - node.labels.database == true
      resources:
        limits:
          memory: 4G
          cpus: '2.0'
        reservations:
          memory: 2G
          cpus: '1.0'
      restart_policy:
        condition: on-failure
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # === HIVE METASTORE ===
  hive-metastore:
    build:
      context: ./data-lake/hive-metastore
    hostname: hive-metastore
    ports:
      - "9083:9083"
    networks:
      - datalake
    environment:
      SERVICE_NAME: metastore
      DB_DRIVER: postgres
      SERVICE_OPTS: >-
        -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
        -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://${POSTGRES_HOST}:5432/${POSTGRES_DB}
        -Djavax.jdo.option.ConnectionUserName=${POSTGRES_USER}
        -Djavax.jdo.option.ConnectionPassword=${POSTGRES_PASSWORD}
        -Djavax.jdo.option.ConnectionPoolingType=HikariCP
        -Dhikaricp.maximumPoolSize=20
        -Dhikaricp.minimumIdle=5
        -Xmx4g -Xms2g
        -XX:+UseG1GC
      AWS_ACCESS_KEY_ID: ${S3_ACCESS_KEY}
      AWS_SECRET_ACCESS_KEY: ${S3_SECRET_KEY}
      S3_ENDPOINT_URL: ${S3_ENDPOINT}
      S3_PATH_STYLE_ACCESS: "true"
    depends_on:
      - postgres-metastore
      - minio
    deploy:
      mode: replicated
      replicas: 2
      placement:
        constraints:
          - node.role == worker
      resources:
        limits:
          memory: 6G
          cpus: '2.0'
        reservations:
          memory: 3G
          cpus: '1.0'
      restart_policy:
        condition: on-failure
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "9083"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 90s

  # === TRINO COORDINATOR ===
  trino-coordinator:
    image: trinodb/trino:435
    hostname: trino-coordinator
    ports:
      - "8080:8080"
    networks:
      - datalake
    volumes:
      - ./data-lake/trino/config-coordinator.properties:/etc/trino/config.properties:ro
      - ./data-lake/trino/jvm-coordinator.config:/etc/trino/jvm.config:ro
      - ./data-lake/trino/node.properties:/etc/trino/node.properties:ro
      - ./data-lake/trino/log.properties:/etc/trino/log.properties:ro
      - ./data-lake/trino/catalog:/etc/trino/catalog:ro
    depends_on:
      - hive-metastore
      - minio
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.role == worker
          - node.labels.trino == coordinator
      resources:
        limits:
          memory: 32G
          cpus: '8.0'
        reservations:
          memory: 16G
          cpus: '4.0'
      restart_policy:
        condition: on-failure
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/v1/info"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s

  # === TRINO WORKERS ===
  trino-worker:
    image: trinodb/trino:435
    networks:
      - datalake
    volumes:
      - ./data-lake/trino/config-worker.properties:/etc/trino/config.properties:ro
      - ./data-lake/trino/jvm-worker.config:/etc/trino/jvm.config:ro
      - ./data-lake/trino/node.properties:/etc/trino/node.properties:ro
      - ./data-lake/trino/log.properties:/etc/trino/log.properties:ro
      - ./data-lake/trino/catalog:/etc/trino/catalog:ro
    depends_on:
      - trino-coordinator
    deploy:
      mode: replicated
      replicas: 5
      placement:
        constraints:
          - node.role == worker
          - node.labels.trino == worker
      resources:
        limits:
          memory: 64G
          cpus: '16.0'
        reservations:
          memory: 32G
          cpus: '8.0'
      restart_policy:
        condition: on-failure
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/v1/info"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
```

---

## Мониторинг и безопасность

### Мониторинг

#### Prometheus + Grafana

Добавьте в `compose.yaml`:

```yaml
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--storage.tsdb.retention.time=30d'

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD}
    volumes:
      - grafana-data:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards:ro
```

#### Метрики для мониторинга:

**MinIO:**
- Requests per second
- Bandwidth (in/out)
- Storage usage
- Latency (p50, p95, p99)

**Trino:**
- Query count
- Query duration
- CPU/Memory usage
- Failed queries

**Kafka Connect:**
- Records processed
- Connector status
- Task failures
- Lag

### Безопасность

#### 1. Сетевая изоляция

```yaml
networks:
  frontend:
    # Только UI сервисы
  backend:
    # Только backend сервисы
    internal: true  # Нет доступа извне
```

#### 2. TLS/SSL

Используйте Traefik или nginx для терминации SSL:

```yaml
  traefik:
    image: traefik:v2.10
    ports:
      - "443:443"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./traefik/traefik.yml:/traefik.yml:ro
      - ./traefik/certs:/certs:ro
```

#### 3. Управление секретами

Используйте Docker Secrets или Vault:

```bash
# Создать секрет
echo "MySecurePassword" | docker secret create postgres_password -

# Использовать в compose
services:
  postgres:
    secrets:
      - postgres_password
    environment:
      POSTGRES_PASSWORD_FILE: /run/secrets/postgres_password
```

#### 4. Backup стратегия

**MinIO:**
```bash
# Ежедневный backup
mc mirror minio/datalake backup-server/datalake-backup
```

**PostgreSQL:**
```bash
# Cron job
0 2 * * * docker exec postgres-metastore \
  pg_dump -U hive metastore_db | \
  gzip > /backups/metastore-$(date +\%Y\%m\%d).sql.gz
```

---

## Заключение

Этот документ описывает полную архитектуру Data Lake на базе:
- MinIO (хранилище)
- Hive Metastore (каталог)
- Trino (SQL engine)
- Kafka Connect (интеграция)
- Jupyter (аналитика)

Для развертывания на production:
1. Замените все пароли и токены
2. Настройте TLS/SSL
3. Используйте внешние volumes
4. Настройте мониторинг
5. Создайте backup стратегию
6. Масштабируйте воркеры Trino
7. Используйте distributed MinIO

**Следующие шаги:**
1. Протестировать локально
2. Настроить CI/CD для деплоя
3. Создать runbook для операций
4. Обучить команду работе с системой
