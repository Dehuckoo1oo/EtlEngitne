# Data Lake Infrastructure

Данный каталог содержит конфигурацию и настройки для компонентов Data Lake.

## Архитектура

```
Kafka (Avro) → Kafka Connect (S3 Sink, Parquet) → MinIO (S3) → Trino (SQL) / Jupyter
```

## Компоненты

### MinIO (S3 Storage)
- **Web Console**: http://localhost:9001
- **S3 API**: http://localhost:9000
- **Credentials**: minioadmin / minioadmin
- **Bucket**: datalake

### Kafka Connect
- **REST API**: http://localhost:8083
- **Connectors**: S3 Sink для топика order-events
- **Режим**: Distributed Worker
- **Plugins**: Confluent S3 Sink Connector 10.5.0

### Trino
- **UI**: http://localhost:8084
- **Catalog**: minio (Hive), iceberg (будущее)
- **Назначение**: Интерактивные SQL запросы поверх Parquet

### Jupyter Notebook
- **URL**: http://localhost:8888
- **Token**: datalake
- **Назначение**: Ручная проверка и анализ Parquet файлов

## Quick Start

### 1. Запуск всех сервисов

```bash
docker-compose up -d
```

### 2. Проверка статуса сервисов

```bash
docker-compose ps
```

Убедитесь, что все сервисы в состоянии `healthy` или `running`.

### 3. Деплой S3 Sink Connector

Подождите ~30-60 секунд, пока Kafka Connect полностью запустится, затем:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @data-lake/kafka-connect/connectors/s3-sink-order-events.json
```

### 4. Проверка статуса коннектора

```bash
curl http://localhost:8083/connectors/s3-sink-order-events/status
```

Должен быть статус `RUNNING` для коннектора и всех tasks.

## Проверка работоспособности

### MinIO

1. Откройте MinIO Console: http://localhost:9001
2. Войдите: minioadmin / minioadmin
3. Проверьте наличие бакета `datalake`

### Kafka Connect

```bash
# Список всех коннекторов
curl http://localhost:8083/connectors

# Детальный статус
curl http://localhost:8083/connectors/s3-sink-order-events/status | jq

# Список задач
curl http://localhost:8083/connectors/s3-sink-order-events/tasks
```

### Trino

```bash
# Подключение к Trino CLI
docker exec -it etl-engine-trino-1 trino

# В Trino CLI выполните:
SHOW CATALOGS;
```

### Jupyter

1. Откройте http://localhost:8888
2. Введите token: `datalake`
3. Откройте `validate-parquet.ipynb`

## Публикация тестовых данных

### Вариант 1: Запуск интеграционного теста

```bash
mvn test -Dtest=KafkaToSqlIntegrationTest
```

Этот тест опубликует 1M записей в топик `order-events`.

### Вариант 2: AKHQ UI

1. Откройте http://localhost:8089 (test / test)
2. Перейдите в Topics → order-events
3. Вручную опубликуйте тестовые сообщения

## Проверка Parquet файлов

### Через MinIO Console

1. Откройте http://localhost:9001
2. Перейдите в Object Browser → datalake
3. Найдите файлы в `topics/order-events/`
4. Проверьте размеры файлов (~128-256 MB)

### Через Trino SQL

```bash
docker exec -it etl-engine-trino-1 trino
```

Выполните SQL:

```sql
-- Создать схему
CREATE SCHEMA IF NOT EXISTS minio.datalake;

-- Создать внешнюю таблицу
CREATE TABLE IF NOT EXISTS minio.datalake.order_events (
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
  special_instructions VARCHAR
)
WITH (
  external_location = 's3a://datalake/topics/order-events/',
  format = 'PARQUET'
);

-- Запросить данные
SELECT COUNT(*) FROM minio.datalake.order_events;

SELECT * FROM minio.datalake.order_events LIMIT 10;

-- Аналитический запрос
SELECT
  status,
  COUNT(*) as total_orders,
  AVG(total_amount) as avg_amount,
  SUM(total_amount) as total_revenue
FROM minio.datalake.order_events
GROUP BY status;
```

### Через Jupyter Notebook

1. Откройте http://localhost:8888 (token: datalake)
2. Откройте `validate-parquet.ipynb`
3. Выполните все ячейки (Cell → Run All)

## Модель данных

### Структура S3 путей

```
s3://datalake/
  topics/
    order-events/
      calc_id=20251222-120000/
        dt=2025-12-22/hour=12/
          part-00000.parquet
          part-00001.parquet
        dt=2025-12-22/hour=13/
          part-00000.parquet
```

### Особенности
- **Иммутабельность**: Данные не обновляются, только добавляются
- **Partitioning**: По времени (hourly) с помощью `calc_id`, `dt`, `hour`
- **Размер файлов**: ~128-256 MB (оптимально для аналитики)
- **Формат**: Parquet с Snappy компрессией
- **Схема**: Управляется через Schema Registry

## Управление коннектором

### Пауза коннектора

```bash
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/pause
```

### Возобновление коннектора

```bash
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/resume
```

### Обновление конфигурации

```bash
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/config \
  -H "Content-Type: application/json" \
  -d @data-lake/kafka-connect/connectors/s3-sink-order-events.json
```

### Удаление коннектора

```bash
curl -X DELETE http://localhost:8083/connectors/s3-sink-order-events
```

### Масштабирование

Увеличение числа задач для параллельной обработки:

```bash
curl -X PUT http://localhost:8083/connectors/s3-sink-order-events/config \
  -H "Content-Type: application/json" \
  -d '{"tasks.max": "24"}'
```

## Мониторинг

### Kafka Connect Metrics

```bash
# Общий статус
curl http://localhost:8083/ | jq

# Статус коннектора
curl http://localhost:8083/connectors/s3-sink-order-events/status | jq

# Метрики задач
curl http://localhost:8083/connectors/s3-sink-order-events/tasks | jq
```

### Kafka Lag (AKHQ)

1. Откройте http://localhost:8089
2. Перейдите в Consumer Groups
3. Найдите группу `kafka-connect-cluster`
4. Проверьте lag по партициям

### MinIO Metrics

1. Откройте http://localhost:9001
2. Перейдите в Monitoring → Metrics
3. Проверьте:
   - Storage usage
   - Upload/download throughput
   - Object count

### Trino Query History

1. Откройте http://localhost:8084
2. Перейдите в Query History
3. Проверьте:
   - Query latency
   - Rows processed
   - Data scanned

## Troubleshooting

### Проблема: Коннектор не стартует

**Симптомы**: Статус коннектора `FAILED`

**Решения**:
1. Проверить логи:
   ```bash
   docker-compose logs kafka-connect
   ```

2. Проверить доступность MinIO:
   ```bash
   docker exec kafka-connect curl http://minio:9000/minio/health/live
   ```

3. Проверить синтаксис JSON конфигурации
4. Убедиться, что Schema Registry доступен:
   ```bash
   curl http://localhost:8081/subjects
   ```

### Проблема: Нет Parquet файлов в MinIO

**Симптомы**: Бакет `datalake` пустой

**Решения**:
1. Проверить, есть ли данные в топике order-events (AKHQ)
2. Проверить статус задач коннектора:
   ```bash
   curl http://localhost:8083/connectors/s3-sink-order-events/tasks | jq
   ```
3. Подождать 5 минут (rotate.interval.ms)
4. Проверить логи коннектора на ошибки

### Проблема: Файлы слишком маленькие (<10 MB)

**Симптомы**: Множество мелких файлов вместо 128-256 MB

**Решения**:
1. Увеличить `flush.size` до 100,000-200,000
2. Увеличить `rotate.interval.ms` до 600,000 (10 минут)
3. Проверить throughput топика (должен быть достаточный поток данных)
4. Рассмотреть увеличение `tasks.max`

### Проблема: Trino не может прочитать файлы

**Симптомы**: Ошибки "file not found" или "access denied"

**Решения**:
1. Проверить credentials в `minio.properties`:
   ```properties
   hive.s3.aws-access-key=minioadmin
   hive.s3.aws-secret-key=minioadmin
   ```

2. Проверить путь в CREATE TABLE statement
3. Протестировать доступ из контейнера Trino:
   ```bash
   docker exec -it etl-engine-trino-1 curl http://minio:9000/
   ```

4. Проверить права доступа к бакету в MinIO Console

### Проблема: Schema mismatch

**Симптомы**: Ошибка "incompatible schema"

**Решения**:
1. Проверить версию схемы в Schema Registry:
   ```bash
   curl http://localhost:8081/subjects/order-events-value/versions
   ```

2. Убедиться, что `schema.compatibility: BACKWARD` в конфигурации коннектора
3. Проверить, что изменения схемы backward compatible
4. При критичных изменениях - пересоздать коннектор

## Настройка производительности

### Kafka Connect

**Оптимизация для больших объёмов**:

```json
{
  "tasks.max": "24",
  "flush.size": "100000",
  "rotate.interval.ms": "600000",
  "consumer.override.max.poll.records": "10000",
  "consumer.override.max.partition.fetch.bytes": "20971520"
}
```

**Оптимизация для низкой задержки**:

```json
{
  "tasks.max": "12",
  "flush.size": "10000",
  "rotate.interval.ms": "60000"
}
```

### Trino

Для тяжёлых запросов увеличьте память в `jvm.config`:

```
-Xmx16G
-Xms16G
```

И в `config.properties`:

```properties
query.max-memory=8GB
query.max-memory-per-node=4GB
```

## Ссылки на UI

| Сервис | URL | Credentials |
|--------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin |
| Kafka Connect API | http://localhost:8083 | - |
| Trino UI | http://localhost:8084 | - |
| Jupyter | http://localhost:8888 | token: datalake |
| AKHQ (Kafka) | http://localhost:8089 | test/test |
| Schema Registry | http://localhost:8081 | - |
| Schema Registry UI | http://localhost:8000 | - |

## Дальнейшие улучшения

1. **Phase 2**: Добавить второй Kafka Connect worker для HA
2. **Phase 3**: Внедрить dedicated Hive Metastore
3. **Phase 4**: Миграция на Apache Iceberg для ACID
4. **Phase 5**: Добавить Apache Spark для batch processing
5. **Phase 6**: Настроить мониторинг (Prometheus + Grafana)

## Документация

- План реализации: `doc/data-lake-implementation.md`
- Архитектура: `doc/architecture_data_lake_kafka_parquet.md`
- Общий план: `doc/plan_kafka_to_parquet_min_io_local_and_prod.md`
