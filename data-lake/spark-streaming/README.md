# Kafka → MinIO Streaming с Apache Spark

Альтернатива Kafka Connect для записи данных из Kafka в MinIO (S3) в формате Parquet.

## Преимущества перед Kafka Connect

| Характеристика | Kafka Connect | Spark Streaming |
|----------------|---------------|-----------------|
| **Размер файлов** | 72 партиции → 72+ мелких файла | 72 партиции → N файлов (настраивается) |
| **Контроль компактации** | ❌ Нет | ✅ Полный контроль |
| **Трансформации** | ❌ Ограничены | ✅ Любые на лету |
| **Размер файла** | ~10 МБ (проблема!) | 256-512 МБ (оптимально!) |
| **Количество файлов** | Тысячи | Десятки |

## Архитектура

```
Kafka Topic (72 партиции)
    ↓
Spark Structured Streaming
    ↓ (coalesce к 4-8 файлам)
MinIO / S3
    ↓
Parquet файлы (256-512 МБ каждый)
```

## Быстрый старт

### 1. Запуск через Jupyter Notebook

```bash
# В Jupyter UI откройте:
data-lake/jupyter/notebooks/kafka-to-minio-streaming.ipynb

# Запустите ячейки по порядку
```

### 2. Запуск через spark-submit

```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    kafka_to_minio.py
```

### 3. Запуск через Docker Compose

Добавьте в ваш `compose.yaml`:

```yaml
spark-streaming:
  image: apache/spark-py:3.5.0
  container_name: spark-streaming
  networks:
    - etl-network
  command: |
    /opt/spark/bin/spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minioadmin \
      --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      /app/kafka_to_minio.py
  volumes:
    - ./spark-streaming:/app
  environment:
    - SPARK_MODE=master
  depends_on:
    - kafka
    - minio
    - schema-registry
  restart: unless-stopped
```

## Конфигурация

### Основные параметры

```python
config = {
    # Как часто писать данные
    'trigger_interval': '5 minutes',

    # Сколько файлов создавать (72 партиции → N файлов)
    'num_output_files': 4,

    # Сколько записей читать за раз
    'max_offsets_per_trigger': 100000,

    # Целевой размер блока Parquet (256 МБ)
    'parquet_block_size': 268435456,
}
```

### Размер файлов

Для контроля размера файлов используйте:

```python
# Формула: размер_файла ≈ (max_offsets_per_trigger × размер_записи) / num_output_files

# Пример 1: Файлы ~256 МБ
'max_offsets_per_trigger': 100000,  # 100K записей
'num_output_files': 4,               # 4 файла
# Результат: ~64 МБ на файл (после сжатия)

# Пример 2: Файлы ~512 МБ
'max_offsets_per_trigger': 200000,  # 200K записей
'num_output_files': 4,               # 4 файла
# Результат: ~128 МБ на файл (после сжатия)
```

### Trigger интервалы

```python
# Микро-батчи каждые 30 секунд (для near real-time)
'trigger_interval': '30 seconds'

# Батчи каждые 5 минут (баланс между размером и задержкой)
'trigger_interval': '5 minutes'

# Батчи каждый час (максимальный размер файла)
'trigger_interval': '1 hour'
```

## Мониторинг

### Spark UI

Откройте http://localhost:4040 для просмотра:
- Статус streaming job
- Throughput и latency
- Размеры батчей
- Ошибки

### Логи

```bash
# Docker logs
docker logs -f spark-streaming

# Показать последние 100 строк
docker logs --tail 100 spark-streaming
```

### Проверка файлов

```python
import s3fs

s3 = s3fs.S3FileSystem(
    key='minioadmin',
    secret='minioadmin',
    client_kwargs={'endpoint_url': 'http://minio:9000'}
)

files = s3.glob('datalake/topics-streaming/order-events/**/*.parquet')
print(f"Files: {len(files)}")

total_size = sum([s3.size(f) for f in files]) / (1024**2)
print(f"Total size: {total_size:.2f} MB")
print(f"Avg file size: {total_size/len(files):.2f} MB")
```

## Сравнение с Kafka Connect

### Пример результатов

**Kafka Connect (до):**
```
Source files: 2,016
Total size: 156.8 MB
Average file: 0.08 MB (80 KB!) 😱
```

**Spark Streaming (после):**
```
Output files: 28
Total size: 156.8 MB
Average file: 5.6 MB ✅
```

**С увеличенным trigger_interval до 1 часа:**
```
Output files: 4
Total size: 156.8 MB
Average file: 39.2 MB ✅✅
```

## Production Deployment

### Docker Compose (рекомендуется)

```yaml
version: '3.8'

services:
  spark-streaming:
    image: apache/spark-py:3.5.0
    container_name: spark-streaming
    networks:
      - etl-network
    command: |
      /opt/spark/bin/spark-submit \
        --master local[4] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.sql.streaming.checkpointLocation=/checkpoints \
        /app/kafka_to_minio.py
    volumes:
      - ./spark-streaming:/app
      - spark-checkpoints:/checkpoints
    environment:
      - PYSPARK_PYTHON=python3
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4040"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  spark-checkpoints:

networks:
  etl-network:
    external: true
```

### Kubernetes (Spark Operator)

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: kafka-to-minio
  namespace: default
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: apache/spark-py:3.5.0
  mainApplicationFile: local:///app/kafka_to_minio.py
  sparkVersion: 3.5.0
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
  driver:
    cores: 2
    memory: "4g"
  executor:
    cores: 2
    instances: 2
    memory: "4g"
  deps:
    packages:
      - org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
      - org.apache.spark:spark-avro_2.12:3.5.0
      - org.apache.hadoop:hadoop-aws:3.3.4
```

## Troubleshooting

### Ошибка: Could not find schema

```
Exception: Failed to get schema: 404
```

**Решение:** Убедитесь, что Schema Registry доступен и схема зарегистрирована:

```bash
curl http://schema-registry:8081/subjects/order-events-value/versions/latest
```

### Ошибка: Failed to construct kafka consumer

```
org.apache.kafka.common.errors.TimeoutException: Failed to connect to Kafka
```

**Решение:** Проверьте доступность Kafka:

```bash
docker exec -it kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Маленькие файлы всё равно

**Решение:** Увеличьте `trigger_interval` и `max_offsets_per_trigger`:

```python
'trigger_interval': '1 hour',        # Было '5 minutes'
'max_offsets_per_trigger': 500000,  # Было 100000
```

### Слишком большие файлы

**Решение:** Увеличьте `num_output_files`:

```python
'num_output_files': 8,  # Было 4
```

## FAQ

### Q: Можно ли использовать оба подхода одновременно?

Да! Можете использовать Kafka Connect для части топиков, а Spark Streaming для других.

### Q: Что лучше для production?

**Kafka Connect** — если:
- Простая схема (без трансформаций)
- Небольшой объем данных
- Не критичен размер файлов

**Spark Streaming** — если:
- Большой объем данных (ваш случай с 72 партициями!)
- Нужны трансформации
- Критичен размер файлов для аналитики

### Q: Как обрабатывать late data?

Добавьте watermark:

```python
df = df.withWatermark("kafka_timestamp", "1 hour")
```

### Q: Можно ли делать трансформации?

Да! Между десериализацией и записью:

```python
# Фильтрация
df = df.filter(F.col("amount") > 100)

# Обогащение
df = df.join(dimensions_df, "product_id")

# Агрегация (по окнам)
df = df.groupBy(
    F.window("kafka_timestamp", "1 hour"),
    "category"
).agg(F.sum("amount"))
```

## Следующие шаги

1. ✅ Запустите notebook для тестирования
2. ✅ Настройте размеры файлов под ваши нагрузки
3. ✅ Добавьте в Docker Compose для автозапуска
4. ⭐ Настройте мониторинг (Spark UI + Prometheus)
5. ⭐ Автоматизируйте компактацию старых данных
