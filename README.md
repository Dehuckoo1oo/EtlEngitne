# ETL Engine Documentation Guide

Полная документация по архитектуре, использованию и внутренним механизмам ETL Engine.

---

## Содержание документации

### 📘 [PIPELINE_GUIDE.md](PIPELINE_GUIDE.md)

**Полное руководство по архитектуре и работе ETL Pipeline**

Охватывает:
- Обзор архитектуры
- Создание и конфигурация ETL Job
- Валидация параметров
- Компоненты Pipeline (Extractor, Transformer, Loader)
- Streaming архитектура и batch-by-batch обработка
- Система метрик и мониторинг
- Реализованные цепочки (Kafka→SQL, SQL→Kafka, SQL→SQL)
- Примеры использования
- Best practices и troubleshooting

**Читать первым для общего понимания системы.**

---

### 🔄 [SEQUENCE_DIAGRAMS.md](SEQUENCE_DIAGRAMS.md)

**Детальные диаграммы последовательности выполнения**

Охватывает:
- Полный поток Kafka → SQL с вызовами методов
- Параллельная обработка партиций
- SQL → SQL партицированное чтение
- Система метрик в реальном времени
- Обработка ошибок и Dead Letter Queue
- Отмена джобов (cancellation)
- Spring DI и регистрация компонентов

**Читать для глубокого понимания внутренних процессов.**

---

## Быстрый старт

### 1. Простой пример Kafka → SQL

```java
EtlJob job = new EtlJob(
    "my-first-job",
    null,
    "analytics.dbo.orders",
    Map.of(
        "extractorType", "kafka",
        "transformerType", "avro",
        "loaderType", "fast-sql",
        "topic", "order-events",
        "format", "avro",
        "startTimestamp", startTime.toEpochMilli(),
        "endTimestamp", endTime.toEpochMilli(),
        "streamBatchSize", 100_000,
        "threads", 4
    )
);

EtlPipeline pipeline = pipelineFactory.create(job);
pipeline.run(job);
```

### 2. Мониторинг выполнения

```java
Optional<EtlJobMetricsSnapshot> metrics = metricsCollector.getSnapshot("my-first-job");

metrics.ifPresent(m -> {
    System.out.println("Status: " + m.status());
    System.out.println("Progress: " + m.loadedRecords() + " records");
    System.out.println("Throughput: " + m.throughput() + " rec/sec");
    System.out.println("Duration: " + m.totalDuration());
});
```

---

## Архитектура (кратко)

```
┌──────────────┐
│   EtlJob     │  Конфигурация
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Validator  │  Валидация
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Factory    │  Создание pipeline
└──────┬───────┘
       │
       ▼
┌─────────────────────────┐
│ StreamingEtlPipeline    │
│                         │
│  Extractor              │
│      ↓                  │
│  Transformer            │
│      ↓                  │
│  Loader                 │
└─────────────────────────┘
```

### Streaming обработка (Batch-by-Batch)

```
Batch 1: Extract → Transform → Load
Batch 2: Extract → Transform → Load
Batch 3: Extract → Transform → Load
...

❌ НЕ ТАК: Extract all → Transform all → Load all
```

---

## Доступные компоненты

### Extractors
- `kafka` - KafkaExtractor (многопоточное чтение партиций)
- `sql` - JdbcExtractor (партицированное чтение SQL)

### Transformers
- `noop` - NoopTransformer (без изменений)
- `avro` - AvroToRecordTransformer (Avro → плоские колонки)
- `record-to-avro` - RecordToAvroTransformer (плоские колонки → Avro)

### Loaders
- `fast-sql` - FastSqlServerLoader (SQL Server Bulk Copy, высокая производительность)
- `kafka` - KafkaLoader (публикация в Kafka)
- `jdbc` - JdbcLoader (стандартный JDBC batch insert)

---

## Поддержка типов данных

### SQL Server ↔ Avro Type Conversion

Engine поддерживает полную конвертацию типов между SQL Server, Java и Avro:

**Основные типы:**
- Числовые: `INT`, `BIGINT`, `DECIMAL(p,s)`, `MONEY`
- Строковые: `NVARCHAR`, `VARCHAR`, `CHAR`
- Даты/время: `DATE`, `DATETIME2`, `TIME`
- Бинарные: `VARBINARY`, `BINARY`
- Специальные: `UNIQUEIDENTIFIER`, `SQL_VARIANT`

**Подробная матрица типов:** См. [`doc/type-conversion.md`](doc/type-conversion.md)

### SQL_VARIANT Support

Engine полностью поддерживает SQL Server тип `SQL_VARIANT`:

**Table-based конфигурация (автоматическая):**
```java
Map.of(
    "extractorType", "sql",
    "table", "dbo.orders",        // Автогенерация SQL_VARIANT_PROPERTY
    "threads", 4
)
```

**Custom query (ручная):**
```sql
SELECT
  id,
  variant_col,
  -- Обязательные метаданные для sql_variant колонок:
  SQL_VARIANT_PROPERTY(variant_col, 'BaseType') as __variant_variant_col_basetype,
  SQL_VARIANT_PROPERTY(variant_col, 'Precision') as __variant_variant_col_precision,
  SQL_VARIANT_PROPERTY(variant_col, 'Scale') as __variant_variant_col_scale,
  SQL_VARIANT_PROPERTY(variant_col, 'MaxLength') as __variant_variant_col_maxlength
FROM orders
```

**Валидация:**
- Table-based режим: автоматическая генерация метаданных
- Custom query: валидация при старте, детальные инструкции при ошибке
- Поддержка разных базовых типов в разных строках

**Known Limitations:**
- При bulk copy `VARCHAR → NVARCHAR`, `CHAR → NCHAR` (функционально эквивалентно)
- См. детали в [`doc/type-conversion-requirements.md`](doc/type-conversion-requirements.md)

---

## Производительность

| Цепочка | Throughput | Примечания |
|---------|-----------|------------|
| Kafka → SQL (Avro) | 50-60k rec/sec | 4 потока, 72 партиции |
| SQL → Kafka (Avro) | 150-170k rec/sec | Партицирование |
| SQL → SQL | 200k+ rec/sec | Bulk Copy |

---

## Best Practices

### 1. Размер батча
```java
// Kafka с множеством партиций
"streamBatchSize", 50_000

// SQL с большими таблицами
"streamBatchSize", 100_000
```

### 2. Количество потоков
```java
// Оптимально: CPU cores
int threads = Runtime.getRuntime().availableProcessors();
```

### 3. Партицирование SQL
```java
"partitions", 72        // Кратно количеству потоков
"partitionColumn", "id" // Колонка с хорошим распределением
```

---

## Troubleshooting

### Низкий throughput
1. Увеличить `threads`
2. Увеличить `streamBatchSize`
3. Проверить `setTableLock(false)` для SQL

### Out of Memory
1. Уменьшить `streamBatchSize`
2. Уменьшить `threads`

### SQL Deadlocks
1. Убедиться что `setTableLock(false)`
2. Уменьшить количество потоков

---

## Дополнительные ресурсы

- [METRICS.md](../METRICS.md) - Система метрик
- [STREAMING.md](../STREAMING.md) - Streaming архитектура
- [UI_GUIDE.md](../UI_GUIDE.md) - Web интерфейс

---

## Контакты и поддержка

Для вопросов и предложений создавайте issues в репозитории проекта.
