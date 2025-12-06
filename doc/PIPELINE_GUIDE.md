# ETL Engine Pipeline Guide

Полное руководство по архитектуре и работе ETL Pipeline от создания джоба до выполнения.

---

## Содержание

1. [Обзор архитектуры](#обзор-архитектуры)
2. [Создание ETL Job](#создание-etl-job)
3. [Валидация джоба](#валидация-джоба)
4. [Создание Pipeline](#создание-pipeline)
5. [Компоненты Pipeline](#компоненты-pipeline)
6. [Streaming архитектура](#streaming-архитектура)
7. [Жизненный цикл выполнения](#жизненный-цикл-выполнения)
8. [Метрики и мониторинг](#метрики-и-мониторинг)
9. [Реализованные цепочки](#реализованные-цепочки)
10. [Примеры использования](#примеры-использования)

---

## Обзор архитектуры

ETL Engine построен на основе **потоковой (streaming) архитектуры**, которая обрабатывает данные батчами, обеспечивая эффективное использование памяти и высокую производительность.

```
┌──────────────┐
│   EtlJob     │  Конфигурация задачи
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ JobValidator │  Валидация параметров
└──────┬───────┘
       │
       ▼
┌──────────────────┐
│ EtlPipelineFactory │  Создание pipeline
└──────┬───────────┘
       │
       ▼
┌─────────────────────────────┐
│  StreamingEtlPipeline       │
│  ┌─────────────────────┐    │
│  │  Extractor          │────┐
│  └─────────────────────┘    │
│           │                 │ Batch 1
│           ▼                 │
│  ┌─────────────────────┐    │
│  │  Transformer        │◄───┘
│  └─────────────────────┘    │
│           │                 │ Batch 1 (transformed)
│           ▼                 │
│  ┌─────────────────────┐    │
│  │  Loader             │◄───┘
│  └─────────────────────┘    │
│           │                 │
│           └─────────────────┤ Repeat for Batch 2, 3, ...
└─────────────────────────────┘
```

### Ключевые принципы:

1. **Streaming Processing** - данные обрабатываются батчами, а не загружаются целиком в память
2. **Batch-by-Batch** - каждый батч проходит через Extract → Transform → Load перед следующим
3. **Параллелизм** - поддержка многопоточной обработки (особенно в Extractors)
4. **Компонентная архитектура** - Extractor, Transformer, Loader - независимые компоненты
5. **Метрики в реальном времени** - отслеживание прогресса, throughput, ошибок

---

## Создание ETL Job

### EtlJob - Модель данных

```java
public class EtlJob {
    private final String jobId;           // Уникальный идентификатор задачи
    private final String source;          // Источник данных (опционально, для JDBC)
    private final String targetTable;     // Целевая таблица
    private final Map<String, Object> parameters;  // Параметры конфигурации
}
```

**Файл:** `src/main/java/ru/pospelov/etl/engine/model/EtlJob.java`

### Обязательные параметры:

```java
Map<String, Object> params = Map.of(
    "extractorType", "kafka",      // Тип экстрактора: kafka, sql
    "transformerType", "avro",     // Тип трансформера: noop, avro
    "loaderType", "fast-sql"       // Тип загрузчика: fast-sql, kafka, jdbc
);
```

### Параметры, специфичные для компонентов:

#### Для KafkaExtractor:
```java
"topic", "order-events"              // Kafka топик
"startTimestamp", 1701878400000L     // Время начала (epoch millis)
"endTimestamp", 1701964800000L       // Время окончания (epoch millis)
"format", "avro"                     // Формат: avro, string
"threads", 4                         // Количество потоков для чтения партиций
```

#### Для JdbcExtractor (SQL):
```java
"source", "SELECT * FROM orders"     // SQL запрос или таблица
"partitionColumn", "bucket"          // Колонка для партицирования
"partitions", 72                     // Количество партиций
"threads", 4                         // Количество потоков
```

#### Общие параметры:
```java
"streamBatchSize", 50000             // Размер батча (по умолчанию 50k)
```

### Пример создания Job:

```java
EtlJob job = new EtlJob(
    "kafka-to-sql-job-001",                    // jobId
    null,                                       // source (не нужен для Kafka)
    "SUPPORT.dbo.orders",                      // targetTable
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
```

---

## Валидация джоба

### JobValidator

Перед выполнением джоб проходит валидацию через систему валидаторов.

**Файл:** `src/main/java/ru/pospelov/etl/engine/validation/JobValidator.java`

### Архитектура валидации:

```
JobValidator (главный)
    ├── ExtractorValidator  (валидация параметров экстрактора)
    ├── TransformerValidator (валидация параметров трансформера)
    └── LoaderValidator      (валидация параметров загрузчика)
```

### Что проверяется:

1. **Обязательные параметры:**
   - `extractorType`, `transformerType`, `loaderType` должны быть заданы
   - Компоненты должны быть зарегистрированы в системе

2. **Параметры для Kafka:**
   - `topic` не пустой
   - `startTimestamp` < `endTimestamp`
   - `format` валиден (avro, string)

3. **Параметры для SQL:**
   - `source` задан (SQL запрос или таблица)
   - `partitionColumn` валиден
   - `partitions` > 0

4. **Целевая таблица:**
   - `targetTable` задана для SQL loaders

### Пример валидации:

```java
ValidationResult result = jobValidator.validate(job);

if (result.hasErrors()) {
    // result.getErrors() содержит список ошибок
    throw new ValidationException("Validation failed", job.getJobId(), result);
}

// Или короткая форма:
jobValidator.validateOrThrow(job);  // Бросит исключение если есть ошибки
```

---

## Создание Pipeline

### EtlPipelineFactory

Factory отвечает за создание и конфигурацию pipeline.

**Файл:** `src/main/java/ru/pospelov/etl/engine/engine/EtlPipelineFactory.java:46`

### Процесс создания:

```java
public EtlPipeline create(EtlJob job) {
    // 1. Валидация джоба
    jobValidator.validateOrThrow(job);

    // 2. Получение типов компонентов из параметров
    String extractorType = requireType(job, "extractorType");
    String transformerType = requireType(job, "transformerType");
    String loaderType = requireType(job, "loaderType");

    // 3. Получение компонентов из Registry
    Extractor extractor = requireExtractor(job, extractorType);
    Transformer transformer = requireTransformer(job, transformerType);
    Loader loader = requireLoader(job, loaderType);

    // 4. Создание StreamingEtlPipeline
    return new StreamingEtlPipeline(
        extractor,
        transformer,
        loader,
        deadLetterQueue,      // Очередь для ошибок
        metricsCollector      // Сборщик метрик
    );
}
```

### EtlComponentRegistry

Registry автоматически регистрирует все Spring компоненты через dependency injection.

**Файл:** `src/main/java/ru/pospelov/etl/engine/engine/EtlComponentRegistry.java`

```java
@Component
public class EtlComponentRegistry {
    private final Map<String, Extractor> extractorMap;
    private final Map<String, Transformer> transformerMap;
    private final Map<String, Loader> loaderMap;

    // Spring автоматически инжектит все бины типа Extractor, Transformer, Loader
    public EtlComponentRegistry(
        List<Extractor> extractors,
        List<Transformer> transformers,
        List<Loader> loaders
    ) {
        // Создаем Map: type -> component
        this.extractorMap = extractors.stream()
            .collect(Collectors.toMap(Extractor::getType, Function.identity()));
        // ... аналогично для transformer и loader
    }
}
```

### Зарегистрированные компоненты:

**Extractors:**
- `kafka` - KafkaExtractor
- `sql` - JdbcExtractor

**Transformers:**
- `noop` - NoopTransformer (пропускает без изменений)
- `avro` - AvroToRecordTransformer (Avro → EtlRecord)
- `record-to-avro` - RecordToAvroTransformer (EtlRecord → Avro)

**Loaders:**
- `fast-sql` - FastSqlServerLoader (SQL Server Bulk Copy)
- `jdbc` - JdbcLoader (стандартный JDBC batch insert)
- `kafka` - KafkaLoader

---

## Компоненты Pipeline

### 1. Extractor

**Интерфейс:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/Extractor.java`

```java
public interface Extractor {
    void extract(EtlJob job, Consumer<Collection<EtlRecord>> batchConsumer);
    String getType();
}
```

#### Принцип работы:

Extractor **не возвращает** данные, а **вызывает callback** (`batchConsumer`) для каждого батча:

```java
extractor.extract(job, batch -> {
    // Обработка батча
    Collection<EtlRecord> transformed = transformer.transform(batch, job);
    loader.load(transformed, job);
});
```

#### Реализации:

##### KafkaExtractor

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/KafkaExtractor.java`

```java
@Component
public class KafkaExtractor implements Extractor {
    public String getType() { return "kafka"; }

    public void extract(EtlJob job, Consumer<Collection<EtlRecord>> batchConsumer) {
        // 1. Получаем партиции топика
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);

        // 2. Создаем thread pool
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // 3. Для каждой партиции запускаем отдельную задачу
        for (PartitionInfo partition : partitions) {
            executor.submit(() ->
                consumePartition(partition, batchConsumer)
            );
        }

        // 4. Ждем завершения всех задач
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
    }

    private void consumePartition(
        TopicPartition tp,
        Consumer<Collection<EtlRecord>> batchConsumer
    ) {
        // Читаем из партиции батчами
        List<EtlRecord> currentBatch = new ArrayList<>(streamBatchSize);

        while (true) {
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, Object> record : records) {
                currentBatch.add(convertToEtlRecord(record));

                // Когда батч заполнен - отправляем на обработку
                if (currentBatch.size() >= streamBatchSize) {
                    batchConsumer.accept(new ArrayList<>(currentBatch));
                    currentBatch.clear();
                }
            }

            if (records.isEmpty()) break;
        }

        // Отправляем остатки
        if (!currentBatch.isEmpty()) {
            batchConsumer.accept(currentBatch);
        }
    }
}
```

**Особенности:**
- Многопоточное чтение (4 потока по умолчанию)
- Каждая партиция обрабатывается независимо
- Фильтрация по timestamp (startTimestamp, endTimestamp)
- Поддержка Avro и String формата

##### JdbcExtractor

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/extractor/JdbcExtractor.java`

```java
@Component
public class JdbcExtractor implements Extractor {
    public String getType() { return "sql"; }

    public void extract(EtlJob job, Consumer<Collection<EtlRecord>> batchConsumer) {
        String source = job.getSource();

        // Определяем: это запрос или таблица
        boolean isQuery = source.trim().toUpperCase().startsWith("SELECT");

        if (isPartitioningEnabled(job)) {
            // Партицированное чтение
            extractPartitioned(job, batchConsumer);
        } else {
            // Простое последовательное чтение
            extractSequential(job, batchConsumer);
        }
    }

    private void extractPartitioned(
        EtlJob job,
        Consumer<Collection<EtlRecord>> batchConsumer
    ) {
        String partitionColumn = job.getParam("partitionColumn");
        int partitions = (int) job.getParam("partitions");

        // Создаем thread pool
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // Для каждой партиции запускаем задачу
        for (int i = 0; i < partitions; i++) {
            final int partition = i;
            executor.submit(() ->
                extractPartition(job, partition, partitions, batchConsumer)
            );
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
    }

    private void extractPartition(
        EtlJob job,
        int partition,
        int totalPartitions,
        Consumer<Collection<EtlRecord>> batchConsumer
    ) {
        // SELECT * FROM table WHERE partition_column % 72 = 0
        String sql = buildPartitionQuery(job, partition, totalPartitions);

        jdbcTemplate.query(sql, rs -> {
            List<EtlRecord> batch = new ArrayList<>(batchSize);

            while (rs.next()) {
                batch.add(convertRowToRecord(rs));

                if (batch.size() >= batchSize) {
                    batchConsumer.accept(new ArrayList<>(batch));
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                batchConsumer.accept(batch);
            }
        });
    }
}
```

**Особенности:**
- Партицирование по модулю: `WHERE column % partitions = partition_id`
- Многопоточное чтение партиций
- Поддержка как SELECT запросов, так и таблиц
- Batch чтение (ResultSet обрабатывается батчами)

---

### 2. Transformer

**Интерфейс:** `src/main/java/ru/pospelov/etl/engine/steps/transformer/Transformer.java`

```java
public interface Transformer {
    Collection<EtlRecord> transform(Collection<EtlRecord> records, EtlJob job);
    String getType();
}
```

#### Принцип работы:

Transformer получает батч записей и возвращает преобразованный батч:

```java
Collection<EtlRecord> input = [...];
Collection<EtlRecord> output = transformer.transform(input, job);
```

#### Реализации:

##### NoopTransformer

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/transformer/NoopTransformer.java`

```java
@Component
public class NoopTransformer implements Transformer {
    public String getType() { return "noop"; }

    public Collection<EtlRecord> transform(Collection<EtlRecord> records, EtlJob job) {
        // Просто возвращаем записи без изменений
        return records;
    }
}
```

**Использование:** Когда трансформация не требуется (SQL → SQL с тем же форматом)

##### AvroToRecordTransformer

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/transformer/AvroToRecordTransformer.java`

```java
@Component
public class AvroToRecordTransformer implements Transformer {
    public String getType() { return "avro"; }

    public Collection<EtlRecord> transform(Collection<EtlRecord> records, EtlJob job) {
        return records.stream()
            .map(this::flattenAvro)
            .toList();
    }

    private EtlRecord flattenAvro(EtlRecord record) {
        Object value = record.get("value");

        if (value instanceof GenericRecord avroRecord) {
            EtlRecord flattened = new EtlRecord(
                record.getTimestamp(),
                record.getSource(),
                record.getOffset()
            );

            // Извлекаем все поля из Avro
            for (Schema.Field field : avroRecord.getSchema().getFields()) {
                flattened.put(field.name(), avroRecord.get(field.name()));
            }

            return flattened;
        }

        return record;
    }
}
```

**Использование:** Kafka Avro → SQL (извлечение полей из Avro в плоские колонки)

##### RecordToAvroTransformer

**Использование:** SQL → Kafka Avro (упаковка полей в Avro схему)

---

### 3. Loader

**Интерфейс:** `src/main/java/ru/pospelov/etl/engine/steps/loader/Loader.java`

```java
public interface Loader {
    void load(Collection<EtlRecord> records, EtlJob job);
    String getType();
}
```

#### Принцип работы:

Loader получает батч и загружает его в целевую систему:

```java
Collection<EtlRecord> batch = [...];
loader.load(batch, job);  // Блокирующая операция
```

#### Реализации:

##### FastSqlServerLoader

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/loader/FastSqlServerLoader.java:36`

```java
@Component
public class FastSqlServerLoader implements Loader {
    public String getType() { return "fast-sql"; }

    public void load(Collection<EtlRecord> records, EtlJob job) {
        String targetTable = job.getTargetTable();
        List<EtlRecord> batch = records instanceof List
            ? (List<EtlRecord>) records
            : new ArrayList<>(records);

        bulkInsertBatch(targetTable, batch, job.getJobId());
    }

    private void bulkInsertBatch(
        String targetTable,
        List<EtlRecord> batch,
        String jobId
    ) {
        try (Connection conn = dataSource.getConnection()) {
            SQLServerConnection sqlConn = conn.unwrap(SQLServerConnection.class);

            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlConn)) {
                bulkCopy.setDestinationTableName(targetTable);

                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setBatchSize(batch.size());
                options.setTableLock(false);      // Параллельная запись!
                options.setCheckConstraints(false);
                options.setFireTriggers(false);
                options.setKeepNulls(true);

                bulkCopy.setBulkCopyOptions(options);

                // Маппинг колонок
                for (String column : batch.get(0).getAll().keySet()) {
                    bulkCopy.addColumnMapping(column, column);
                }

                // Bulk insert
                bulkCopy.writeToServer(new EtlBulkRecord(batch));
            }
        }
    }
}
```

**Особенности:**
- SQL Server Bulk Copy API - очень быстрая загрузка
- `setTableLock(false)` - позволяет параллельные вставки из разных потоков
- `setBatchSize(batch.size())` - оптимальный размер сетевого пакета
- Поддержка многопоточности (критично для Kafka партиций)

**Производительность:** ~50-200k записей/сек в зависимости от конфигурации

##### KafkaLoader

**Файл:** `src/main/java/ru/pospelov/etl/engine/steps/loader/KafkaLoader.java`

```java
@Component
public class KafkaLoader implements Loader {
    public String getType() { return "kafka"; }

    public void load(Collection<EtlRecord> records, EtlJob job) {
        KafkaProducer<String, Object> producer = getOrCreateProducer(job);
        String topic = (String) job.getParam("topic");

        List<Future<RecordMetadata>> futures = new ArrayList<>();

        // Асинхронная отправка
        for (EtlRecord record : records) {
            ProducerRecord<String, Object> kafkaRecord =
                new ProducerRecord<>(topic, record.get("value"));

            futures.add(producer.send(kafkaRecord));
        }

        // Ждем подтверждения
        for (Future<RecordMetadata> future : futures) {
            future.get();  // Блокируется до успешной отправки
        }
    }
}
```

**Особенности:**
- Переиспользование Producer (синглтон)
- Асинхронная отправка с ожиданием подтверждения
- Поддержка Avro schema registry

---

## Streaming архитектура

### StreamingEtlPipeline

**Файл:** `src/main/java/ru/pospelov/etl/engine/engine/EtlPipelineFactory.java:98`

Это сердце ETL Engine - здесь происходит оркестрация всех компонентов.

### Основной цикл выполнения:

```java
public void run(EtlJob job) {
    metrics.onJobStatusChanged(job, EtlJobStatus.RUNNING);

    final AtomicInteger batchCounter = new AtomicInteger(0);
    final AtomicInteger totalExtracted = new AtomicInteger(0);
    final AtomicInteger totalTransformed = new AtomicInteger(0);
    final AtomicInteger totalLoaded = new AtomicInteger(0);

    // ГЛАВНЫЙ ЦИКЛ: Extractor вызывает callback для каждого батча
    extractor.extract(job, extractedBatch -> {
        int batchNum = batchCounter.incrementAndGet();

        // 1. EXTRACT
        totalExtracted.addAndGet(extractedBatch.size());
        metrics.onExtractComplete(job, totalExtracted.get(), extractDuration);

        // 2. TRANSFORM
        Collection<EtlRecord> transformedBatch = transformBatch(job, extractedBatch);
        totalTransformed.addAndGet(transformedBatch.size());
        metrics.onTransformComplete(job, totalTransformed.get(), transformDuration);

        // 3. LOAD
        if (!transformedBatch.isEmpty()) {
            loadBatch(job, transformedBatch);
            totalLoaded.addAndGet(transformedBatch.size());
            metrics.onLoadComplete(job, totalLoaded.get(), loadDuration);
        }

        // Цикл повторяется для следующего батча
    });

    // После обработки всех батчей
    metrics.onJobStatusChanged(job, EtlJobStatus.COMPLETED);
}
```

### Ключевые особенности:

#### 1. Batch-by-Batch обработка

Каждый батч проходит **полный цикл** Extract → Transform → Load перед следующим батчем:

```
Batch 1: Extract → Transform → Load
Batch 2: Extract → Transform → Load
Batch 3: Extract → Transform → Load
...
```

**НЕ ТАК:**
```
❌ Extract all → Transform all → Load all  (весь датасет в памяти!)
```

#### 2. Эффективное использование памяти

В памяти находится только:
- Текущий батч в обработке
- Для многопоточных extractors: `threads × streamBatchSize` записей

Пример:
```
threads = 4
streamBatchSize = 100,000
Max Memory Usage = 4 × 100,000 = 400,000 записей
```

Для 1 миллиона записей вместо загрузки всех в память!

#### 3. Параллельная обработка в Extractor

```
Partition 0 Thread: [Extract Batch] → [Transform] → [Load]
Partition 1 Thread: [Extract Batch] → [Transform] → [Load]
Partition 2 Thread: [Extract Batch] → [Transform] → [Load]
Partition 3 Thread: [Extract Batch] → [Transform] → [Load]
```

Каждый поток **независимо** проходит Extract → Transform → Load.

**Важно:** Loader должен поддерживать параллельные вставки!
- `FastSqlServerLoader` с `setTableLock(false)` ✅
- `KafkaLoader` с thread-safe Producer ✅

#### 4. Оптимизация метрик

Для больших объемов метрики обновляются реже:

```java
// Обновляем метрики:
// - Всегда для первых 10 батчей (для тестов)
// - Каждые 10 батчей для больших объемов (оптимизация)
boolean shouldUpdateMetrics = (batchNum <= 10) || (batchNum % 10 == 0);

if (shouldUpdateMetrics) {
    metrics.onExtractComplete(job, totalExtracted.get(), 0);
}
```

Это снижает overhead на ~90% для больших датасетов.

---

## Жизненный цикл выполнения

### Полный поток от создания до завершения:

```
1. СОЗДАНИЕ JOB
   │
   ├─> EtlJob job = new EtlJob(jobId, source, target, params);
   │
   ▼
2. ВАЛИДАЦИЯ
   │
   ├─> jobValidator.validateOrThrow(job);
   │   ├─> Проверка обязательных параметров
   │   ├─> ExtractorValidator
   │   ├─> TransformerValidator
   │   └─> LoaderValidator
   │
   ▼
3. СОЗДАНИЕ PIPELINE
   │
   ├─> EtlPipeline pipeline = pipelineFactory.create(job);
   │   ├─> Получение компонентов из Registry
   │   │   ├─> Extractor extractor = registry.getExtractor("kafka");
   │   │   ├─> Transformer transformer = registry.getTransformer("avro");
   │   │   └─> Loader loader = registry.getLoader("fast-sql");
   │   │
   │   └─> new StreamingEtlPipeline(extractor, transformer, loader, ...);
   │
   ▼
4. ЗАПУСК
   │
   ├─> pipeline.run(job);
   │
   ▼
5. ИНИЦИАЛИЗАЦИЯ
   │
   ├─> metrics.onJobStatusChanged(job, RUNNING);
   ├─> metrics.onExtractStart(job);
   │
   ▼
6. EXTRACTION (параллельно для каждой партиции/потока)
   │
   ├─> Thread 1: consumePartition(partition0)
   │   ├─> while (hasRecords) {
   │   │       currentBatch.add(record);
   │   │       if (currentBatch.size() >= streamBatchSize) {
   │   │           batchConsumer.accept(currentBatch);  ──┐
   │   │       }                                          │
   │   │   }                                              │
   │   └─> batchConsumer.accept(remainingRecords);  ─────┤
   │                                                      │
   ├─> Thread 2: consumePartition(partition1)            │
   │   └─> batchConsumer.accept(batch);  ────────────────┤
   │                                                      │
   ├─> Thread 3: consumePartition(partition2)            │
   │   └─> batchConsumer.accept(batch);  ────────────────┤
   │                                                      │
   └─> Thread 4: consumePartition(partition3)            │
       └─> batchConsumer.accept(batch);  ────────────────┤
                                                          │
   ┌──────────────────────────────────────────────────────┘
   ▼
7. ОБРАБОТКА БАТЧА (вызывается для каждого батча)
   │
   ├─> batchConsumer.accept(extractedBatch) {
   │       │
   │       ├─> batchNum++;
   │       ├─> totalExtracted += batch.size();
   │       ├─> metrics.onExtractComplete(...);
   │       │
   │       ▼
   │   TRANSFORM
   │       │
   │       ├─> metrics.onTransformStart(job, batchSize);
   │       ├─> transformedBatch = transformer.transform(extractedBatch, job);
   │       ├─> totalTransformed += transformedBatch.size();
   │       └─> metrics.onTransformComplete(...);
   │       │
   │       ▼
   │   LOAD
   │       │
   │       ├─> metrics.onLoadStart(job, batchSize);
   │       ├─> loader.load(transformedBatch, job);
   │       ├─> totalLoaded += transformedBatch.size();
   │       └─> metrics.onLoadComplete(...);
   │   }
   │
   └─> (Цикл повторяется для каждого батча)
   │
   ▼
8. ЗАВЕРШЕНИЕ
   │
   ├─> Все потоки завершились
   ├─> metrics.onExtractComplete(job, totalExtracted, totalDuration);
   ├─> metrics.onTransformComplete(job, totalTransformed, 0);
   ├─> metrics.onLoadComplete(job, totalLoaded, 0);
   │
   ├─> metrics.onJobStatusChanged(job, COMPLETED);
   │
   └─> log.info("Job completed: extracted={}, loaded={}, throughput={} rec/sec",
                totalExtracted, totalLoaded, throughput);
```

### Статусы джоба:

```java
public enum EtlJobStatus {
    PENDING,        // Создан, но не запущен
    RUNNING,        // Инициализация
    EXTRACTING,     // Идет extraction
    TRANSFORMING,   // Идет transformation (первый батч)
    LOADING,        // Идет loading (первый батч)
    COMPLETED,      // Успешно завершен
    FAILED,         // Завершен с ошибкой
    CANCELLED       // Отменен пользователем
}
```

**Важно:** Статусы TRANSFORMING и LOADING выставляются только для первого батча, чтобы не создавать overhead. Для остальных батчей статус остается LOADING.

---

## Метрики и мониторинг

### EtlMetricsCollector

**Файл:** `src/main/java/ru/pospelov/etl/engine/metrics/EtlMetricsCollector.java`

Собирает метрики в реальном времени:

```java
@Component
public class EtlMetricsCollector implements EtlMetrics {

    // Метрики по каждому джобу
    private final ConcurrentMap<String, MutableJobMetrics> metricsByJob;

    // Подписчики на события (для UI, логирования и т.д.)
    private final CopyOnWriteArrayList<EtlMetrics> listeners;

    @Override
    public void onExtractComplete(EtlJob job, int extractedRecords, long durationMillis) {
        MutableJobMetrics metrics = getOrCreate(job);
        metrics.updateExtract(extractedRecords, durationMillis);

        // Уведомляем всех подписчиков
        publish(listener -> listener.onExtractComplete(job, extractedRecords, durationMillis));
    }

    // Аналогично для Transform и Load
}
```

### EtlJobMetricsSnapshot

**Файл:** `src/main/java/ru/pospelov/etl/engine/metrics/EtlJobMetricsSnapshot.java`

Immutable snapshot метрик:

```java
public record EtlJobMetricsSnapshot(
    String jobId,
    EtlJobStatus status,
    Instant startedAt,
    Instant lastUpdatedAt,
    long extractedRecords,
    long processedRecords,
    long transformedRecords,
    long loadedRecords,
    long errorCount,
    long extractDurationMillis,
    long transformDurationMillis,
    long loadDurationMillis,
    double throughput                    // records/sec
) {
    public Duration totalDuration() {
        // Total = Extract + Transform + Load
        long totalMillis = extractDurationMillis +
                          transformDurationMillis +
                          loadDurationMillis;
        return Duration.ofMillis(totalMillis);
    }
}
```

### Получение метрик:

```java
// Метрики конкретного джоба
Optional<EtlJobMetricsSnapshot> snapshot = metricsCollector.getSnapshot("job-001");

if (snapshot.isPresent()) {
    EtlJobMetricsSnapshot metrics = snapshot.get();
    System.out.println("Status: " + metrics.status());
    System.out.println("Loaded: " + metrics.loadedRecords());
    System.out.println("Throughput: " + metrics.throughput() + " rec/sec");
    System.out.println("Total Duration: " + metrics.totalDuration());
}

// Метрики всех джобов
Map<String, EtlJobMetricsSnapshot> allMetrics = metricsCollector.snapshotAll();
```

### Расчет Throughput:

```java
private double computeThroughput() {
    long totalDurationMillis = extractDurationMillis +
                              transformDurationMillis +
                              loadDurationMillis;

    if (totalDurationMillis <= 0) return 0d;

    double seconds = totalDurationMillis / 1000d;
    return loadedRecords / seconds;
}
```

**Важно:** Throughput считается от **реального времени обработки** (суммы длительностей), а не от wall-clock time между startedAt и lastUpdatedAt.

---

## Реализованные цепочки

### 1. Kafka → SQL Server (Avro)

**Use Case:** Загрузка событий из Kafka в аналитическую базу данных

```java
EtlJob job = new EtlJob(
    "kafka-to-sql",
    null,
    "SUPPORT.dbo.orders",
    Map.of(
        "extractorType", "kafka",
        "transformerType", "avro",      // Avro → Flat columns
        "loaderType", "fast-sql",

        "topic", "order-events",
        "format", "avro",
        "startTimestamp", startTime.toEpochMilli(),
        "endTimestamp", endTime.toEpochMilli(),
        "streamBatchSize", 100_000,
        "threads", 4
    )
);
```

**Поток данных:**
```
Kafka Topic (Avro)
    → KafkaExtractor (4 threads × 72 partitions)
    → AvroToRecordTransformer (GenericRecord → flat columns)
    → FastSqlServerLoader (SQL Server Bulk Copy)
    → SQL Server Table
```

**Производительность:** 50-60k rec/sec для 1M записей

**Особенности:**
- Параллельное чтение всех партиций Kafka
- Параллельная загрузка в SQL (setTableLock=false)
- Automatic schema extraction из Avro

---

### 2. SQL Server → Kafka (Avro)

**Use Case:** CDC (Change Data Capture), репликация данных в event stream

```java
EtlJob job = new EtlJob(
    "sql-to-kafka",
    "SELECT * FROM SUPPORT.dbo.orders",
    null,
    Map.of(
        "extractorType", "sql",
        "transformerType", "noop",       // Без трансформации
        "loaderType", "kafka",

        "partitionColumn", "bucket",
        "partitions", 72,
        "threads", 4,

        "topic", "order-events",
        "streamBatchSize", 100_000
    )
);
```

**Поток данных:**
```
SQL Server Table
    → JdbcExtractor (4 threads × 72 partitions)
    → NoopTransformer (pass-through)
    → KafkaLoader (Avro Producer)
    → Kafka Topic (Avro)
```

**Производительность:** 150-170k rec/sec для 1M записей

**Особенности:**
- Партицирование по модулю: `WHERE bucket % 72 = partition_id`
- Параллельное чтение SQL
- Асинхронная отправка в Kafka
- Automatic schema registration в Schema Registry

---

### 3. SQL Server → SQL Server (партиционированная копия)

**Use Case:** Быстрое копирование больших таблиц, партицирование данных

```java
EtlJob job = new EtlJob(
    "sql-to-sql",
    "SUPPORT.dbo.orders_source",
    "SUPPORT.dbo.orders_target",
    Map.of(
        "extractorType", "sql",
        "transformerType", "noop",
        "loaderType", "fast-sql",

        "partitionColumn", "id",
        "partitions", 8,
        "threads", 4,
        "streamBatchSize", 50_000
    )
);
```

**Поток данных:**
```
SQL Table (source)
    → JdbcExtractor (partitioned read)
    → NoopTransformer
    → FastSqlServerLoader (Bulk Copy)
    → SQL Table (target)
```

**Производительность:** 200k+ rec/sec

---

### 4. Kafka → SQL Server (String format)

**Use Case:** Загрузка JSON событий из Kafka

```java
EtlJob job = new EtlJob(
    "kafka-json-to-sql",
    null,
    "SUPPORT.dbo.events",
    Map.of(
        "extractorType", "kafka",
        "transformerType", "noop",
        "loaderType", "fast-sql",

        "topic", "json-events",
        "format", "string",              // String вместо Avro
        "startTimestamp", startTime.toEpochMilli(),
        "endTimestamp", endTime.toEpochMilli(),
        "streamBatchSize", 50_000,
        "threads", 4
    )
);
```

**Поток данных:**
```
Kafka Topic (JSON String)
    → KafkaExtractor (String deserializer)
    → NoopTransformer
    → FastSqlServerLoader
    → SQL Server Table
```

---

## Примеры использования

### Пример 1: Простой Kafka → SQL

```java
@Service
public class EtlService {

    @Autowired
    private EtlPipelineFactory pipelineFactory;

    public void loadKafkaToSql(Instant start, Instant end) {
        EtlJob job = new EtlJob(
            "daily-orders-" + LocalDate.now(),
            null,
            "analytics.dbo.daily_orders",
            Map.of(
                "extractorType", "kafka",
                "transformerType", "avro",
                "loaderType", "fast-sql",
                "topic", "order-events",
                "format", "avro",
                "startTimestamp", start.toEpochMilli(),
                "endTimestamp", end.toEpochMilli(),
                "streamBatchSize", 100_000,
                "threads", 4
            )
        );

        EtlPipeline pipeline = pipelineFactory.create(job);
        pipeline.run(job);

        // Получаем метрики
        Optional<EtlJobMetricsSnapshot> metrics =
            metricsCollector.getSnapshot(job.getJobId());

        metrics.ifPresent(m -> {
            log.info("Job completed: loaded {} records in {}, throughput: {} rec/sec",
                m.loadedRecords(),
                m.totalDuration(),
                m.throughput()
            );
        });
    }
}
```

### Пример 2: Scheduled job с мониторингом

```java
@Component
public class DailyEtlJob {

    @Autowired
    private EtlPipelineFactory pipelineFactory;

    @Autowired
    private EtlMetricsCollector metricsCollector;

    @Scheduled(cron = "0 0 1 * * *")  // Каждый день в 01:00
    public void runDailyEtl() {
        Instant yesterday = Instant.now().minus(1, ChronoUnit.DAYS);
        Instant today = Instant.now();

        EtlJob job = new EtlJob(
            "daily-etl-" + LocalDate.now(),
            null,
            "analytics.dbo.daily_orders",
            Map.of(
                "extractorType", "kafka",
                "transformerType", "avro",
                "loaderType", "fast-sql",
                "topic", "order-events",
                "format", "avro",
                "startTimestamp", yesterday.toEpochMilli(),
                "endTimestamp", today.toEpochMilli(),
                "streamBatchSize", 100_000,
                "threads", 4
            )
        );

        // Подписываемся на метрики для мониторинга
        metricsCollector.registerListener(new EtlMetrics() {
            @Override
            public void onExtractComplete(EtlJob j, int records, long duration) {
                log.info("Extracted {} records", records);
            }

            @Override
            public void onLoadComplete(EtlJob j, int records, long duration) {
                log.info("Loaded {} records in {}ms", records, duration);
            }

            @Override
            public void onJobStatusChanged(EtlJob j, EtlJobStatus status) {
                if (status == EtlJobStatus.FAILED) {
                    sendAlert("ETL job failed: " + j.getJobId());
                }
            }
        });

        try {
            EtlPipeline pipeline = pipelineFactory.create(job);
            pipeline.run(job);

            log.info("Daily ETL completed successfully");
        } catch (Exception e) {
            log.error("Daily ETL failed", e);
            sendAlert("Daily ETL failed: " + e.getMessage());
        }
    }

    private void sendAlert(String message) {
        // Отправка алерта в Slack, email и т.д.
    }
}
```

### Пример 3: REST API для запуска ETL

```java
@RestController
@RequestMapping("/api/etl")
public class EtlController {

    @Autowired
    private EtlPipelineFactory pipelineFactory;

    @Autowired
    private EtlMetricsCollector metricsCollector;

    @Autowired
    private ExecutorService etlExecutor;  // Background execution

    @PostMapping("/jobs")
    public ResponseEntity<JobResponse> createJob(@RequestBody JobRequest request) {
        EtlJob job = new EtlJob(
            UUID.randomUUID().toString(),
            request.getSource(),
            request.getTarget(),
            request.getParameters()
        );

        // Запускаем асинхронно
        etlExecutor.submit(() -> {
            try {
                EtlPipeline pipeline = pipelineFactory.create(job);
                pipeline.run(job);
            } catch (Exception e) {
                log.error("Job {} failed", job.getJobId(), e);
            }
        });

        return ResponseEntity.ok(new JobResponse(job.getJobId(), "RUNNING"));
    }

    @GetMapping("/jobs/{jobId}/metrics")
    public ResponseEntity<EtlJobMetricsSnapshot> getMetrics(@PathVariable String jobId) {
        return metricsCollector.getSnapshot(jobId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/jobs")
    public Map<String, EtlJobMetricsSnapshot> getAllJobs() {
        return metricsCollector.snapshotAll();
    }
}
```

---

## Производительность

### Benchmark результаты (1M записей):

| Цепочка | Throughput | Total Duration | Примечания |
|---------|-----------|----------------|------------|
| **Kafka → SQL** (Avro) | 50-60k rec/sec | ~18-20 сек | 72 партиции, 4 потока |
| **SQL → Kafka** (Avro) | 150-170k rec/sec | ~6 сек | 72 партиции, 4 потока |
| **SQL → SQL** | 200k+ rec/sec | ~5 сек | Bulk Copy, партицирование |

### Факторы производительности:

1. **Параллелизм:**
   - Больше потоков = выше throughput (до CPU limit)
   - Больше партиций = лучше распределение

2. **Batch Size:**
   - Оптимальный: 50k-100k
   - Меньше = больше overhead
   - Больше = риск out of memory

3. **Network:**
   - SQL Server Bulk Copy очень эффективен для сети
   - Kafka Producer batching

4. **Database:**
   - `setTableLock(false)` критично для параллельных вставок
   - Отключение constraints/triggers ускоряет загрузку

---

## Best Practices

### 1. Размер батча

```java
// Для Kafka с множеством партиций
"streamBatchSize", 50_000   // Меньше, т.к. данные распределены

// Для SQL с большими таблицами
"streamBatchSize", 100_000  // Больше для лучшего throughput
```

### 2. Количество потоков

```java
// Оптимально: количество CPU cores
int threads = Runtime.getRuntime().availableProcessors();

// Для I/O bound задач можно больше
int threads = cores * 2;
```

### 3. Партицирование

```java
// Для равномерного распределения
"partitions", 72            // Кратно количеству потоков (4 * 18 = 72)
"partitionColumn", "hash_column"  // Колонка с хорошим распределением
```

### 4. Обработка ошибок

```java
try {
    pipeline.run(job);
} catch (ExtractionException e) {
    log.error("Failed to extract from source", e);
    // Retry logic
} catch (TransformationException e) {
    log.error("Failed to transform records", e);
    // Check data quality
} catch (LoadingException e) {
    log.error("Failed to load to target", e);
    // Check target availability
}
```

### 5. Мониторинг

```java
// Real-time monitoring
metricsCollector.registerListener(new EtlMetrics() {
    @Override
    public void onLoadComplete(EtlJob job, int records, long duration) {
        double throughput = records / (duration / 1000.0);

        if (throughput < THRESHOLD) {
            log.warn("Low throughput detected: {} rec/sec", throughput);
        }
    }
});
```

---

## Troubleshooting

### Проблема: Низкий throughput

**Решения:**
1. Увеличить `threads`
2. Увеличить `streamBatchSize`
3. Проверить `setTableLock(false)` для SQL
4. Проверить сетевую задержку

### Проблема: Out of Memory

**Решения:**
1. Уменьшить `streamBatchSize`
2. Уменьшить `threads`
3. Увеличить heap: `-Xmx4g`

### Проблема: Deadlocks в SQL

**Решения:**
1. Убедиться что `setTableLock(false)`
2. Проверить индексы на целевой таблице
3. Уменьшить количество потоков

---

## Заключение

ETL Engine предоставляет мощную и гибкую архитектуру для построения high-performance ETL pipelines с поддержкой:

- ✅ Streaming обработки (batch-by-batch)
- ✅ Параллелизма (многопоточность)
- ✅ Компонентной архитектуры (Extractor, Transformer, Loader)
- ✅ Реального времени метрик
- ✅ Высокой производительности (50-200k rec/sec)

Следуя best practices и используя правильные конфигурации, можно достичь оптимальной производительности для любых ETL задач.
