# Clean Slate Refactoring: Final Architecture

## Контекст

Проект еще **НЕ в production**, что означает:
- ✅ Нет legacy данных для миграции
- ✅ Можно менять схему БД свободно
- ✅ Можно делать breaking changes в API
- ✅ Можно использовать все фичи Java 21
- ✅ Можно выбрать наиболее правильное решение

**Версия Java:** 21
**Spring Boot:** 3.5.0

---

## Принятые архитектурные решения

| Вопрос | Решение | Обоснование |
|--------|---------|-------------|
| **Defaults** | ❌ НЕТ defaults в коде. Пользователь заполняет всё. | Явность лучше неявности. UI показывает рекомендации. |
| **Nullable поля** | ✅ `Optional<T>` для опциональных полей | Type-safe null handling, явный контракт API |
| **Схема БД** | ✅ Раздельные колонки для типов и конфигураций | Возможность SQL запросов, индексы по типам |
| **Pattern matching** | ✅ Централизованный Factory | Exhaustive checks, один entry point |
| **Validation** | ✅ Только Bean Validation | Декларативно, без custom логики в constructors |

---

## Архитектура: Sealed Interfaces + Records + Pattern Matching

### Почему это лучшее решение?

1. **Максимальная type safety** - компилятор проверяет всё
2. **Exhaustive pattern matching** - невозможно забыть обработать случай
3. **Immutability by default** - Records неизменяемы
4. **Минимум boilerplate** - Records автогенерируют equals/hashCode/toString
5. **Явный контракт** - sealed interfaces закрывают список реализаций
6. **Optional для nullable** - explicit null handling
7. **Нет рефлексии в runtime** - всё проверяется на compile-time

---

## 1. Конфигурации компонентов

### Extractors

```java
// src/main/java/ru/pospelov/etl/engine/config/extractor/ExtractorConfig.java
package ru.pospelov.etl.engine.config.extractor;

/**
 * Базовая конфигурация для всех экстракторов.
 * Sealed - можем добавлять новые типы только явно.
 */
public sealed interface ExtractorConfig
    permits JdbcExtractorConfig, KafkaExtractorConfig {
    String type();
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/extractor/JdbcExtractorConfig.java
package ru.pospelov.etl.engine.config.extractor;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

/**
 * Конфигурация JDBC экстрактора.
 *
 * Все поля обязательны (NotNull), кроме Optional полей.
 * НЕТ defaults - пользователь явно указывает все значения.
 */
public record JdbcExtractorConfig(
    @NotBlank(message = "SQL query is required")
    String sqlQuery,

    Optional<String> partitionColumn,

    @NotNull(message = "Partitions is required")
    @Min(value = 1, message = "Partitions must be >= 1")
    Integer partitions,

    Optional<String> keyColumn,

    @NotNull(message = "Threads is required")
    @Min(value = 1, message = "Threads must be >= 1")
    Integer threads,

    @NotNull(message = "Stream batch size is required")
    @Min(value = 100, message = "Stream batch size must be >= 100")
    @Max(value = 1_000_000, message = "Stream batch size must be <= 1,000,000")
    Integer streamBatchSize
) implements ExtractorConfig {

    @Override
    public String type() {
        return "sql";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/extractor/KafkaExtractorConfig.java
package ru.pospelov.etl.engine.config.extractor;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.pospelov.etl.engine.config.KafkaFormat;

/**
 * Конфигурация Kafka экстрактора.
 */
public record KafkaExtractorConfig(
    @NotBlank(message = "Kafka topic is required")
    String topic,

    @NotNull(message = "Start timestamp is required")
    Long startTimestamp,

    @NotNull(message = "End timestamp is required")
    Long endTimestamp,

    @NotNull(message = "Format is required")
    KafkaFormat format,

    @NotNull(message = "Threads is required")
    @Min(value = 1, message = "Threads must be >= 1")
    Integer threads,

    @NotNull(message = "Stream batch size is required")
    @Min(value = 100, message = "Stream batch size must be >= 100")
    @Max(value = 1_000_000, message = "Stream batch size must be <= 1,000,000")
    Integer streamBatchSize
) implements ExtractorConfig {

    @Override
    public String type() {
        return "kafka";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/KafkaFormat.java
package ru.pospelov.etl.engine.config;

/**
 * Формат данных в Kafka - type-safe enum вместо String.
 */
public enum KafkaFormat {
    AVRO,
    JSON,
    STRING
}
```

### Transformers

```java
// src/main/java/ru/pospelov/etl/engine/config/transformer/TransformerConfig.java
package ru.pospelov.etl.engine.config.transformer;

public sealed interface TransformerConfig
    permits NoopTransformerConfig, AvroToRecordTransformerConfig, RecordToAvroTransformerConfig {
    String type();
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/transformer/NoopTransformerConfig.java
package ru.pospelov.etl.engine.config.transformer;

public record NoopTransformerConfig() implements TransformerConfig {
    @Override
    public String type() {
        return "noop";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/transformer/AvroToRecordTransformerConfig.java
package ru.pospelov.etl.engine.config.transformer;

public record AvroToRecordTransformerConfig() implements TransformerConfig {
    @Override
    public String type() {
        return "avro";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/transformer/RecordToAvroTransformerConfig.java
package ru.pospelov.etl.engine.config.transformer;

import jakarta.validation.constraints.NotBlank;

public record RecordToAvroTransformerConfig(
    @NotBlank(message = "Avro schema subject is required")
    String avroSchemaSubject
) implements TransformerConfig {
    @Override
    public String type() {
        return "record-to-avro";
    }
}
```

### Loaders

```java
// src/main/java/ru/pospelov/etl/engine/config/loader/LoaderConfig.java
package ru.pospelov.etl.engine.config.loader;

public sealed interface LoaderConfig
    permits JdbcLoaderConfig, FastSqlLoaderConfig, KafkaLoaderConfig {
    String type();
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/loader/JdbcLoaderConfig.java
package ru.pospelov.etl.engine.config.loader;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record JdbcLoaderConfig(
    @NotBlank(message = "Target table is required")
    String targetTable,

    @NotNull(message = "Stream batch size is required")
    @Min(value = 100, message = "Stream batch size must be >= 100")
    @Max(value = 1_000_000, message = "Stream batch size must be <= 1,000,000")
    Integer streamBatchSize
) implements LoaderConfig {
    @Override
    public String type() {
        return "sql";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/loader/FastSqlLoaderConfig.java
package ru.pospelov.etl.engine.config.loader;

import jakarta.validation.constraints.NotBlank;

public record FastSqlLoaderConfig(
    @NotBlank(message = "Target table is required")
    String targetTable
) implements LoaderConfig {
    @Override
    public String type() {
        return "fast-sql";
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/config/loader/KafkaLoaderConfig.java
package ru.pospelov.etl.engine.config.loader;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.pospelov.etl.engine.config.KafkaFormat;

import java.util.Optional;

public record KafkaLoaderConfig(
    @NotBlank(message = "Kafka topic is required")
    String topic,

    @NotNull(message = "Format is required")
    KafkaFormat format,

    Optional<String> avroSchemaSubject
) implements LoaderConfig {
    @Override
    public String type() {
        return "kafka";
    }
}
```

---

## 2. Модель EtlJob - максимально упрощена

```java
// src/main/java/ru/pospelov/etl/engine/model/EtlJob.java
package ru.pospelov.etl.engine.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import ru.pospelov.etl.engine.config.extractor.*;
import ru.pospelov.etl.engine.config.transformer.*;
import ru.pospelov.etl.engine.config.loader.*;

/**
 * ETL Job - полностью type-safe конфигурация.
 *
 * УБРАНО:
 * - Map<String, Object> parameters ❌
 * - String source ❌
 * - String targetTable ❌
 * - getParamOrDefault() ❌
 *
 * ДОБАВЛЕНО:
 * - Type-safe конфигурации ✅
 * - Jackson полиморфная сериализация ✅
 * - Bean Validation ✅
 * - Optional для nullable полей ✅
 */
public record EtlJob(
    @NotBlank(message = "Job ID is required")
    String jobId,

    @Valid
    @NotNull(message = "Extractor configuration is required")
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "extractorType")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = JdbcExtractorConfig.class, name = "sql"),
        @JsonSubTypes.Type(value = KafkaExtractorConfig.class, name = "kafka")
    })
    ExtractorConfig extractorConfig,

    @Valid
    @NotNull(message = "Transformer configuration is required")
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "transformerType")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = NoopTransformerConfig.class, name = "noop"),
        @JsonSubTypes.Type(value = AvroToRecordTransformerConfig.class, name = "avro"),
        @JsonSubTypes.Type(value = RecordToAvroTransformerConfig.class, name = "record-to-avro")
    })
    TransformerConfig transformerConfig,

    @Valid
    @NotNull(message = "Loader configuration is required")
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "loaderType")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = JdbcLoaderConfig.class, name = "sql"),
        @JsonSubTypes.Type(value = FastSqlLoaderConfig.class, name = "fast-sql"),
        @JsonSubTypes.Type(value = KafkaLoaderConfig.class, name = "kafka")
    })
    LoaderConfig loaderConfig
) {
    // Record - никаких методов не нужно!
}
```

**Пример JSON:**
```json
{
  "jobId": "orders-sql-to-kafka",
  "extractorType": "sql",
  "extractorConfig": {
    "sqlQuery": "SELECT * FROM orders WHERE created_at > '2024-01-01'",
    "partitionColumn": null,
    "partitions": 1,
    "keyColumn": "order_id",
    "threads": 8,
    "streamBatchSize": 100000
  },
  "transformerType": "record-to-avro",
  "transformerConfig": {
    "avroSchemaSubject": "order-events-value"
  },
  "loaderType": "kafka",
  "loaderConfig": {
    "topic": "orders-topic",
    "format": "AVRO",
    "avroSchemaSubject": "order-events-value"
  }
}
```

---

## 3. Схема БД - раздельные колонки

```sql
-- Flyway migration: V2__refactor_job_schema.sql

-- Удаляем старые колонки
ALTER TABLE SUPPORT.service.tEtlJob
DROP COLUMN Source;

ALTER TABLE SUPPORT.service.tEtlJob
DROP COLUMN Target;

ALTER TABLE SUPPORT.service.tEtlJob
DROP COLUMN Params;

-- Добавляем новые колонки для раздельного хранения
ALTER TABLE SUPPORT.service.tEtlJob
ADD ExtractorType VARCHAR(50) NOT NULL;

ALTER TABLE SUPPORT.service.tEtlJob
ADD ExtractorConfig NVARCHAR(MAX) NOT NULL;

ALTER TABLE SUPPORT.service.tEtlJob
ADD TransformerType VARCHAR(50) NOT NULL;

ALTER TABLE SUPPORT.service.tEtlJob
ADD TransformerConfig NVARCHAR(MAX) NOT NULL;

ALTER TABLE SUPPORT.service.tEtlJob
ADD LoaderType VARCHAR(50) NOT NULL;

ALTER TABLE SUPPORT.service.tEtlJob
ADD LoaderConfig NVARCHAR(MAX) NOT NULL;

-- Индексы для быстрого поиска по типам
CREATE INDEX IX_tEtlJob_ExtractorType ON SUPPORT.service.tEtlJob(ExtractorType);
CREATE INDEX IX_tEtlJob_LoaderType ON SUPPORT.service.tEtlJob(LoaderType);
```

**Обновленная JPA Entity:**
```java
// src/main/java/ru/pospelov/etl/engine/api/entity/JobEntity.java
package ru.pospelov.etl.engine.api.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "tEtlJob", schema = "service", catalog = "SUPPORT", indexes = {
    @Index(name = "IX_tEtlJob_ExtractorType", columnList = "ExtractorType"),
    @Index(name = "IX_tEtlJob_LoaderType", columnList = "LoaderType"),
    @Index(name = "IX_tEtlJob_Status", columnList = "Status"),
    @Index(name = "IX_tEtlJob_CreatedAt", columnList = "CreatedAt")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobEntity {

    @Id
    @Column(name = "Id", nullable = false, length = 255)
    private String id;

    @Column(name = "Name", length = 500)
    private String name;

    @Column(name = "ExtractorType", nullable = false, length = 50)
    private String extractorType;

    @Column(name = "ExtractorConfig", nullable = false, columnDefinition = "NVARCHAR(MAX)")
    private String extractorConfig;

    @Column(name = "TransformerType", nullable = false, length = 50)
    private String transformerType;

    @Column(name = "TransformerConfig", nullable = false, columnDefinition = "NVARCHAR(MAX)")
    private String transformerConfig;

    @Column(name = "LoaderType", nullable = false, length = 50)
    private String loaderType;

    @Column(name = "LoaderConfig", nullable = false, columnDefinition = "NVARCHAR(MAX)")
    private String loaderConfig;

    @Column(name = "CreatedAt", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "UpdatedAt", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "CreatedBy", length = 255)
    private String createdBy;

    @Column(name = "Status", nullable = false, length = 50)
    private String status;

    @Column(name = "Description", columnDefinition = "NVARCHAR(MAX)")
    private String description;

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        createdAt = now;
        updatedAt = now;
        if (status == null || status.trim().isEmpty()) {
            status = "ACTIVE";
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
```

**Преимущества раздельных колонок:**
- ✅ SQL запросы: `WHERE ExtractorType = 'sql'`
- ✅ Индексы по типам компонентов
- ✅ Статистика: `SELECT ExtractorType, COUNT(*) FROM ... GROUP BY ExtractorType`
- ✅ Возможность миграции только конкретного типа конфигурации

---

## 4. Централизованный Factory с Pattern Matching

```java
// src/main/java/ru/pospelov/etl/engine/engine/EtlComponentFactory.java
package ru.pospelov.etl.engine.engine;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.extractor.*;
import ru.pospelov.etl.engine.config.transformer.*;
import ru.pospelov.etl.engine.config.loader.*;
import ru.pospelov.etl.engine.model.EtlJob;
import ru.pospelov.etl.engine.model.EtlRecord;
import ru.pospelov.etl.engine.steps.extractor.*;
import ru.pospelov.etl.engine.steps.transformer.*;
import ru.pospelov.etl.engine.steps.loader.*;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Централизованная фабрика для создания и запуска ETL компонентов.
 * Использует exhaustive pattern matching для type-safe диспетчеризации.
 */
@Component
@RequiredArgsConstructor
public class EtlComponentFactory {

    private final JdbcExtractor jdbcExtractor;
    private final KafkaPartitionExtractor kafkaExtractor;

    private final NoopTransformer noopTransformer;
    private final AvroToRecordTransformer avroToRecordTransformer;
    private final RecordToAvroTransformer recordToAvroTransformer;

    private final JdbcLoader jdbcLoader;
    private final FastSqlServerLoader fastSqlLoader;
    private final KafkaLoader kafkaLoader;

    /**
     * Извлечь данные используя конфигурацию extractor.
     * Pattern matching гарантирует обработку всех типов.
     */
    public void extract(EtlJob job, Consumer<Collection<EtlRecord>> batchConsumer) {
        switch (job.extractorConfig()) {
            case JdbcExtractorConfig config ->
                jdbcExtractor.extract(config, job.jobId(), batchConsumer);

            case KafkaExtractorConfig config ->
                kafkaExtractor.extract(config, job.jobId(), batchConsumer);

            // Компилятор проверяет exhaustiveness!
            // Если добавить новый sealed тип - код не скомпилируется без обработки
        }
    }

    /**
     * Трансформировать данные используя конфигурацию transformer.
     */
    public Collection<EtlRecord> transform(EtlJob job, Collection<EtlRecord> records) {
        return switch (job.transformerConfig()) {
            case NoopTransformerConfig config ->
                noopTransformer.transform(records);

            case AvroToRecordTransformerConfig config ->
                avroToRecordTransformer.transform(records);

            case RecordToAvroTransformerConfig config ->
                recordToAvroTransformer.transform(records, config);
        };
    }

    /**
     * Загрузить данные используя конфигурацию loader.
     */
    public void load(EtlJob job, Collection<EtlRecord> records) {
        switch (job.loaderConfig()) {
            case JdbcLoaderConfig config ->
                jdbcLoader.load(config, job.jobId(), records);

            case FastSqlLoaderConfig config ->
                fastSqlLoader.load(config, job.jobId(), records);

            case KafkaLoaderConfig config ->
                kafkaLoader.load(config, job.jobId(), records);
        }
    }
}
```

**Преимущества:**
- ✅ Exhaustive check - компилятор проверяет все случаи
- ✅ Единая точка входа для всех компонентов
- ✅ Легко добавить логирование/метрики
- ✅ Убрали логику выбора компонента из самих компонентов

---

## 5. Обновленные компоненты

### JdbcExtractor

```java
// src/main/java/ru/pospelov/etl/engine/steps/extractor/jdbc/JdbcExtractor.java
package ru.pospelov.etl.engine.steps.extractor.jdbc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.extractor.JdbcExtractorConfig;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Slf4j
@Component
@RequiredArgsConstructor
public class JdbcExtractor {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Извлечение данных из JDBC источника.
     * Все параметры type-safe и NOT NULL (кроме Optional).
     * НЕТ getParamOrDefault - всё явно указано пользователем.
     */
    public void extract(
        JdbcExtractorConfig config,
        String jobId,
        Consumer<Collection<EtlRecord>> batchConsumer
    ) {
        // ✅ Type-safe доступ к полям
        String query = config.sqlQuery();
        int threads = config.threads();
        int batchSize = config.streamBatchSize();
        int partitions = config.partitions();

        // ✅ Optional - явная обработка nullable
        Optional<String> partitionColumn = config.partitionColumn();
        Optional<String> keyColumn = config.keyColumn();

        log.info("Job '{}' extraction started: query={}, threads={}, batchSize={}, partitions={}",
                jobId, query, threads, batchSize, partitions);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<?>> tasks = new ArrayList<>();

        try {
            // Партиционирование только если указан partition column
            if (partitionColumn.isPresent() && partitions > 1) {
                log.info("Job '{}' using partition-based extraction: column={}, partitions={}",
                        jobId, partitionColumn.get(), partitions);

                for (int p = 0; p < partitions; p++) {
                    String partQuery = query +
                        (query.toLowerCase().contains("where") ? " AND " : " WHERE ") +
                        partitionColumn.get() + " = " + p;

                    tasks.add(executor.submit(new JdbcPartitionQueryTask(
                        jdbcTemplate,
                        partQuery,
                        p,
                        batchSize,
                        keyColumn.orElse(null),
                        batchConsumer
                    )));
                }
            } else {
                // Offset-based extraction
                Integer totalRows = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM (" + query + ") t",
                    Integer.class
                );
                int total = (totalRows == null) ? 0 : totalRows;

                log.info("Job '{}' using offset-based extraction: totalRows={}, tasks={}",
                        jobId, total, (total + batchSize - 1) / batchSize);

                for (int offset = 0; offset < total; offset += batchSize) {
                    tasks.add(executor.submit(new JdbcOffsetQueryTask(
                        jdbcTemplate,
                        query,
                        offset,
                        batchSize,
                        keyColumn.orElse(null),
                        batchConsumer
                    )));
                }
            }

            waitForTasks(tasks, jobId);
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                throw new ExtractionException("JdbcExtractor timeout", jobId);
            }

            log.info("Job '{}' extraction completed: {} tasks finished", jobId, tasks.size());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ExtractionException("JdbcExtractor interrupted", jobId, e);
        } catch (Exception e) {
            throw new ExtractionException("JdbcExtractor failed", jobId, e);
        } finally {
            executor.shutdownNow();
        }
    }

    private static void waitForTasks(List<Future<?>> tasks, String jobId) {
        // ... (без изменений)
    }
}
```

### KafkaPartitionExtractor

```java
// src/main/java/ru/pospelov/etl/engine/steps/extractor/kafka/KafkaPartitionExtractor.java
package ru.pospelov.etl.engine.steps.extractor.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.pospelov.etl.engine.config.KafkaFormat;
import ru.pospelov.etl.engine.config.extractor.KafkaExtractorConfig;
import ru.pospelov.etl.engine.config.KafkaClientFactory;
import ru.pospelov.etl.engine.exception.ExtractionException;
import ru.pospelov.etl.engine.model.EtlRecord;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaPartitionExtractor {

    private final KafkaClientFactory consumerFactory;

    public void extract(
        KafkaExtractorConfig config,
        String jobId,
        Consumer<Collection<EtlRecord>> batchConsumer
    ) {
        // ✅ Type-safe доступ
        String topic = config.topic();
        long startMillis = config.startTimestamp();
        long endMillis = config.endTimestamp();
        int threads = config.threads();
        int streamBatchSize = config.streamBatchSize();
        KafkaFormat format = config.format();

        boolean isAvro = (format == KafkaFormat.AVRO);

        log.info("Job '{}' Kafka extraction started: topic={}, start={}, end={}, threads={}, format={}",
                jobId, topic, Instant.ofEpochMilli(startMillis), Instant.ofEpochMilli(endMillis),
                threads, format);

        // ... остальная логика без изменений
    }
}
```

---

## 6. API Controller - автоматическая валидация

```java
// src/main/java/ru/pospelov/etl/engine/api/controller/JobController.java
package ru.pospelov.etl.engine.api.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.pospelov.etl.engine.api.repository.JobRepository;
import ru.pospelov.etl.engine.model.EtlJob;

import java.util.List;

@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobRepository jobRepository;

    /**
     * Создание job с автоматической валидацией.
     * Spring автоматически:
     * 1. Десериализует JSON в EtlJob record
     * 2. Выполняет Bean Validation (@Valid)
     * 3. Возвращает 400 Bad Request с детальными ошибками
     */
    @PostMapping
    public ResponseEntity<EtlJob> createJob(@Valid @RequestBody EtlJob job) {
        // ✅ Никакой валидации не нужно - если дошли сюда, job валидный
        EtlJob saved = jobRepository.save(job);
        return ResponseEntity.ok(saved);
    }

    @PutMapping("/{id}")
    public ResponseEntity<EtlJob> updateJob(
        @PathVariable String id,
        @Valid @RequestBody EtlJob job
    ) {
        if (!id.equals(job.jobId())) {
            return ResponseEntity.badRequest().build();
        }
        EtlJob updated = jobRepository.save(job);
        return ResponseEntity.ok(updated);
    }

    @GetMapping("/{id}")
    public ResponseEntity<EtlJob> getJob(@PathVariable String id) {
        return jobRepository.findById(id)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<EtlJob>> getAllJobs() {
        return ResponseEntity.ok(jobRepository.findAll());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteJob(@PathVariable String id) {
        boolean deleted = jobRepository.deleteById(id);
        return deleted
            ? ResponseEntity.noContent().build()
            : ResponseEntity.notFound().build();
    }
}
```

---

## 7. Component Schema API - автогенерация из Records

```java
// src/main/java/ru/pospelov/etl/engine/api/controller/JobSchemaController.java
package ru.pospelov.etl.engine.api.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.pospelov.etl.engine.api.dto.ComponentSchemaResponse;
import ru.pospelov.etl.engine.api.service.ComponentSchemaGenerator;

/**
 * API для получения schema компонентов ETL.
 * Schema генерируется автоматически из Record классов.
 */
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobSchemaController {

    private final ComponentSchemaGenerator schemaGenerator;

    @GetMapping("/schema")
    public ResponseEntity<ComponentSchemaResponse> getComponentSchema() {
        return ResponseEntity.ok(schemaGenerator.generate());
    }
}
```

```java
// src/main/java/ru/pospelov/etl/engine/api/service/ComponentSchemaGenerator.java
package ru.pospelov.etl.engine.api.service;

import org.springframework.stereotype.Service;
import ru.pospelov.etl.engine.api.dto.*;
import ru.pospelov.etl.engine.config.extractor.*;
import ru.pospelov.etl.engine.config.transformer.*;
import ru.pospelov.etl.engine.config.loader.*;

import jakarta.validation.constraints.*;
import java.lang.reflect.RecordComponent;
import java.util.*;

/**
 * Генератор schema из Record классов с использованием рефлексии.
 * Читает поля Records и Bean Validation аннотации.
 */
@Service
public class ComponentSchemaGenerator {

    public ComponentSchemaResponse generate() {
        return ComponentSchemaResponse.builder()
            .extractors(Map.of(
                "sql", generateSchema(JdbcExtractorConfig.class, "SQL Database"),
                "kafka", generateSchema(KafkaExtractorConfig.class, "Kafka Topic")
            ))
            .transformers(Map.of(
                "noop", generateSchema(NoopTransformerConfig.class, "No Operation"),
                "avro", generateSchema(AvroToRecordTransformerConfig.class, "Avro to Record"),
                "record-to-avro", generateSchema(RecordToAvroTransformerConfig.class, "Record to Avro")
            ))
            .loaders(Map.of(
                "sql", generateSchema(JdbcLoaderConfig.class, "SQL Database"),
                "fast-sql", generateSchema(FastSqlLoaderConfig.class, "Fast SQL (Bulk Insert)"),
                "kafka", generateSchema(KafkaLoaderConfig.class, "Kafka Topic")
            ))
            .build();
    }

    private ComponentSchema generateSchema(Class<?> recordClass, String displayName) {
        RecordComponent[] components = recordClass.getRecordComponents();
        List<FieldSchema> fields = new ArrayList<>();

        for (RecordComponent component : components) {
            fields.add(FieldSchema.builder()
                .name(component.getName())
                .type(mapType(component.getType()))
                .label(formatLabel(component.getName()))
                .required(isRequired(component))
                .min(getMin(component))
                .max(getMax(component))
                .build());
        }

        return ComponentSchema.builder()
            .displayName(displayName)
            .fields(fields)
            .build();
    }

    private String mapType(Class<?> javaType) {
        if (javaType == String.class) return "text";
        if (javaType == Integer.class) return "number";
        if (javaType == Long.class) return "number";
        if (javaType.isEnum()) return "enum";
        if (javaType == Optional.class) return "optional";
        return "text";
    }

    private boolean isRequired(RecordComponent component) {
        return component.isAnnotationPresent(NotNull.class)
            || component.isAnnotationPresent(NotBlank.class);
    }

    private Integer getMin(RecordComponent component) {
        Min min = component.getAnnotation(Min.class);
        return min != null ? (int) min.value() : null;
    }

    private Integer getMax(RecordComponent component) {
        Max max = component.getAnnotation(Max.class);
        return max != null ? (int) max.value() : null;
    }

    private String formatLabel(String fieldName) {
        // sqlQuery -> SQL Query
        // streamBatchSize -> Stream Batch Size
        return fieldName.replaceAll("([A-Z])", " $1")
            .replaceAll("^.", m -> m.group().toUpperCase())
            .trim();
    }
}
```

---

## План реализации (финальный)

### Фаза 1: Config Records (1 день)
- [ ] Создать package structure: `config/extractor`, `config/transformer`, `config/loader`
- [ ] Создать sealed interfaces
- [ ] Создать все Record конфигурации с Bean Validation
- [ ] Создать enum `KafkaFormat`
- [ ] Unit тесты

### Фаза 2: Модель EtlJob (0.5 дня)
- [ ] Заменить class на record
- [ ] Удалить старые поля (`source`, `targetTable`, `parameters`)
- [ ] Настроить Jackson @JsonTypeInfo/@JsonSubTypes
- [ ] Тесты сериализации/десериализации

### Фаза 3: БД Schema (0.5 дня)
- [ ] Flyway migration V2 - удалить Source/Target/Params
- [ ] Добавить раздельные колонки для типов и конфигураций
- [ ] Индексы по типам
- [ ] Обновить JobEntity

### Фаза 4: Repository (0.5 дня)
- [ ] Обновить маппинг JobEntity <-> EtlJob
- [ ] Тесты CRUD операций

### Фаза 5: EtlComponentFactory (1 день)
- [ ] Создать централизованный Factory
- [ ] Pattern matching для всех компонентов
- [ ] Обновить EtlPipelineFactory для использования Factory
- [ ] Тесты

### Фаза 6: Компоненты (2 дня)
- [ ] Обновить JdbcExtractor - принимать JdbcExtractorConfig
- [ ] Обновить KafkaPartitionExtractor - принимать KafkaExtractorConfig
- [ ] Обновить все Transformers
- [ ] Обновить все Loaders
- [ ] Удалить все `getParamOrDefault()`
- [ ] Обработка Optional полей
- [ ] Unit тесты

### Фаза 7: Component Schema API (1 день)
- [ ] ComponentSchemaGenerator с рефлексией
- [ ] DTO классы (ComponentSchema, FieldSchema, etc)
- [ ] Endpoint `/api/jobs/schema`
- [ ] Тесты генерации

### Фаза 8: Frontend (2 дня)
- [ ] Переработать job-form.html (компонентно-ориентированная структура)
- [ ] Обновить job-form.js - загрузка schema, динамический рендеринг
- [ ] Убрать все hardcoded defaults
- [ ] Обработка Optional полей (nullable)
- [ ] UI показывает рекомендации вместо defaults
- [ ] Тестирование create/edit

### Фаза 9: E2E тестирование (1 день)
- [ ] Тесты всех комбинаций (SQL→SQL, SQL→Kafka, Kafka→SQL, Kafka→Kafka)
- [ ] Тестирование валидации
- [ ] Тестирование Optional полей
- [ ] Тестирование UI

**Общее время:** 9 дней

---

## Преимущества финальной архитектуры

### 1. Type Safety - 100%

```java
// ❌ СТАРЫЙ КОД
int threads = (int) job.getParamOrDefault("threads", 4);  // runtime cast
String topic = (String) job.getParam("topic");            // может быть null

// ✅ НОВЫЙ КОД
int threads = config.threads();                           // compile-time safe
String topic = config.topic();                            // compile-time safe
Optional<String> keyCol = config.keyColumn();             // explicit null handling
```

### 2. Нет defaults в коде

```java
// ❌ СТАРЫЙ КОД - defaults разбросаны
int threads = (int) job.getParamOrDefault("threads", 4);  // JdbcExtractor
int threads = (int) job.getParamOrDefault("threads", 4);  // KafkaExtractor

// ✅ НОВЫЙ КОД - пользователь указывает всё явно
// UI показывает: "Threads (Recommended: 4)"
// Пользователь сознательно выбирает: 4, 8, 16, etc.
```

### 3. Exhaustive Pattern Matching

```java
// ✅ Компилятор проверяет все случаи
switch (job.extractorConfig()) {
    case JdbcExtractorConfig c -> handleJdbc(c);
    case KafkaExtractorConfig c -> handleKafka(c);
    // Если добавить новый sealed тип - код не скомпилируется!
}
```

### 4. SQL запросы по типам

```sql
-- ✅ Найти все jobs с Kafka extractor
SELECT * FROM tEtlJob WHERE ExtractorType = 'kafka';

-- ✅ Статистика использования
SELECT ExtractorType, LoaderType, COUNT(*)
FROM tEtlJob
GROUP BY ExtractorType, LoaderType;

-- ✅ Индексы работают
SELECT * FROM tEtlJob
WHERE ExtractorType = 'sql' AND LoaderType = 'kafka';
```

### 5. Меньше кода

| Компонент | Старый код | Новый код | Разница |
|-----------|------------|-----------|---------|
| EtlJob | ~180 строк | ~50 строк | **-72%** |
| JdbcExtractor | ~160 строк | ~100 строк | **-38%** |
| JobEntity | ~100 строк | ~80 строк | **-20%** |

### 6. Автогенерация Schema

```java
// ✅ Добавил новое поле в Record - schema обновится автоматически
public record JdbcExtractorConfig(
    // ... существующие поля
    @NotNull @Min(1) Integer prefetchSize  // ← новое поле
) {}

// Schema API автоматически вернет новое поле в /api/jobs/schema
// Frontend автоматически отобразит новое поле
```

---

## Итоговая архитектура

```
┌─────────────────────────────────────────────────────┐
│                    Frontend (UI)                    │
│  - Загружает schema из /api/jobs/schema            │
│  - Динамически рендерит поля                       │
│  - НЕТ hardcoded defaults                          │
│  - Показывает рекомендации (Recommended: 4)        │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│              REST API (JobController)               │
│  - @Valid автоматическая валидация                 │
│  - Bean Validation из Records                      │
│  - НЕТ ручной валидации                            │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│                  EtlJob (Record)                    │
│  - extractorConfig: ExtractorConfig                │
│  - transformerConfig: TransformerConfig            │
│  - loaderConfig: LoaderConfig                      │
│  - Type-safe, immutable                            │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│      EtlComponentFactory (Pattern Matching)        │
│  - Exhaustive switch для всех sealed типов         │
│  - Диспетчеризация в правильный компонент          │
└─────────────────────────────────────────────────────┘
                         ↓
┌───────────────┬───────────────┬─────────────────────┐
│  JdbcExtractor│  KafkaExtractor│  ...              │
│  JdbcLoader   │  KafkaLoader   │  ...              │
│  - Type-safe  │  - Optional    │                   │
│  - НЕТ defaults│  - НЕТ casts  │                   │
└───────────────┴───────────────┴─────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│          Database (раздельные колонки)              │
│  - ExtractorType, ExtractorConfig                  │
│  - TransformerType, TransformerConfig              │
│  - LoaderType, LoaderConfig                        │
│  - Индексы по типам                                │
└─────────────────────────────────────────────────────┘
```

**Готово к реализации!** 🚀
